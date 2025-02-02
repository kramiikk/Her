"""Main script, where all the fun starts"""

# Friendly Telegram (telegram userbot)
# Copyright (C) 2018-2019 The Authors
# ©️ Dan Gazizullin, codrago 2021-2024
# This file is a part of Her
# 🌐 https://github.com/hikariatama/Hikka


import argparse
import asyncio
import contextlib
import importlib
import getpass
import git
import json
import logging
import os
import random
import signal
import socket
import sqlite3
import sys
import typing
from pathlib import Path

import hikkatl
from hikkatl import events
from hikkatl.errors import AuthKeyInvalidError
from hikkatl.sessions import MemorySession, SQLiteSession
from hikkatl.network.connection import ConnectionTcpFull


from . import configurator, database, loader, utils, version
from .dispatcher import CommandDispatcher
from .tl_cache import CustomTelegramClient
from .version import __version__

BASE_DIR = (
    "/data"
    if "DOCKER" in os.environ
    else os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

BASE_PATH = Path(BASE_DIR)
CONFIG_PATH = BASE_PATH / "config.json"

IS_DOCKER = "DOCKER" in os.environ
IS_RAILWAY = "RAILWAY" in os.environ
IS_USERLAND = "userland" in os.environ.get("USER", "")
IS_WSL = False
with contextlib.suppress(Exception):
    from platform import uname

    if "microsoft-standard" in uname().release:
        IS_WSL = True
# fmt: off








OFFICIAL_CLIENTS = [
    "Telegram Android",
    "Telegram Desktop",
    "Telegram Web",
    "Telegram Win",
    "Telegram Linux",
]
# fmt: on


class ApiToken(typing.NamedTuple):
    ID: int
    HASH: str


def generate_random_system_version():
    systems = [
        (
            # Android
            lambda: f"Android {random.randint(10, 14)} (SDK {random.randint(30, 34)})",
            "arm64-v8a",
        ),
        (
            # Windows
            lambda: f"11 Build {random.randint(19000, 22621)}",
            "x64",
        ),
        (
            # Linux
            lambda: f"Ubuntu 2{random.randint(2, 4)}.04",
            "x64",
        ),
    ]

    version_gen, arch = random.choice(systems)
    return f"{version_gen()} [{arch}]"


try:
    import uvloop

    uvloop.install()
except Exception:
    pass


def get_config_key(key: str) -> typing.Union[str, bool]:
    """
    Parse and return key from config
    :param key: Key name in config
    :return: Value of config key or `False`, if it doesn't exist
    """
    try:
        return json.loads(CONFIG_PATH.read_text()).get(key, False)
    except FileNotFoundError:
        return False


def save_config_key(key: str, value: str) -> bool:
    """
    Save `key` with `value` to config
    :param key: Key name in config
    :param value: Desired value in config
    :return: `True` on success, otherwise `False`
    """
    try:
        # Try to open our newly created json config

        config = json.loads(CONFIG_PATH.read_text())
    except FileNotFoundError:
        # If it doesn't exist, just default config to none
        # It won't cause problems, bc after new save
        # we will create new one

        config = {}
    # Assign config value

    config[key] = value
    # And save config

    CONFIG_PATH.write_text(json.dumps(config, indent=4))
    return True


def gen_port(cfg: str = "port", no8080: bool = False) -> int:
    """Генерация порта с проверкой занятости"""
    if not no8080 and "DOCKER" in os.environ:
        return 8080
    if (cfg_port := get_config_key(cfg)) is not None:
        return int(cfg_port)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def parse_arguments() -> dict:
    """
    Parses the arguments
    :returns: Dictionary with arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--port",
        dest="port",
        action="store",
        default=None,
        type=int,
        help="Порт для веб-сервера (по умолчанию: автоматический выбор)",
    )
    parser.add_argument("--phone", "-p", action="append")
    parser.add_argument(
        "--root",
        dest="disable_root_check",
        action="store_true",
        help="Disable `force_insecure` warning",
    )
    arguments = parser.parse_args()
    return arguments


class SessionExpiredError(Exception):
    """Raised when session needs re-authentication"""


class InteractiveAuthRequired(Exception):
    """Is being rased by Telethon, if phone is required"""


def raise_auth():
    """Raises `InteractiveAuthRequired`"""
    raise InteractiveAuthRequired()


class Her:
    """Main userbot instance, which can handle multiple clients"""

    def __init__(self):
        try:
            self.arguments = parse_arguments()
            self.base_dir = self._get_base_dir()
            self.base_path = Path(self.base_dir)
            self.config_path = self.base_path / "config.json"

            self.sessions = []
            self._read_sessions()

            self.loop = asyncio.get_event_loop()
            self.ready = asyncio.Event()
            self._get_api_token()
            self.clients = []
            if self.arguments.port is None:
                self.arguments.port = gen_port()
        except Exception as e:
            logging.critical(f"Failed to initialize Her instance: {e}")
            raise

    def _get_base_dir(self):
        return (
            "/data"
            if "DOCKER" in os.environ
            else os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        )

    def _read_sessions(self):
        """Load sessions without manual AuthKeyInvalidError check"""
        session_path = Path(BASE_DIR) / "her.session"
        self.sessions = []

        if not session_path.exists():
            logging.warning("Session file not found")
            return
        try:
            session = SQLiteSession(str(session_path))
            self.sessions.append(session)
            logging.info(f"Loaded session from {session_path}")
        except sqlite3.DatabaseError as e:
            logging.error(f"Invalid session: {e}")
            self._handle_corrupted_session(session_path)
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            self._handle_corrupted_session(session_path)

    def _handle_corrupted_session(self, session_path: Path):
        """Обработка поврежденных сессий"""
        logging.warning("Removing corrupted session...")
        try:
            session_path.unlink(missing_ok=True)
            # Удален вызов _init_session_structure()
        except Exception as e:
            logging.error(f"Failed to reset session: {e}")
            raise RuntimeError("Session recovery failed") from e

    def _get_api_token(self):
        """Get API Token from disk or environment"""
        try:
            # Legacy migration

            if not get_config_key("api_id"):
                api_id, api_hash = (
                    line.strip()
                    for line in (Path(BASE_DIR) / "api_token.txt")
                    .read_text()
                    .splitlines()
                )
                save_config_key("api_id", int(api_id))
                save_config_key("api_hash", api_hash)
                (Path(BASE_DIR) / "api_token.txt").unlink()
            api_token = ApiToken(
                int(get_config_key("api_id")),
                get_config_key("api_hash"),
            )
        except FileNotFoundError:
            try:
                from . import api_token
            except ImportError:
                try:
                    api_token = ApiToken(
                        os.environ["api_id"],
                        os.environ["api_hash"],
                    )
                except KeyError:
                    api_token = None
        self.api_token = api_token

    async def _get_token(self):
        """Reads or waits for user to enter API credentials"""
        while self.api_token is None:
            configurator.api_config()
            importlib.invalidate_caches()
            self._get_api_token()

    def _create_client(
        self, session: typing.Union[MemorySession, SQLiteSession]
    ) -> CustomTelegramClient:
        """Фабричный метод для создания клиента Telegram"""
        return CustomTelegramClient(
            session,
            self.api_token.ID,
            self.api_token.HASH,
            connection=ConnectionTcpFull,
            proxy=None,
            connection_retries=None,
            device_model=random.choice(OFFICIAL_CLIENTS),
            system_version=generate_random_system_version(),
            app_version=self._generate_app_version(),
            lang_code="en",
            system_lang_code="en-US",
        )

    def _generate_app_version(self) -> str:
        """Генерация версии приложения"""
        base_version = f"{random.randint(8, 10)}"
        return (
            f"{base_version}.{random.randint(0, 9)}"
            if random.choice([True, False])
            else f"{base_version}.{random.randint(0, 9)}.{random.randint(0, 9)}"
        )

    async def _common_client_setup(self, client: CustomTelegramClient):
        """Общая настройка клиента"""
        try:
            await client.connect()
            client.phone = "🏴‍☠️ +888###"
            return client
        except OSError as e:
            logging.error(f"Connection error: {e}")
            await client.disconnect()
            raise

    async def _initial_setup(self) -> bool:
        """Исправленный процесс начальной настройки"""
        client = None
        try:
            session_path = self.base_path / "her.session"
            session = SQLiteSession(str(session_path))

            if not hasattr(self, "api_token") or not self.api_token:
                await self._get_token()
            client = self._create_client(session)
            await client.connect()

            if not await client.is_user_authorized():
                logging.info("Starting authentication...")
                await client.start(
                    phone=lambda: input("Phone: "),
                    password=lambda: getpass.getpass("2FA: "),
                    code_callback=lambda: input("Code: "),
                )
            client.session.save()
            self._read_sessions()
            return True
        except Exception as e:
            logging.error(f"Setup failed: {e}")
            return False
        finally:
            if client:
                await client.disconnect()

    async def _init_clients(self) -> bool:
        """Initialize clients with proper exception handling"""
        if not self.sessions:
            logging.info("Starting initial setup...")
            return await self._initial_setup()
        try:
            client = await self._common_client_setup(
                self._create_client(self.sessions[0])
            )

            if not await client.is_user_authorized():
                raise SessionExpiredError  # Use custom exception
            self.clients.append(client)  # Store the client instance
            return True
        except (AuthKeyInvalidError, SessionExpiredError):
            logging.error("Session invalid or expired, starting re-auth...")
            return await self._initial_setup()

    async def amain_wrapper(self, client: CustomTelegramClient):
        """Wrapper around amain"""
        try:
            await client.start()
            first = True
            me = await client.get_me()
            client.tg_id = me.id
            client.hikka_me = me

            while True:
                try:
                    await self.amain(first, client)
                    first = False
                except (ConnectionError, AuthKeyInvalidError):
                    break
        finally:
            await client.disconnect()
            client.session.save()

    async def _badge(self, client: CustomTelegramClient):
        """Call the badge in shell"""
        try:
            repo = git.Repo()
            build = utils.get_git_hash()
            upd = (
                "Update required"
                if repo.git.log([f"HEAD..origin/{version.branch}", "--oneline"])
                else "Up-to-date"
            )

            logging.info(
                f"\n• Prefix: «{client.hikka_db.get(__name__, 'command_prefix', False) or '.'}»"
                f"\n• Version: {'.'.join(list(map(str, list(__version__))))} {build[:7]} {upd}"
                f"\n• For {client.tg_id}"
            )
        except Exception:
            logging.exception("Badge error")

    async def _add_dispatcher(
        self,
        client: CustomTelegramClient,
        modules: loader.Modules,
        db: database.Database,
    ):
        """Inits and adds dispatcher instance to client"""
        dispatcher = CommandDispatcher(modules, client, db)
        client.dispatcher = dispatcher

        client.add_event_handler(
            dispatcher.handle_incoming,
            events.NewMessage,
        )

        client.add_event_handler(
            dispatcher.handle_incoming,
            events.ChatAction,
        )

        client.add_event_handler(
            dispatcher.handle_command,
            events.NewMessage(forwards=False),
        )

        client.add_event_handler(
            dispatcher.handle_command,
            events.MessageEdited(),
        )

        client.add_event_handler(
            dispatcher.handle_raw,
            events.Raw(),
        )

    async def amain(self, first: bool, client: CustomTelegramClient):
        """Entrypoint for async init, run once for each user"""
        client.parse_mode = "HTML"
        await client.start()

        db = database.Database(client)
        client.hikka_db = db
        await db.init()

        modules = loader.Modules(client, self.sessions[0], db)
        client.loader = modules

        await self._add_dispatcher(client, modules, db)

        await modules.register_all(None)
        modules.send_config()
        await modules.send_ready()

        if first:
            await self._badge(client)
        await client.run_until_disconnected()

    async def _main(self):
        """Main entrypoint"""
        try:
            save_config_key("port", self.arguments.port)
            await self._get_token()

            if not await self._init_clients():
                return
            self.loop.set_exception_handler(self._exception_handler)

            await self.amain_wrapper(self.clients[0])
        except Exception as e:
            logging.critical(f"Critical error: {e}")
            sys.exit(1)

    def _exception_handler(self, loop, context):
        """Обработчик исключений event loop"""
        logging.error(
            "Event loop error: %s (message: %s)",
            context.get("exception"),
            context["message"],
        )

    def _shutdown_handler(self):
        """Shutdown handler"""
        logging.info("Bye")
        sys.exit(0)

    def main(self):
        """Main entrypoint"""
        signal.signal(signal.SIGINT, self._shutdown_handler)
        try:
            self.loop.run_until_complete(self._main())
        except KeyboardInterrupt:
            self._shutdown_handler()
        self.loop.close()


hikkatl.extensions.html.CUSTOM_EMOJIS = not get_config_key("disable_custom_emojis")

hikka = Her()
