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
from hikkatl.network.connection import (
    ConnectionTcpFull,
    ConnectionTcpMTProxyRandomizedIntermediate,
)
from hikkatl.sessions import MemorySession, SQLiteSession


from . import configurator, database, loader, utils, version
from .dispatcher import CommandDispatcher
from .tl_cache import CustomTelegramClient
from .version import __version__

web_available = False

BASE_DIR = (
    "/data"
    if "DOCKER" in os.environ
    else os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

BASE_PATH = Path(BASE_DIR)
CONFIG_PATH = BASE_PATH / "config.json"

IS_CODESPACES = "CODESPACES" in os.environ
IS_DOCKER = "DOCKER" in os.environ
IS_RAILWAY = "RAILWAY" in os.environ
IS_GOORM = "GOORM" in os.environ
IS_ORACLE = "ORACLE_OS" in os.environ
IS_AWS = "AWS_OS" in os.environ
IS_SERV00 = "serv00" in socket.gethostname()
IS_AEZA = "aeza" in socket.gethostname()
IS_USERLAND = "userland" in os.environ.get("USER", "")
IS_WSL = False
with contextlib.suppress(Exception):
    from platform import uname

    if "microsoft-standard" in uname().release:
        IS_WSL = True
# fmt: off









OFFICIAL_CLIENTS = [
    "Telegram Android",
    "Telegram iOS",
    "Telegram Desktop",
    "Telegram Web",
    "Telegram macOS",
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
            # iOS
            lambda: f"{random.randint(15, 17)}.{random.randint(0, 6)}.{random.randint(1, 3)}",
            "Apple ARM",
        ),
        (
            # Windows
            lambda: f"10 Build {random.randint(19000, 22621)}",
            "x64",
        ),
        (
            # macOS
            lambda: f"13.{random.randint(0, 4)}",
            "x86_64",
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
    """
    Generates random free port in case of VDS.
    In case of Docker, also return 8080, as it's already exposed by default.
    :returns: Integer value of generated port
    """
    if "DOCKER" in os.environ and not no8080:
        return 8080
    # But for own server we generate new free port, and assign to it

    if port := get_config_key(cfg):
        return port
    # If we didn't get port from config, generate new one
    # First, try to randomly get port

    while port := random.randint(1024, 65536):
        if socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect_ex(
            ("localhost", port)
        ):
            break
    return port


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
        default=gen_port(),
        type=int,
    )
    parser.add_argument("--phone", "-p", action="append")
    parser.add_argument(
        "--data-root",
        dest="data_root",
        default="",
        help="Root path to store session files in",
    )
    parser.add_argument(
        "--no-auth",
        dest="no_auth",
        action="store_true",
        help="Disable authentication and API token input, exitting if needed",
    )
    parser.add_argument(
        "--proxy-host",
        dest="proxy_host",
        action="store",
        help="MTProto proxy host, without port",
    )
    parser.add_argument(
        "--proxy-port",
        dest="proxy_port",
        action="store",
        type=int,
        help="MTProto proxy port",
    )
    parser.add_argument(
        "--proxy-secret",
        dest="proxy_secret",
        action="store",
        help="MTProto proxy secret",
    )
    parser.add_argument(
        "--root",
        dest="disable_root_check",
        action="store_true",
        help="Disable `force_insecure` warning",
    )
    parser.add_argument(
        "--sandbox",
        dest="sandbox",
        action="store_true",
        help="Die instead of restart",
    )
    parser.add_argument(
        "--proxy-pass",
        dest="proxy_pass",
        action="store_true",
        help="Open proxy pass tunnel on start (not needed on setup)",
    )
    parser.add_argument(
        "--no-tty",
        dest="tty",
        action="store_false",
        default=True,
        help="Do not print colorful output using ANSI escapes",
    )
    arguments = parser.parse_args()
    return arguments


class InteractiveAuthRequired(Exception):
    """Is being rased by Telethon, if phone is required"""


def raise_auth():
    """Raises `InteractiveAuthRequired`"""
    raise InteractiveAuthRequired()


class Her:
    """Main userbot instance, which can handle multiple clients"""

    def __init__(self):
        global BASE_DIR, BASE_PATH, CONFIG_PATH
        self.arguments = parse_arguments()
        if self.arguments.data_root:
            BASE_DIR = self.arguments.data_root
            BASE_PATH = Path(BASE_DIR)
            CONFIG_PATH = BASE_PATH / "config.json"
        
        self._init_session_structure()
        
        self.sessions = []
        self._read_sessions()
        
        self.loop = asyncio.get_event_loop()
        self.ready = asyncio.Event()
        self._get_api_token()
        self._get_proxy()
        
    def _init_session_structure(self):
        """Создаёт необходимую файловую структуру"""
        session_dir = Path(BASE_DIR)
        session_dir.mkdir(parents=True, exist_ok=True)
        
        session_file = session_dir / "her.session"
        if not session_file.exists():
            with contextlib.closing(sqlite3.connect(session_file)) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS sessions (
                        dc_id INTEGER PRIMARY KEY,
                        server_address TEXT,
                        port INTEGER,
                        auth_key BLOB
                    )
                """)
                conn.commit()

    def _get_proxy(self):
        """
        Get proxy tuple from --proxy-host, --proxy-port and --proxy-secret
        and connection to use (depends on proxy - provided or not)
        """
        if (
            self.arguments.proxy_host is not None
            and self.arguments.proxy_port is not None
            and self.arguments.proxy_secret is not None
        ):
            self.proxy, self.conn = (
                (
                    self.arguments.proxy_host,
                    self.arguments.proxy_port,
                    self.arguments.proxy_secret,
                ),
                ConnectionTcpMTProxyRandomizedIntermediate,
            )
            return
        self.proxy, self.conn = None, ConnectionTcpFull

    def _read_sessions(self):
        """Загружает сессии с улучшенной обработкой ошибок"""
        session_path = Path(BASE_DIR) / "her.session"
        try:
            if session_path.exists():
                with contextlib.closing(sqlite3.connect(session_path)) as conn:
                    conn.execute("PRAGMA integrity_check")
                    conn.execute("SELECT * FROM sessions LIMIT 1")
                
                self.sessions = [SQLiteSession(str(session_path))]
                logging.debug(f"Session loaded: {session_path}")
            else:
                logging.warning("Session file not found after structure init")
                self.sessions = []
        except sqlite3.DatabaseError as e:
            logging.error(f"Invalid session: {e}, recreating...")
            session_path.unlink(missing_ok=True)
            self._init_session_structure()
            self._read_sessions()

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
            if self.arguments.no_auth:
                return
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
            connection=self.conn,
            proxy=self.proxy,
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

    async def save_client_session(self, client: CustomTelegramClient):
        session_path = Path(BASE_DIR) / "her.session"
        temp_session_path = session_path.with_name("her.temp.session")
        
        try:
            session_path.parent.mkdir(parents=True, exist_ok=True)
            
            temp_session = SQLiteSession(str(temp_session_path))
            temp_session.set_dc(
                client.session.dc_id,
                client.session.server_address,
                client.session.port,
            )
            temp_session.auth_key = client.session.auth_key
            
            temp_session.save()
            if not temp_session_path.exists():
                raise FileNotFoundError(f"Temp file {temp_session_path} not created!")
                
            temp_session_path.replace(session_path)
            logging.info(f"Session rotated: {temp_session_path} → {session_path}")
            
            await client.disconnect()
            client.session = SQLiteSession(str(session_path))
            await self._common_client_setup(client)
            
            client.hikka_db = database.Database(client)
            await client.hikka_db.init()
            
        except Exception as e:
            logging.error(f"Session save failed: {repr(e)}")
            if temp_session_path.exists():
                temp_session_path.unlink(missing_ok=True)
            raise
        finally:
            if temp_session_path.exists():
                temp_session_path.unlink(missing_ok=True)

    async def _initial_setup(self) -> bool:
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                client = await self._common_client_setup(
                    self._create_client(MemorySession())
                )

                await client.start(
                    phone=lambda: input("\n📱 Enter phone number: "),
                    password=lambda: getpass.getpass("🔒 2FA password (if set): "),
                    code_callback=lambda: input("📳 SMS/Telegram code: "),
                )

                me = await client.get_me()
                client.tg_id = me.id
                await self.save_client_session(client)

                # Добавляем обновление списка сессий
                self._read_sessions()  # <-- Важное исправление
                logging.info("✅ Successfully authorized!")
                return True
            except Exception as e:
                logging.error(f"❌ Setup error ({attempt}/{max_attempts}): {repr(e)}")
                if attempt == max_attempts:
                    return False
                await asyncio.sleep(2)
        return False

    async def _init_clients(self) -> bool:
        """Инициализация клиентов с защитой от IndexError"""
        if not self.sessions:
            logging.info("Starting initial setup...")
            return await self._initial_setup()
            
        try:
            client = await self._common_client_setup(
                self._create_client(self.sessions[0])
            )
            
            if not await client.is_user_authorized():
                raise AuthKeyInvalidError
                
            return True
        except AuthKeyInvalidError:
            logging.error("Session expired, starting re-auth...")
            return await self._initial_setup()

    async def amain_wrapper(self, client: CustomTelegramClient):
        """Wrapper around amain"""
        async with client:
            first = True
            me = await client.get_me()
            client.tg_id = me.id
            client.hikka_me = me
            while await self.amain(first, client):
                first = False

    async def _badge(self, client: CustomTelegramClient):
        """Call the badge in shell"""
        try:
            import git

            repo = git.Repo()

            build = utils.get_git_hash()
            diff = repo.git.log([f"HEAD..origin/{version.branch}", "--oneline"])
            upd = "Update required" if diff else "Up-to-date"

            logging.info(
                f"• Build: {build[:7]}\n"
                f"• Version: {'.'.join(list(map(str, list(__version__))))}\n"
                f"• {upd}\n"
                f"• For {client.tg_id}\n"
                f"• Prefix: «{client.hikka_db.get(__name__, 'command_prefix', False) or '.'}»"
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
        save_config_key("port", self.arguments.port)
        await self._get_token()

        if not await self._init_clients():
            return
        self.loop.set_exception_handler(
            lambda _, x: logging.error(
                "Exception on event loop! %s",
                x["message"],
                exc_info=x.get("exception", None),
            )
        )

        await self.amain_wrapper(self.sessions[0])

    def _shutdown_handler(self):
        """Shutdown handler"""
        logging.info("Bye")
        if self.sessions:
            self.sessions[0].disconnect()
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
