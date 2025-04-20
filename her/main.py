import argparse
import asyncio
import importlib
import json
import logging
import os
import random
import signal
import sys
import typing
from pathlib import Path


from hikkatl import events
from hikkatl.errors import AuthKeyInvalidError
from hikkatl.sessions import SQLiteSession
from hikkatl.network.connection import ConnectionTcpFull


from . import configurator, database, loader
from .dispatcher import TextDispatcher
from .tl_cache import CustomTelegramClient

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

BASE_PATH = Path(BASE_DIR)
CONFIG_PATH = BASE_PATH / "config.json"

OFFICIAL_CLIENTS = [
    "Telegram Desktop",
    "Telegram Linux",
]


class ApiToken(typing.NamedTuple):
    ID: int
    HASH: str


def generate_random_system_version():
    systems = [
        (
            lambda: f"11 Build {random.randint(19000, 22621)}",
            "x64",
        ),
        (
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
    try:
        return json.loads(CONFIG_PATH.read_text()).get(key, False)
    except FileNotFoundError:
        return False


def save_config_key(key: str, value: str) -> bool:
    try:
        config = json.loads(CONFIG_PATH.read_text())
    except FileNotFoundError:
        config = {}
    config[key] = value

    CONFIG_PATH.write_text(json.dumps(config, indent=4))
    return True


def parse_arguments() -> dict:
    """Parses the arguments"""
    parser = argparse.ArgumentParser()
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
            self.base_dir = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..")
            )
            self.base_path = Path(self.base_dir)
            self.config_path = self.base_path / "config.json"

            self.sessions = []
            self.loop = asyncio.get_event_loop()
            self.ready = asyncio.Event()
            self.clients = []

            self._get_api_token()

            if self.api_token:
                self._read_sessions()
        except Exception as e:
            logging.critical(f"Failed to initialize Her instance: {e}")
            raise

    def _read_sessions(self):
        """Load sessions without trying to fix errors"""
        session_path = self.base_path / "her.session"
        self.sessions = []

        if not session_path.exists():
            return
        try:
            session = SQLiteSession(str(session_path))
            self.sessions.append(session)
        except Exception as e:
            logging.critical(f"Invalid session file: {e}")

    def _get_api_token(self):
        """Get API Token from disk or environment"""
        self.api_token = None

        try:
            if get_config_key("api_id") and get_config_key("api_hash"):
                self.api_token = ApiToken(
                    int(get_config_key("api_id")),
                    get_config_key("api_hash"),
                )
                return
            token_file = Path(self.base_dir) / "api_token.txt"
            if token_file.exists():
                api_id, api_hash = (
                    line.strip() for line in token_file.read_text().splitlines()
                )
                save_config_key("api_id", int(api_id))
                save_config_key("api_hash", api_hash)
                token_file.unlink()
                self.api_token = ApiToken(int(api_id), api_hash)
                return
            try:
                from . import api_token

                self.api_token = ApiToken(
                    api_token.ID,
                    api_token.HASH,
                )
                return
            except ImportError:
                pass
            if os.environ.get("api_id") and os.environ.get("api_hash"):
                self.api_token = ApiToken(
                    int(os.environ["api_id"]),
                    os.environ["api_hash"],
                )
                return
        except Exception as e:
            logging.critical(f"Failed to get API token: {e}")
        logging.critical(
            "No API token found. Please run configurator or set environment variables."
        )

    def _create_client(self, session):
        """Create a new Telegram client"""
        if not self.api_token:
            logging.critical("Cannot create client without API token")
            return None
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
        """Generate a random app version"""
        base_version = f"{random.randint(8, 10)}"
        return (
            f"{base_version}.{random.randint(0, 9)}"
            if random.choice([True, False])
            else f"{base_version}.{random.randint(0, 9)}.{random.randint(0, 9)}"
        )

    async def _setup_client(self):
        """Set up a client with proper authentication"""
        if not self.api_token:
            logging.critical("No API token available")
            return None
        session_path = self.base_path / "her.session"
        try:
            session = SQLiteSession(str(session_path))
        except Exception as e:
            logging.critical(f"Failed to create session: {e}")
            return None
        client = self._create_client(session)
        if not client:
            return None
        try:
            await client.connect()
        except Exception as e:
            logging.critical(f"Failed to connect: {e}")
            await client.disconnect()
            return None
        try:
            if not await client.is_user_authorized():
                phone = (
                    self.arguments.phone[0]
                    if self.arguments.phone
                    else os.environ.get("TELEGRAM_PHONE")
                )
                password = os.environ.get("TELEGRAM_2FA", "")

                if not phone and not sys.stdin.isatty():
                    logging.critical(
                        "Cannot authenticate: running in non-interactive mode without phone credentials"
                    )
                    await client.disconnect()
                    return None
                if not phone:
                    try:
                        phone = input("Phone: ")
                    except (EOFError, KeyboardInterrupt):
                        logging.critical("Authentication cancelled")
                        await client.disconnect()
                        return None
                try:
                    await client.start(
                        phone=lambda: phone,
                        password=lambda: password,
                        code_callback=lambda: (
                            input("Code: ")
                            if sys.stdin.isatty()
                            else os.environ.get("TELEGRAM_CODE", "")
                        ),
                    )
                except (EOFError, KeyboardInterrupt):
                    logging.critical("Authentication cancelled")
                    await client.disconnect()
                    return None
                except Exception as e:
                    logging.critical(f"Authentication failed: {e}")
                    await client.disconnect()
                    return None
            client.session.save()
            return client
        except AuthKeyInvalidError:
            logging.critical("Session expired or invalid")
            await client.disconnect()
            try:
                session_path.unlink(missing_ok=True)
            except Exception as e:
                logging.error(f"Failed to delete invalid session: {e}")
            return None
        except Exception as e:
            logging.critical(f"Setup failed: {e}")
            await client.disconnect()
            return None

    async def _add_dispatcher(self, client, modules, db):
        """Inits and adds dispatcher instance to client"""
        dispatcher = TextDispatcher(modules, client, db)
        client.dispatcher = dispatcher

        client.add_event_handler(
            dispatcher.handle_incoming,
            events.NewMessage,
        )

    async def amain(self, first: bool, client: CustomTelegramClient):
        """Entrypoint for async init, run once for each user"""
        client.parse_mode = "HTML"

        try:
            me = await client.get_me()
            client.tg_id = me.id
        except Exception as e:
            logging.critical(f"Failed to get user info: {e}")
            return
        db = database.Database(client)
        client.her_db = db
        await db.init()

        modules = loader.Modules(client, client.session, db)
        client.loader = modules

        await self._add_dispatcher(client, modules, db)

        await modules.register_all()
        modules.send_config()
        await modules.send_ready()

        if first:
            logging.info(f"\n\n• Version 1.8.1\n\n• For {client.tg_id}")
        await client.run_until_disconnected()

    async def _main(self):
        """Main entrypoint"""
        if not self.api_token:
            if sys.stdin.isatty():
                configurator.api_config()
                importlib.invalidate_caches()
                self._get_api_token()
            if not self.api_token:
                logging.critical("No API token available. Exiting.")
                return
        client = await self._setup_client()
        if not client:
            logging.critical("Failed to setup client. Exiting.")
            return
        self.clients.append(client)

        try:
            await self.amain(True, client)
        except Exception as e:
            logging.critical(f"Error in main loop: {e}")
        finally:
            await client.disconnect()

    async def _close_clients(self):
        """Gracefully close all client connections"""
        for client in self.clients:
            if client.is_connected():
                try:
                    await client.disconnect()
                except Exception:
                    pass

    def _shutdown_handler(self, sig=None, frame=None):
        """Handle shutdown signal"""
        logging.info("Shutting down gracefully...")

        for task in asyncio.all_tasks(self.loop):
            if task is not asyncio.current_task(self.loop):
                task.cancel()
        self.loop.create_task(self._close_clients())

        self.loop.stop()

        logging.info("Bye")

        sys.exit(0)

    def main(self):
        """Main entrypoint"""
        signal.signal(signal.SIGINT, self._shutdown_handler)
        signal.signal(signal.SIGTERM, self._shutdown_handler)

        try:
            self.loop.run_until_complete(self._main())
        except KeyboardInterrupt:
            self._shutdown_handler()
        except Exception as e:
            logging.critical(f"Unhandled error: {e}")
            self._shutdown_handler()
        finally:
            try:
                self._shutdown_handler()
            except Exception:
                pass


her = Her()
