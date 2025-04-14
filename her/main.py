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
from hikkatl.sessions import MemorySession, SQLiteSession
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
            self.base_dir = self._get_base_dir()
            self.base_path = Path(self.base_dir)
            self.config_path = self.base_path / "config.json"

            self.sessions = []
            self._read_sessions()

            self.loop = asyncio.get_event_loop()
            self.ready = asyncio.Event()
            self._get_api_token()
            self.clients = []
        except Exception as e:
            logging.critical(f"Failed to initialize Her instance: {e}")
            raise

    def _get_base_dir(self):
        return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    def _read_sessions(self):
        """Load sessions without manual AuthKeyInvalidError check"""
        session_path = Path(BASE_DIR) / "her.session"
        self.sessions = []

        if not session_path.exists():
            return
        try:
            session = SQLiteSession(str(session_path))
            self.sessions.append(session)
        except Exception as e:
            logging.error(f"Invalid session: {e}")
            self._handle_corrupted_session(session_path)

    def _handle_corrupted_session(self, session_path: Path):
        """bs"""
        logging.warning("Removing corrupted session...")
        try:
            session_path.unlink(missing_ok=True)
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
        """Telegram"""
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
        """s"""
        try:
            await client.connect()
            client.phone = "🏴‍☠️ +888"
            return client
        except OSError as e:
            logging.error(f"Connection error: {e}")
            await client.disconnect()
            raise

    async def _initial_setup(self) -> bool:
        """Improved initial setup with non-interactive fallback"""
        client = None
        try:
            session_path = self.base_path / "her.session"
            session = SQLiteSession(str(session_path))

            if not hasattr(self, "api_token") or not self.api_token:
                await self._get_token()
            client = self._create_client(session)
            await client.connect()

            if not await client.is_user_authorized():
                phone = (
                    self.arguments.phone[0]
                    if self.arguments.phone
                    else os.environ.get("TELEGRAM_PHONE")
                )
                password = os.environ.get("TELEGRAM_2FA", "")

                if not phone:
                    try:
                        phone = input("Phone: ")
                    except EOFError:
                        logging.error("Cannot read phone number interactively.")
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
                except EOFError:
                    logging.error("Cannot read authentication code interactively.")
                    return None
            client.session.save()
            self._read_sessions()
            return client
        except Exception as e:
            logging.error(f"Setup failed: {e}")
            return None
        finally:
            if client:
                await client.disconnect()

    async def _init_clients(self) -> bool:
        """Initialize clients with better error handling"""
        if not self.sessions:
            client = await self._initial_setup()
            if client:
                self.clients.append(client)
                return True
            logging.critical("Unable to initialize client. Exiting.")
            return False
        try:
            client = await self._common_client_setup(
                self._create_client(self.sessions[0])
            )
            if not await client.is_user_authorized():
                logging.warning("Session expired, attempting to re-authenticate")
                await client.disconnect()
                return await self._handle_expired_session()
            self.clients.append(client)
            return True
        except (AuthKeyInvalidError, SessionExpiredError):
            logging.error("Session invalid. Attempting to recreate...")
            return await self._handle_expired_session()

    async def _handle_expired_session(self):
        """Handle expired session by removing it and trying to create a new one"""
        session_path = self.base_path / "her.session"
        try:
            session_path.unlink(missing_ok=True)
            self.sessions = []
            return await self._initial_setup() is not None
        except Exception as e:
            logging.error(f"Failed to handle expired session: {e}")
            return False

    async def amain_wrapper(self, client: CustomTelegramClient):
        """Wrapper around amain"""
        try:
            await asyncio.sleep(random.uniform(13, 99))

            await client.start()
            first = True
            me = await client.get_me()
            client.tg_id = me.id

            while True:
                try:
                    await self.amain(first, client)
                    first = False
                except (ConnectionError, AuthKeyInvalidError):
                    break
        finally:
            await client.disconnect()
            client.session.save()

    async def _add_dispatcher(
        self,
        client: CustomTelegramClient,
        modules: loader.Modules,
        db: database.Database,
    ):
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
        await client.start()

        db = database.Database(client)
        client.her_db = db
        await db.init()

        modules = loader.Modules(client, self.sessions[0], db)
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
        try:
            await self._get_token()

            if not await self._init_clients() or not self.clients:
                logging.critical("No valid client initialization")
                sys.exit(1)
            await self.amain_wrapper(self.clients[0])
        except Exception as e:
            logging.critical(f"Critical error: {e}")
            sys.exit(1)

    def _shutdown_handler(self, sig=None, frame=None):
        """Improved shutdown handler that cleans up pending tasks"""
        logging.info("Shutting down gracefully...")

        for task in asyncio.all_tasks(self.loop):
            if task is not asyncio.current_task(self.loop):
                task.cancel()
        self.loop.create_task(self._close_clients())
        logging.info("Bye")

        if sig is not None:
            sys.exit(0)

    async def _close_clients(self):
        """Gracefully close all client connections"""
        close_tasks = []
        for client in self.clients:
            if client.is_connected():
                close_tasks.append(client.disconnect())
        if close_tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*close_tasks), timeout=5)
            except asyncio.TimeoutError:
                logging.warning("Some clients didn't disconnect in time")
        self.loop.stop()

    def main(self):
        """Improved main entrypoint with better error handling"""
        signal.signal(signal.SIGINT, self._shutdown_handler)
        try:
            self.loop.run_until_complete(self._main())
        except KeyboardInterrupt:
            self._shutdown_handler()
        except Exception as e:
            logging.critical(f"Unhandled error: {e}")
            self._shutdown_handler()
            sys.exit(1)
        finally:
            pending = asyncio.all_tasks(self.loop)
            for task in pending:
                task.cancel()
            if pending:
                try:
                    self.loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True)
                    )
                except Exception:
                    pass
            self.loop.close()


her = Her()
