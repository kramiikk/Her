import asyncio
import logging

from .tl_cache import CustomTelegramClient
from hikkatl.tl.types import Message

from .database import Database
from .loader import Modules

logger = logging.getLogger(__name__)


class SecurityError(Exception):
    pass


class TextDispatcher:
    """Handles dispatching and message processing"""

    def __init__(
        self,
        modules: Modules,
        client: CustomTelegramClient,
        db: Database,
    ):
        self.modules = modules
        self.client = client
        self.db = db

    async def handle_incoming(self, event) -> None:
        """Handle incoming events more flexibly"""

        message = None

        if hasattr(event, "message"):
            message = event.message
        elif isinstance(event, Message):
            message = event
        if not message:
            return
        default_attrs = {"text": "", "raw_text": "", "out": False}
        for attr, default in default_attrs.items():
            try:
                if not hasattr(message, attr):
                    setattr(message, attr, default)
                elif getattr(message, attr) is None:
                    setattr(message, attr, default)
            except (AttributeError, UnicodeDecodeError) as e:
                logger.error(f"Error setting default attribute {attr}: {e}")
        if self.modules.watchers:
            tasks = []
            for func in self.modules.watchers:
                task = asyncio.create_task(self.future_dispatcher(func, message))
                tasks.append(task)

    async def future_dispatcher(self, func: callable, message: Message, *args) -> None:
        """Dispatch function execution to the future"""
        try:
            await func(message, *args)
        except Exception as exc:
            logger.exception("Error running watcher", exc_info=exc)
