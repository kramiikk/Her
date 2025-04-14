import asyncio
import logging
import re
from typing import Optional, List

from .tl_cache import CustomTelegramClient
from hikkatl.tl.types import Message

from . import utils
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

    async def watcher_exc(
        self, exc: Exception, _func: callable, _message: Message
    ) -> None:
        """Handle watcher exceptions"""
        logger.exception("Error running watcher", exc_info=exc)

    async def handle_incoming(self, event) -> None:
        """Handle incoming events more flexibly"""

        message = None

        if hasattr(event, "message"):
            message = event.message
        elif isinstance(event, Message):
            message = event
        if not message:
            return
        try:
            message = utils.censor(message)
        except Exception as e:
            logger.error(f"Censoring failed: {e}")
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
                task = asyncio.create_task(
                    self.future_dispatcher(
                        func,
                        message,
                        self.watcher_exc,
                    )
                )
                tasks.append(task)

    async def future_dispatcher(
        self, func: callable, message: Message, exception_handler: callable, *args
    ) -> None:
        """Dispatch function execution to the future"""
        try:
            await func(message, *args)
        except Exception as e:
            await exception_handler(e, func, message)


class GrepHandler:
    """Handles grep-like filtering of messages"""

    def __init__(self, message: Message, dispatcher: TextDispatcher):
        self.message = message
        self.dispatcher = dispatcher
        self._process_grep()

    def _process_grep(self) -> None:

        if not hasattr(self.message, "text") or not isinstance(self.message.text, str):
            raise SecurityError("Invalid message type for grep")
        if len(self.message.text) > 4096:
            raise SecurityError("Слишком длинное сообщение для grep")
        if "||grep" in self.message.text or "|| grep" in self.message.text:
            self._handle_escaped_grep()
            return
        grep_match = re.search(r".+\| ?grep (.+)", self.message.raw_text)
        if not grep_match:
            return
        grep = grep_match.group(1)
        self._clean_message()

        ungrep = self._extract_ungrep(grep)
        grep = utils.escape_html(grep).strip() if grep else None
        ungrep = utils.escape_html(ungrep).strip() if ungrep else None

        self._setup_modified_methods(grep, ungrep)

    def _handle_escaped_grep(self) -> None:

        for attr in ["raw_text", "text", "message"]:
            if hasattr(self.message, attr):
                setattr(
                    self.message,
                    attr,
                    re.sub(r"\|\| ?grep", "| grep", getattr(self.message, attr)),
                )

    def _clean_message(self) -> None:

        pattern = re.compile(r"\| ?grep.+")
        for attr in ["text", "raw_text", "message"]:
            if hasattr(self.message, attr):
                setattr(
                    self.message,
                    attr,
                    pattern.sub("", getattr(self.message, attr)),
                )

    def _extract_ungrep(self, grep: str) -> Optional[str]:
        ungrep_match = re.search(r"-v (.+)", grep)
        if ungrep_match:
            ungrep = ungrep_match.group(1)
            grep = re.sub(r"(.+) -v .+", r"\g<1>", grep)
            return ungrep
        return None

    def _setup_modified_methods(self, grep: str, ungrep: str) -> None:

        grep_html_pattern = re.compile(re.escape(grep)) if grep else None

        def process_text(text: str) -> str:

            res = []
            for line in text.split("\n"):
                if self._should_include_line(line, grep, ungrep):
                    processed_line = utils.remove_html(line, escape=True)
                    if grep and grep_html_pattern:
                        processed_line = grep_html_pattern.sub(
                            f"<u>{grep}</u>", processed_line
                        )
                    res.append(processed_line)
            return self._format_result(res, grep, ungrep)

        async def modified_edit(text, *args, **kwargs):
            kwargs["parse_mode"] = "HTML"
            processed_text = process_text(text)
            return await self.message.edit(
                self.message, processed_text, *args, **kwargs
            )

        async def modified_reply(text, *args, **kwargs):
            kwargs["parse_mode"] = "HTML"
            processed_text = process_text(text)
            return await self.message.reply(processed_text, *args, **kwargs)

        async def modified_respond(text, *args, **kwargs):
            kwargs["parse_mode"] = "HTML"
            kwargs.setdefault("reply_to", utils.get_topic(self.message))
            processed_text = process_text(text)
            return await self.message.respond(processed_text, *args, **kwargs)

        self.message.edit = modified_edit
        self.message.reply = modified_reply
        self.message.respond = modified_respond

    @staticmethod
    def _should_include_line(line: str, grep: str, ungrep: str) -> bool:
        clean_line = utils.remove_html(line)

        if grep and grep not in clean_line:
            return False
        if ungrep and ungrep in clean_line:
            return False
        return bool(grep) or bool(ungrep)

    @staticmethod
    def _format_result(res: List[str], grep: str, ungrep: str) -> str:
        if not res:

            conditions = []
            if grep:
                conditions.append(f"contain <b>{grep}</b>")
            if ungrep:
                conditions.append(f"do not contain <b>{ungrep}</b>")
            return f"💬 <i>No lines that {' and '.join(conditions)}</i>"
        header_parts = ["💬 <i>Lines that "]

        if grep:
            header_parts.append(f"contain <b>{grep}</b>")
        if grep and ungrep:
            header_parts.append(" and")
        if ungrep:
            header_parts.append(f" do not contain <b>{ungrep}</b>")
        header_parts.append(":</i>\n")
        header = "".join(header_parts)

        return header + "\n".join(res)
