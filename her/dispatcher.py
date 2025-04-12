import asyncio
import contextlib
import logging
import re
from typing import Optional, Union, List

from hikkatl import events
from hikkatl.errors import FloodWaitError, RPCError
from hikkatl.tl.types import Message

from . import main, utils
from .database import Database
from .loader import Modules
from .tl_cache import CustomTelegramClient

logger = logging.getLogger(__name__)


class SecurityError(Exception):
    pass


class CommandDispatcher:
    """Handles command dispatching and message processing"""

    def __init__(
        self,
        modules: Modules,
        client: CustomTelegramClient,
        db: Database,
    ):
        self.modules = modules
        self.client = client
        self.db = db

        self.raw_handlers = []

        self._flood_delay = 3
        self._last_reset = 0.0
        self._reset_interval = 30.0

    async def _handle_command(self, event, watcher=False) -> Union[bool, tuple]:
        # Quick validation checks to avoid unnecessary processing
        if not event.out:
            return False
            
        # Early message validation
        if not hasattr(event, "message") or not hasattr(event.message, "message"):
            return False
            
        message = utils.censor(event.message)
        
        # More validation
        if not hasattr(message, "message") or message.message is None:
            return False
            
        prefix = "."
        if not message.message.startswith(prefix):
            return False
            
        # Extract command more efficiently
        cmd_parts = message.message[len(prefix):].strip().split(maxsplit=1)
        if not cmd_parts:
            return False
            
        command = cmd_parts[0]
        if not command:
            return False
            
        txt, func = self.modules.dispatch(command)
        if not func:
            return False
            
        message.message = prefix + txt + message.message[len(prefix + command):]

        # Only process grep if needed
        if self.db.get(main.__name__, "grep", False) and not watcher:
            try:
                message = GrepHandler(message, self).message
            except SecurityError as e:
                logger.warning("Grep security error: %s", e)
                return False
                
        return message, prefix, txt, func

    async def handle_command(self, event: events.NewMessage) -> None:
        """Handle incoming commands, optimized to reduce API calls"""
        # Skip processing for non-NewMessage events
        if not isinstance(event, events.NewMessage):
            return
            
        result = await self._handle_command(event)
        if not result:
            return
            
        message, _, _, func = result

        # Use create_task to avoid blocking
        asyncio.create_task(
            self.future_dispatcher(
                func,
                message,
                self.command_exc,
            )
        )

    async def command_exc(
        self, exc: Exception, _func: callable, message: Message
    ) -> None:
        """Handle command exceptions"""
        logger.exception("Command failed", exc_info=exc)

        if isinstance(exc, RPCError):
            if isinstance(exc, FloodWaitError):
                # Calculate time components more efficiently
                seconds = exc.seconds
                hours, remainder = divmod(seconds, 3600)
                minutes, secs = divmod(remainder, 60)
                
                time_parts = []
                if hours:
                    time_parts.append(f"{hours} hours")
                if minutes:
                    time_parts.append(f"{minutes} minutes")
                if secs:
                    time_parts.append(f"{secs} seconds")
                    
                fw_time = ", ".join(time_parts)

                txt = (
                    "🕒 <b>Call</b>"
                    f" <code>{utils.escape_html(message.message)}</code>"
                    f" <b>caused FloodWait of {fw_time} on method</b>"
                    f" <code>{type(exc.request).__name__}</code>"
                )
            else:
                txt = (
                    "🚫 <b>Call</b>"
                    f" <code>{utils.escape_html(message.message)}</code>"
                    " <b>failed due to RPC error:</b>"
                    f" <code>{utils.escape_html(str(exc))}</code>"
                )
        else:
            txt = (
                "🚫 <b>Call</b>"
                f" <code>{utils.escape_html(message.message)}</code>"
                "<b> failed!</b>"
            )
            
        # Use contextlib.suppress to avoid extra try/except blocks
        with contextlib.suppress(Exception):
            await utils.answer(message, txt)

    async def watcher_exc(
        self, exc: Exception, _func: callable, _message: Message
    ) -> None:
        """Handle watcher exceptions"""
        logger.exception("Error running watcher", exc_info=exc)

    async def handle_incoming(self, event: events.NewMessage) -> None:
        """Handle only new incoming messages, reducing API calls"""
        # Early validation
        if not isinstance(event, events.NewMessage):
            return
            
        message = utils.censor(getattr(event, "message", None))
        if not isinstance(message, Message):
            return
            
        # Set default attributes in a single loop to reduce operations
        default_attrs = {"text": "", "raw_text": "", "out": ""}
        for attr, default in default_attrs.items():
            with contextlib.suppress(AttributeError, UnicodeDecodeError):
                if not hasattr(message, attr):
                    setattr(message, attr, default)
                    
        # Only process watchers if there are any registered
        if self.modules.watchers:
            for func in self.modules.watchers:
                asyncio.create_task(
                    self.future_dispatcher(
                        func,
                        message,
                        self.watcher_exc,
                    )
                )

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

    def __init__(self, message: Message, dispatcher: CommandDispatcher):
        self.message = message
        self.dispatcher = dispatcher
        self._process_grep()

    def _process_grep(self) -> None:
        # Validate message early
        if not hasattr(self.message, "text") or not isinstance(self.message.text, str):
            raise SecurityError("Invalid message type for grep")
            
        if len(self.message.text) > 4096:
            raise SecurityError("Слишком длинное сообщение для grep")
            
        # Check for escaped grep first
        if "||grep" in self.message.text or "|| grep" in self.message.text:
            self._handle_escaped_grep()
            return
            
        # Use one regex search instead of multiple checks
        grep_match = re.search(r".+\| ?grep (.+)", self.message.raw_text)
        if not grep_match:
            return
            
        grep = grep_match.group(1)
        self._clean_message()

        # Process grep and ungrep in one go
        ungrep = self._extract_ungrep(grep)
        grep = utils.escape_html(grep).strip() if grep else None
        ungrep = utils.escape_html(ungrep).strip() if ungrep else None

        self._setup_modified_methods(grep, ungrep)

    def _handle_escaped_grep(self) -> None:
        # Process all attributes in one loop
        for attr in ["raw_text", "text", "message"]:
            if hasattr(self.message, attr):
                setattr(
                    self.message,
                    attr,
                    re.sub(r"\|\| ?grep", "| grep", getattr(self.message, attr))
                )

    def _clean_message(self) -> None:
        # Process all attributes in one go
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
        # Pre-compile patterns for better performance
        grep_html_pattern = re.compile(re.escape(grep)) if grep else None
        
        def process_text(text: str) -> str:
            # Process lines in one go
            res = []
            for line in text.split("\n"):
                if self._should_include_line(line, grep, ungrep):
                    processed_line = utils.remove_html(line, escape=True)
                    if grep and grep_html_pattern:
                        processed_line = grep_html_pattern.sub(f"<u>{grep}</u>", processed_line)
                    res.append(processed_line)
                    
            return self._format_result(res, grep, ungrep)

        # Define method overrides with default HTML parse mode
        async def modified_edit(text, *args, **kwargs):
            kwargs["parse_mode"] = "HTML"
            processed_text = process_text(text)
            return await self.message.edit(self.message, processed_text, *args, **kwargs)

        async def modified_reply(text, *args, **kwargs):
            kwargs["parse_mode"] = "HTML"
            processed_text = process_text(text)
            return await self.message.reply(processed_text, *args, **kwargs)

        async def modified_respond(text, *args, **kwargs):
            kwargs["parse_mode"] = "HTML"
            kwargs.setdefault("reply_to", utils.get_topic(self.message))
            processed_text = process_text(text)
            return await self.message.respond(processed_text, *args, **kwargs)

        # Apply method overrides
        self.message.edit = modified_edit
        self.message.reply = modified_reply
        self.message.respond = modified_respond

    @staticmethod
    def _should_include_line(line: str, grep: str, ungrep: str) -> bool:
        clean_line = utils.remove_html(line)
        
        # Short-circuit evaluation for performance
        if grep and grep not in clean_line:
            return False
        if ungrep and ungrep in clean_line:
            return False
            
        return bool(grep) or bool(ungrep)

    @staticmethod
    def _format_result(res: List[str], grep: str, ungrep: str) -> str:
        if not res:
            # Build condition message more efficiently
            conditions = []
            if grep:
                conditions.append(f"contain <b>{grep}</b>")
            if ungrep:
                conditions.append(f"do not contain <b>{ungrep}</b>")
                
            return f"💬 <i>No lines that {' and '.join(conditions)}</i>"
            
        # Build header more efficiently
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