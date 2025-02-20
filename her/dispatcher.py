import asyncio
import contextlib
import logging
import re
import time
from typing import Optional, Union, List

from hikkatl import events
from hikkatl.errors import FloodWaitError, RPCError, ChatAdminRequiredError
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
        self._modules = modules
        self._client = client
        self._db = db

        self.owner = db.pointer(__name__, "owner", [])

        self.raw_handlers = []

        self._api_lock = asyncio.Lock()
        self._api_calls = 0
        self._max_api_calls = 15
        self._flood_delay = 3
        self._last_reset = 0.0
        self._reset_interval = 30.0
        self._reload_rights()

    def _reload_rights(self):
        """Internal method to ensure that account owner is always in the owner list"""
        if self._client.tg_id not in self.owner:
            self.owner.append(self._client.tg_id)

    async def _api_call_guard(self):
        """Контролирует частоту запросов к API с периодическим сбросом"""
        current_time = time.time()

        async with self._api_lock:
            if current_time - self._last_reset >= self._reset_interval:
                self._api_calls = 0
                self._last_reset = current_time
            if self._api_calls < self._max_api_calls:
                self._api_calls += 1
                return
            await asyncio.sleep(self._flood_delay)
            self._api_calls = 1
            self._last_reset = current_time

    async def safe_api_call(self, coro):
        """Обёртка для безопасного вызова API"""
        try:
            await self._api_call_guard()

            for _ in range(3):
                try:
                    return await coro
                except FloodWaitError as e:
                    wait_time = e.seconds + 5
                    logger.warning(
                        f"FloodWait detected. Sleeping for {wait_time} seconds"
                    )
                    await asyncio.sleep(wait_time)
                except ChatAdminRequiredError:
                    logger.error("Missing admin rights for operation")
                    raise
                except RPCError as e:
                    if "CHAT_WRITE_FORBIDDEN" in str(e):
                        logger.error("Can't write to this chat")
                        raise
                    logger.error("RPC error: %s", e)
                    await asyncio.sleep(1)
                    continue
            raise RuntimeError("Failed after 3 retries")
        except Exception as e:
            logger.exception("Error in safe_api_call: %s", e)
            raise

    async def _handle_command(self, event, watcher=False) -> Union[bool, tuple]:
        if not hasattr(event, "message") or not hasattr(event.message, "message"):
            return False
        message = utils.censor(event.message)

        if isinstance(event, events.NewMessage):
            if (
                event.sticker
                or event.dice
                or event.audio
                or event.via_bot_id
                or getattr(event, "reactions", False)
            ):
                return False
        if not hasattr(message, "sender_id"):
            if hasattr(message, "from_id"):
                message.sender_id = message.from_id.user_id
            else:
                return False
        if message.sender_id not in self.owner:
            return False
        prefix = "."

        if not message.message.startswith(prefix):
            return False
        cmd_text = message.message[len(prefix):].strip()
        if not cmd_text:
            return False
        try:
            command = cmd_text.split(maxsplit=1)[0]
        except IndexError:
            return False
        if len(message.message) <= len(prefix):
            return False

        tag = command.split("@", maxsplit=1)

        if len(tag) == 2:
            if tag[1] == "me" and not message.out:
                return False
        elif not (event.out or event.mentioned) and not event.is_private:
            return False
        txt, func = self._modules.dispatch(tag[0])
        if not func:
            return False
        if (
            message.edit_date
            and not message.is_group
            and not message.out
        ):
            return False
        message.message = prefix + txt + message.message[len(prefix + command) :]

        if self._db.get(main.__name__, "grep", False) and not watcher:
            try:
                message = GrepHandler(message, self).message
            except SecurityError as e:
                logger.warning("Grep security error: %s", e)
                return False
        return message, prefix, txt, func

    async def handle_raw(self, event: events.Raw) -> None:
        """Handle raw events"""
        for handler in self.raw_handlers:
            if isinstance(event, tuple(handler.updates)):
                try:
                    await handler(event)
                except Exception as e:
                    logger.exception("Error in raw handler %s: %s", handler.id, e)

    async def handle_command(
        self,
        event: Union[events.NewMessage, events.MessageDeleted],
    ) -> None:
        """Handle incoming commands"""
        result = await self._handle_command(event)
        if not result:
            return
        message, _, _, func = result

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
                time_parts = []
                seconds = exc.seconds

                hours = seconds // 3600
                if hours:
                    time_parts.append(f"{hours} hours")
                minutes = (seconds % 3600) // 60
                if minutes:
                    time_parts.append(f"{minutes} minutes")
                secs = seconds % 60
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
        with contextlib.suppress(Exception):
            await utils.answer(message, txt)

    async def watcher_exc(
        self, exc: Exception, _func: callable, _message: Message
    ) -> None:
        """Handle watcher exceptions"""
        logger.exception("Error running watcher", exc_info=exc)

    async def handle_incoming(
        self,
        event: Union[events.NewMessage, events.MessageDeleted],
    ) -> None:
        """Handle all incoming messages"""
        message = utils.censor(getattr(event, "message", event))
        if isinstance(message, Message):
            for attr in {"text", "raw_text", "out"}:
                with contextlib.suppress(AttributeError, UnicodeDecodeError):
                    if not hasattr(message, attr):
                        setattr(message, attr, "")
        for func in self._modules.watchers:
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
        self.message.raw_text = re.sub(r"\|\| ?grep", "| grep", self.message.raw_text)
        self.message.text = re.sub(r"\|\| ?grep", "| grep", self.message.text)
        self.message.message = re.sub(r"\|\| ?grep", "| grep", self.message.message)

    def _clean_message(self) -> None:
        for attr in ["text", "raw_text", "message"]:
            setattr(
                self.message,
                attr,
                re.sub(r"\| ?grep.+", "", getattr(self.message, attr)),
            )

    def _extract_ungrep(self, grep: str) -> Optional[str]:
        ungrep_match = re.search(r"-v (.+)", grep)
        if ungrep_match:
            ungrep = ungrep_match.group(1)
            grep = re.sub(r"(.+) -v .+", r"\g<1>", grep)
            return ungrep
        return None

    def _setup_modified_methods(self, grep: str, ungrep: str) -> None:
        def process_text(text: str) -> str:
            res = []
            for line in text.split("\n"):
                if self._should_include_line(line, grep, ungrep):
                    processed_line = utils.remove_html(line, escape=True)
                    if grep:
                        processed_line = processed_line.replace(grep, f"<u>{grep}</u>")
                    res.append(processed_line)
            return self._format_result(res, grep, ungrep)

        async def modified_edit(text, *args, **kwargs):
            kwargs["parse_mode"] = "HTML"
            return await self.dispatcher.safe_api_call(
                self.message.edit(self.message, process_text(text), *args, **kwargs)
            )

        async def modified_reply(text, *args, **kwargs):
            kwargs["parse_mode"] = "HTML"
            return await self.dispatcher.safe_api_call(
                self.message.reply(process_text(text), *args, **kwargs)
            )

        async def modified_respond(text, *args, **kwargs):
            kwargs["parse_mode"] = "HTML"
            kwargs.setdefault("reply_to", utils.get_topic(self.message))
            return await self.dispatcher.safe_api_call(
                self.message.respond(process_text(text), *args, **kwargs)
            )

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
        header = "💬 <i>Lines that "
        if grep:
            header += f"contain <b>{grep}</b>"
        if grep and ungrep:
            header += " and"
        if ungrep:
            header += f" do not contain <b>{ungrep}</b>"
        header += ":</i>\n"

        return header + "\n".join(res)
