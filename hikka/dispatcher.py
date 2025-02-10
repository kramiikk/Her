# 🌟 Hikka, Friendly Telegram

# Maintainers  | Dan Gazizullin, codrago
# Years Active | 2018 - 2024
# Repository   | https://github.com/hikariatama/Hikka


import asyncio
import contextlib
import logging
import re
import time
from typing import Optional, Union, Dict, List, Callable

from hikkatl import events
from hikkatl.errors import FloodWaitError, RPCError, ChatAdminRequiredError
from hikkatl.tl.types import Message, Channel, PeerUser, PeerChannel

from . import utils
from .database import Database
from .loader import Modules
from .tl_cache import CustomTelegramClient

logger = logging.getLogger(__name__)


class SecurityError(Exception):
    pass


class MessageTags:
    """Message tags validator"""

    TAGS = {
        "deleted_message",
        "no_commands",
        "only_commands",
        "out",
        "in",
        "only_messages",
        "editable",
        "no_media",
        "only_media",
        "only_photos",
        "only_videos",
        "only_audios",
        "only_docs",
        "only_stickers",
        "only_channels",
        "only_groups",
        "only_pm",
        "no_pm",
        "no_channels",
        "no_groups",
        "no_stickers",
        "no_docs",
        "no_audios",
        "no_videos",
        "no_photos",
        "no_forwards",
        "no_reply",
        "no_mention",
        "mention",
        "only_reply",
        "only_forwards",
        "startswith",
        "endswith",
        "contains",
        "regex",
        "filter",
        "from_id",
        "chat_id",
        "thumb_url",
    }

    @staticmethod
    def check_message(m: Message) -> Dict[str, Callable[[], bool]]:
        """Returns dictionary of tag checking functions"""
        return {
            "deleted_message": lambda: getattr(m, "deleted", False),
            "out": lambda: getattr(m, "out", False),
            "in": lambda: not getattr(m, "out", True),
            "only_messages": lambda: isinstance(m, Message),
            "editable": lambda: (
                not getattr(m, "out", False)
                and not getattr(m, "fwd_from", False)
                and not getattr(m, "sticker", False)
                and not getattr(m, "via_bot_id", False)
            ),
            "no_media": lambda: not isinstance(m, Message)
            or not getattr(m, "media", False),
            "only_media": lambda: isinstance(m, Message) and getattr(m, "media", False),
            "only_photos": lambda: utils.mime_type(m).startswith("image/"),
            "only_videos": lambda: utils.mime_type(m).startswith("video/"),
            "only_audios": lambda: utils.mime_type(m).startswith("audio/"),
            "only_stickers": lambda: getattr(m, "sticker", False),
            "only_docs": lambda: getattr(m, "document", False),
            "only_channels": lambda: getattr(m, "is_channel", False)
            and not getattr(m, "is_group", False),
            "no_channels": lambda: not getattr(m, "is_channel", False),
            "only_groups": lambda: getattr(m, "is_group", False)
            and not getattr(m, "private", False)
            and not getattr(m, "is_channel", False),
            "no_groups": lambda: not getattr(m, "is_group", False)
            or getattr(m, "private", False)
            or getattr(m, "is_channel", False),
            "only_pm": lambda: getattr(m, "private", False),
            "no_pm": lambda: not getattr(m, "private", False),
            "no_forwards": lambda: not getattr(m, "fwd_from", False),
            "only_forwards": lambda: getattr(m, "fwd_from", False),
            "no_reply": lambda: not getattr(m, "reply_to_msg_id", False),
            "only_reply": lambda: getattr(m, "reply_to_msg_id", False),
            "mention": lambda: getattr(m, "mentioned", False),
            "no_mention": lambda: not getattr(m, "mentioned", False),
        }


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
        self._me = self._client.hikka_me.id

        self.raw_handlers = []

        self._api_lock = asyncio.Lock()
        self._api_calls = 0
        self._max_api_calls = 15
        self._flood_delay = 3
        self._last_reset = 0.0
        self._reset_interval = 30.0
        self._semaphore = asyncio.Semaphore(10)
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
        """Обрабатывает команды только для NewMessage"""

        if not isinstance(event, events.NewMessage):
            logger.debug("Попытка обработки неверного типа события: %s", type(event))
            return False
        if not hasattr(event, "message") or not isinstance(event.message, Message):
            logger.warning("Событие не содержит валидного сообщения")
            return False
        message = utils.censor(event.message)

        message.text = getattr(message, "text", "")
        message.raw_text = getattr(message, "raw_text", "")
        message.out = getattr(message, "out", False)

        prefix = "."
        if not message.text.startswith(prefix):
            return False
        cmd_body = message.text[len(prefix) :].strip()
        if not cmd_body:
            return False
        try:
            base_command = cmd_body.split(maxsplit=1)[0].lower()
        except IndexError:
            return False
        # Разделение команды и тега (например, "command@name")

        command_parts = base_command.split("@", 1)
        command_name = command_parts[0]

        if len(command_parts) > 1:
            if command_parts[1] != "me" and not message.out:
                return False
        txt, handler = self._modules.dispatch(command_name)
        if not handler:
            return False
        if watcher:
            return (message, prefix, txt, handler)
        if not self._is_owner(message):
            return False
        return (
            message,
            prefix,
            txt,
            handler,
        )

    def _is_owner(self, message: Message) -> bool:
        """Проверяет, является ли отправитель владельцем"""
        return getattr(message, "sender_id", None) and message.sender_id in self.owner

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
        """Handle incoming commands (only for NewMessage events)"""

        if not isinstance(event, events.NewMessage):
            return
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
                    "<emoji document_id=5877458226823302157>🕒</emoji> <b>Call</b>"
                    f" <code>{utils.escape_html(message.message)}</code>"
                    f" <b>caused FloodWait of {fw_time} on method</b>"
                    f" <code>{type(exc.request).__name__}</code>"
                )
            else:
                txt = (
                    "<emoji document_id=5877477244938489129>🚫</emoji> <b>Call</b>"
                    f" <code>{utils.escape_html(message.message)}</code>"
                    " <b>failed due to RPC error:</b>"
                    f" <code>{utils.escape_html(str(exc))}</code>"
                )
        else:
            txt = (
                "<emoji document_id=5877477244938489129>🚫</emoji><b> Call</b>"
                f" <code>{utils.escape_html(message.message)}</code>"
                "<b> failed!</b>"
            )
        with contextlib.suppress(Exception):
            await utils.answer(message, txt)

    async def watcher_exc(
        self, exc: Exception, _func: callable, message: Message
    ) -> None:
        """Handle watcher exceptions"""
        if isinstance(exc, SecurityError):
            await self._report_security_error(message, str(exc))
        else:
            logger.exception("Error running watcher", exc_info=exc)

    async def _report_security_error(self, message: Message, error: str):
        """Отправка уведомления о security error"""
        text = (
            "<emoji document_id=5452069934089649634>🚨</emoji> "
            "<b>Security Error:</b>\n"
            f"<code>{utils.escape_html(error)}</code>"
        )
        await utils.answer(message, text)

    async def _handle_tags(
        self, event: Union[events.NewMessage, events.MessageDeleted], func: callable
    ) -> bool:
        """Handle message tags"""
        return bool(await self._handle_tags_ext(event, func))

    async def _handle_tags_ext(
        self, event: Union[events.NewMessage, events.MessageDeleted], func: callable
    ) -> Optional[str]:
        """Extended tag handling with reason return"""
        message = (
            event if isinstance(event, Message) else getattr(event, "message", event)
        )

        if getattr(func, "no_commands", False):
            if await self._handle_command(event, watcher=True):
                return "no_commands"
        if getattr(func, "only_commands", False):
            if not await self._handle_command(event, watcher=True):
                return "only_commands"
        tag_checks = MessageTags.check_message(message)

        if hasattr(func, "startswith"):
            tag_checks["startswith"] = lambda: (
                isinstance(message, Message)
                and message.raw_text.startswith(func.startswith)
            )
        if hasattr(func, "endswith"):
            tag_checks["endswith"] = lambda: (
                isinstance(message, Message)
                and message.raw_text.endswith(func.endswith)
            )
        if hasattr(func, "contains"):
            tag_checks["contains"] = lambda: (
                isinstance(message, Message) and func.contains in message.raw_text
            )
        if hasattr(func, "regex"):
            tag_checks["regex"] = lambda: (
                isinstance(message, Message) and re.search(func.regex, message.raw_text)
            )
        if hasattr(func, "filter"):
            tag_checks["filter"] = lambda: callable(func.filter) and func.filter(
                message
            )
        if hasattr(func, "from_id"):
            tag_checks["from_id"] = lambda: (
                getattr(message, "sender_id", None) == func.from_id
            )
        if hasattr(func, "chat_id"):
            tag_checks["chat_id"] = lambda: utils.get_chat_id(message) == func.chat_id
        for tag in MessageTags.TAGS:
            if getattr(func, tag, False) and tag in tag_checks:
                if not tag_checks[tag]():
                    return tag
        return None

    async def _handle_deleted(self, event: events.MessageDeleted) -> None:
        """Обработчик удалённых сообщений с использованием кеша для получения sender_id"""
        try:
            # Получаем все удалённые сообщения из кеша

            messages = await self._client.get_messages(
                entity=event.chat_id,
                ids=event.deleted_ids,
            )

            for msg in messages:
                if not isinstance(msg, Message):
                    continue
                # Создаём прокси для каждого удалённого сообщения

                class DeletedMessageProxy:
                    def __init__(self, original_msg):
                        self.deleted = True
                        self.text = "[Сообщение удалено]"
                        self.raw_text = getattr(original_msg, "raw_text", "")
                        self.out = getattr(original_msg, "out", False)
                        self.media = getattr(original_msg, "media", None)
                        self.sticker = getattr(original_msg, "sticker", None)
                        self.sender_id = self._get_sender_id(original_msg)
                        self.chat_id = event.chat_id
                        self.id = getattr(original_msg, "id", None)

                    @staticmethod
                    def _get_sender_id(msg):
                        """Получаем sender_id из оригинального сообщения"""
                        if hasattr(msg, "sender_id"):
                            return msg.sender_id
                        if hasattr(msg, "from_id"):
                            if isinstance(msg.from_id, PeerUser):
                                return msg.from_id.user_id
                            if isinstance(msg.from_id, PeerChannel):
                                return msg.from_id.channel_id
                        return None

                proxy = DeletedMessageProxy(msg)

                # Отправляем в обработчики

                for watcher in self._modules.watchers:
                    asyncio.create_task(
                        self.future_dispatcher(
                            watcher,
                            proxy,
                            self.watcher_exc,
                        )
                    )
        except Exception as e:
            logger.error("Ошибка обработки удалённых сообщений: %s", e, exc_info=True)

    async def handle_incoming(
        self,
        event: Union[events.NewMessage, events.MessageDeleted],
    ) -> None:
        """Обработка всех входящих сообщений"""
        try:
            if isinstance(event, events.MessageDeleted):

                await self._handle_deleted(event)
                return
            message = utils.censor(getattr(event, "message", event))

            if isinstance(message, Message):
                message.text = getattr(message, "text", "")
                message.raw_text = getattr(message, "raw_text", "")
                message.out = getattr(message, "out", False)
                message.media = getattr(message, "media", None)
                if hasattr(message, "from_id"):
                    if isinstance(message.from_id, PeerUser):
                        sender_id = message.from_id.user_id
                    elif isinstance(message.from_id, PeerChannel):
                        sender_id = message.from_id.channel_id
                    else:
                        sender_id = None
                else:
                    sender_id = None
                message.sender_id = getattr(message, "sender_id", None) or sender_id
            for watcher in self._modules.watchers:
                asyncio.create_task(
                    self.future_dispatcher(
                        watcher,
                        message,
                        self.watcher_exc,
                    )
                )
            await self.handle_command(event)
        except Exception as e:
            logger.error("Ошибка обработки сообщения: %s", e, exc_info=True)

    async def future_dispatcher(
        self, func: callable, message: Message, exception_handler: callable, *args
    ) -> None:
        """Dispatch function execution to the future"""
        try:
            async with self._semaphore:
                await func(message, *args)
        except Exception as e:
            await exception_handler(e, func, message)


class GrepHandler:
    """Handles grep-like filtering with full regex support"""

    def __init__(self, message: Message, dispatcher: CommandDispatcher):
        if hasattr(message, "hikka_grepped") and message.hikka_grepped:
            raise SecurityError("Message already processed")
        self.message = message
        self.dispatcher = dispatcher
        self._compiled_grep = None
        self._compiled_ungrep = None

        try:
            self._process_grep()
            self._modify_message_methods()
        except SecurityError as e:
            raise  # Пробрасываем для обработки выше
        except Exception as e:
            logger.error(f"GrepHandler init error: {str(e)}")

    def _process_grep(self):
        """Main grep processing logic with regex support"""
        raw_text = getattr(self.message, "raw_text", "")

        # 1. Проверка на экранированный grep

        if re.search(r"\|\| ?grep", raw_text):
            self._handle_escaped_grep()
            return
        # 2. Извлечение паттерна

        grep_match = re.search(r"\| ?grep (.+)", raw_text)
        if not grep_match:
            return
        # 3. Парсинг параметров

        full_pattern = grep_match.group(1)
        pattern_parts = full_pattern.split(" -v ", 1)

        try:
            # 4. Компиляция основного паттерна

            self._compiled_grep = re.compile(
                pattern_parts[0], flags=re.IGNORECASE if "i" in pattern_parts[0] else 0
            )
        except re.error as e:
            raise SecurityError(f"Invalid regex: {str(e)}") from e
        # 5. Обработка инверсного поиска (-v)

        if len(pattern_parts) > 1:
            try:
                self._compiled_ungrep = re.compile(pattern_parts[1])
            except re.error as e:
                raise SecurityError(f"Invalid inverse regex: {str(e)}") from e
        # 6. Очистка исходного сообщения

        self._clean_original_message()

    def _handle_escaped_grep(self):
        """Обработка экранированного grep с помощью ||"""
        self.message.raw_text = re.sub(r"\|\| ?grep", "| grep", self.message.raw_text)
        self.message.text = self.message.raw_text

    def _clean_original_message(self):
        """Очистка исходного сообщения от grep-параметров"""
        self.message.raw_text = re.sub(
            r"\| ?grep .+", "", self.message.raw_text, flags=re.DOTALL
        )
        self.message.text = self.message.raw_text

    def _modify_message_methods(self):
        """Модифицируем методы ответа для поддержки regex"""

        def process_content(text: str) -> str:
            if not self._compiled_grep:
                return text
            result = []
            for line in text.split("\n"):
                # Проверка основного паттерна

                grep_match = self._compiled_grep.search(line)

                # Проверка инверсного паттерна

                ungrep_match = (
                    self._compiled_ungrep.search(line)
                    if self._compiled_ungrep
                    else False
                )

                if grep_match and not ungrep_match:
                    # Подсветка совпадений

                    highlighted = self._compiled_grep.sub(
                        lambda m: f"<u>{utils.escape_html(m.group(0))}</u>",
                        utils.escape_html(line),
                    )
                    result.append(highlighted)
            return self._format_result(
                result,
                self._compiled_grep.pattern if self._compiled_grep else None,
                self._compiled_ungrep.pattern if self._compiled_ungrep else None,
            )

        # Модификация методов сообщения

        original_edit = self.message.edit
        original_reply = self.message.reply

        async def new_edit(text: str, *args, **kwargs):
            return await original_edit(
                process_content(text), parse_mode="HTML", *args, **kwargs
            )

        async def new_reply(text: str, *args, **kwargs):
            return await original_reply(
                process_content(text), parse_mode="HTML", *args, **kwargs
            )

        self.message.edit = new_edit
        self.message.reply = new_reply
        self.message.respond = new_reply
        self.message.hikka_grepped = True

    @staticmethod
    def _format_result(
        res: List[str], grep: Optional[str], ungrep: Optional[str]
    ) -> str:
        if not res:
            conditions = []
            if grep:
                conditions.append(f"match <b>{utils.escape_html(grep)}</b>")
            if ungrep:
                conditions.append(f"do not match <b>{utils.escape_html(ungrep)}</b>")
            return f"💬 <i>No lines that {' and '.join(conditions)}</i>"
        header = "💬 <i>Lines that "
        if grep:
            header += f"match <b>{utils.escape_html(grep)}</b>"
        if grep and ungrep:
            header += " and"
        if ungrep:
            header += f" do not match <b>{utils.escape_html(ungrep)}</b>"
        header += ":</i>\n"
        return header + "\n".join(res)
