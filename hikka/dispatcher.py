"""Processes incoming events and dispatches them to appropriate handlers"""

# Friendly Telegram (telegram userbot)
# Copyright (C) 2018-2019 The Authors
# ©️ Dan Gazizullin, codrago 2021-2024
# This file is a part of Her
# 🌐 https://github.com/hikariatama/Hikka

import asyncio
import collections
import contextlib
import inspect
import logging
import re
import sys
import typing

from hikkatl import events
from hikkatl.errors import FloodWaitError, RPCError
from hikkatl.tl.types import Message

from . import main, security, utils
from .database import Database
from ._internal import fw_protect
from .loader import Modules
from .tl_cache import CustomTelegramClient

logger = logging.getLogger(__name__)

# Keys for layout switch
ru_keys = 'ёйцукенгшщзхъфывапролджэячсмитьбю.Ё"№;%:?ЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭ/ЯЧСМИТЬБЮ,'
en_keys = "`qwertyuiop[]asdfghjkl;'zxcvbnm,./~@#$%^&QWERTYUIOP{}ASDFGHJKL:\"|ZXCVBNM<>?"
ALL_TAGS = [
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
]


def _decrement_ratelimit(delay, data, key, severity):
    def inner():
        data[key] = max(0, data[key] - severity)

    asyncio.get_event_loop().call_later(delay, inner)


class CommandDispatcher:
    def __init__(
        self,
        modules: Modules,
        client: CustomTelegramClient,
        db: Database,
    ):
        self._modules = modules
        self._client = client
        self.client = client
        self._db = db

        self._ratelimit_storage_user = collections.defaultdict(int)
        self._ratelimit_storage_chat = collections.defaultdict(int)
        self._ratelimit_storage_api = collections.defaultdict(int)
        self._ratelimit_max_user = db.get(__name__, "ratelimit_max_user", 30)
        self._ratelimit_max_chat = db.get(__name__, "ratelimit_max_chat", 100)
        self._ratelimit_max_api = db.get(__name__, "ratelimit_max_api", 20)
        self._ratelimit_delay_api = db.get(__name__, "ratelimit_delay_api", 1)

        self.security = security.SecurityManager(client, db)

        self.check_security = self.security.check
        self._me = self._client.hikka_me.id

        self.raw_handlers = []
    
    async def _handle_ratelimit_api(self, user_id: int, fw_protection: bool = True) -> bool:
        """Handles ratelimit and uses fw_protect"""
        if not user_id:
            return True

        count = self._ratelimit_storage_api[user_id]
        if count >= self._ratelimit_max_api:
            if fw_protection: # Используем защиту от FloodWait
                await fw_protect()
            await asyncio.sleep(self._ratelimit_delay_api)
            self._ratelimit_storage_api[user_id] = 0
            return False

        self._ratelimit_storage_api[user_id] += 1
        return True

    async def _handle_ratelimit(self, message: Message, func: callable) -> bool:
        if await self.security.check(message, security.OWNER):
            return True

        func = getattr(func, "__func__", func)
        ret = True
        chat = self._ratelimit_storage_chat[message.chat_id]

        if message.sender_id:
            user = self._ratelimit_storage_user[message.sender_id]
            severity = (5 if getattr(func, "ratelimit", False) else 2) * (
                (user + chat) // 30 + 1
            )
            user += severity
            self._ratelimit_storage_user[message.sender_id] = user
            if user > self._ratelimit_max_user:
                ret = False
            else:
                self._ratelimit_storage_chat[message.chat_id] = chat

            _decrement_ratelimit(
                self._ratelimit_max_user * severity,
                self._ratelimit_storage_user,
                message.sender_id,
                severity,
            )
        else:
            severity = (5 if getattr(func, "ratelimit", False) else 2) * (
                chat // 15 + 1
            )

        chat += severity

        if chat > self._ratelimit_max_chat:
            ret = False

        _decrement_ratelimit(
            self._ratelimit_max_chat * severity,
            self._ratelimit_storage_chat,
            message.chat_id,
            severity,
        )

        return ret

    def _handle_grep(self, message: Message) -> Message:
        # Allow escaping grep with double stick
        if "||grep" in message.text or "|| grep" in message.text:
            message.raw_text = re.sub(r"\|\| ?grep", "| grep", message.raw_text)
            message.text = re.sub(r"\|\| ?grep", "| grep", message.text)
            message.message = re.sub(r"\|\| ?grep", "| grep", message.message)
            return message

        grep = False
        if not re.search(r".+\| ?grep (.+)", message.raw_text):
            return message

        grep = re.search(r".+\| ?grep (.+)", message.raw_text).group(1)
        message.text = re.sub(r"\| ?grep.+", "", message.text)
        message.raw_text = re.sub(r"\| ?grep.+", "", message.raw_text)
        message.message = re.sub(r"\| ?grep.+", "", message.message)

        ungrep = False

        if re.search(r"-v (.+)", grep):
            ungrep = re.search(r"-v (.+)", grep).group(1)
            grep = re.sub(r"(.+) -v .+", r"\g<1>", grep)

        grep = utils.escape_html(grep).strip() if grep else False
        ungrep = utils.escape_html(ungrep).strip() if ungrep else False

        def process_text(text: str) -> str:
            nonlocal grep, ungrep
            res = []

            for line in text.split("\n"):
                if (
                    grep
                    and grep in utils.remove_html(line)
                    and (not ungrep or ungrep not in utils.remove_html(line))
                ):
                    res.append(
                        utils.remove_html(line, escape=True).replace(
                            grep, f"<u>{grep}</u>"
                        )
                    )

                if not grep and ungrep and ungrep not in utils.remove_html(line):
                    res.append(utils.remove_html(line, escape=True))

            cont = (
                (f"contain <b>{grep}</b>" if grep else "")
                + (" and" if grep and ungrep else "")
                + ((" do not contain <b>" + ungrep + "</b>") if ungrep else "")
            )

            if res:
                text = f"<i>💬 Lines that {cont}:</i>\n" + "\n".join(res)
            else:
                text = f"💬 <i>No lines that {cont}</i>"

            return text

        async def my_edit(text, *args, **kwargs):
            text = process_text(text)
            kwargs["parse_mode"] = "HTML"
            return await utils.answer(message, text, *args, **kwargs)

        async def my_reply(text, *args, **kwargs):
            text = process_text(text)
            kwargs["parse_mode"] = "HTML"
            return await message.reply(text, *args, **kwargs)

        async def my_respond(text, *args, **kwargs):
            text = process_text(text)
            kwargs["parse_mode"] = "HTML"
            kwargs.setdefault("reply_to", utils.get_topic(message))
            return await message.respond(text, *args, **kwargs)

        message.edit = my_edit
        message.reply = my_reply
        message.respond = my_respond
        message.hikka_grepped = True

        return message

    async def _handle_command(
        self,
        event: typing.Union[events.NewMessage, events.MessageDeleted],
        watcher: bool = False,
    ) -> typing.Union[bool, typing.Tuple[Message, str, str, callable]]:
        if not hasattr(event, "message") or not hasattr(event.message, "message"):
            return False

        prefix = self._db.get(main.__name__, "command_prefix", False) or "."
        change = str.maketrans(ru_keys + en_keys, en_keys + ru_keys)
        message = utils.censor(event.message)

        if not event.message.message:
            return False

        if (
            message.out
            and len(message.message) > len(prefix) * 2
            and (
                message.message.startswith(prefix * 2)
                and any(s != prefix for s in message.message)
                or message.message.startswith(str.translate(prefix * 2, change))
                and any(s != str.translate(prefix, change) for s in message.message)
            )
        ):
            # Allow escaping commands using .'s
            if not watcher:
                await utils.answer(
                    message,
                    message.message[len(prefix):],
                    parse_mode=lambda s: (
                        s,
                        utils.relocate_entities(message.entities, -1, message.message)
                        or (),
                    ),
                )
            return False

        if (
            event.message.message.startswith(str.translate(prefix, change))
            and str.translate(prefix, change) != prefix
        ):
            message.message = str.translate(message.message, change)
            message.text = str.translate(message.text, change)
        elif not event.message.message.startswith(prefix):
            return False

        if (
            event.sticker
            or event.dice
            or event.audio
            or event.via_bot_id
            or getattr(event, "reactions", False)
        ):
            return False

        if not message.message or len(message.message) == len(prefix):
            return False  # Message is just the prefix

        command = message.message[len(prefix):].strip().split(maxsplit=1)[0]
        tag = command.split("@", maxsplit=1)

        if len(tag) == 2:
            if tag[1] == "me":
                if not message.out:
                    return False
        elif (
            event.out
            or event.mentioned
            and event.message is not None
            and event.message.message is not None
        ):
            pass
        elif (
            not event.is_private
        ):
            return False

        txt, func = self._modules.dispatch(tag[0])

        if (
            not func
            or not await self._handle_ratelimit(message, func)
        ):
            return False

        if message.is_channel and message.edit_date and not message.is_group and not message.out:
            return False
        message.message = prefix + txt + message.message[len(prefix + command) :]

        if await self._handle_tags(event, func):
            return False

        if self._db.get(main.__name__, "grep", False) and not watcher:
            message = self._handle_grep(message)

        return message, prefix, txt, func

    async def handle_raw(self, event: events.Raw):
        """Handle raw events."""
        for handler in self.raw_handlers:
            if isinstance(event, tuple(handler.updates)):
                try:
                    await handler(event)
                except Exception as e:
                    logger.exception("Error in raw handler %s: %s", handler.id, e)

    async def handle_command(
        self,
        event: typing.Union[events.NewMessage, events.MessageDeleted],
    ):
        """Handle all commands"""
        message = await self._handle_command(event)
        if not message:
            return

        message, _, _, func = message

        if not await self._handle_ratelimit_api(message.sender_id):
            logger.debug("Too many api request, skipping")
            return # Добавили return, чтобы не выполнялась команда

        asyncio.ensure_future(
            self.future_dispatcher(
                func,
                message,
                self.command_exc,
            )
        )

    async def command_exc(self, _, message: Message):
        """Handle command exceptions."""
        exc = sys.exc_info()[1]
        logger.exception("Command failed", extra={"stack": inspect.stack()})
        if isinstance(exc, RPCError):
            if isinstance(exc, FloodWaitError):
                hours = exc.seconds // 3600
                minutes = (exc.seconds % 3600) // 60
                seconds = exc.seconds % 60
                hours = f"{hours} hours, " if hours else ""
                minutes = f"{minutes} minutes, " if minutes else ""
                seconds = f"{seconds} seconds" if seconds else ""
                fw_time = f"{hours}{minutes}{seconds}"
                txt = (
                    "<emoji document_id=5877458226823302157>🕒</emoji> <b>Call</b> <code>{}</code> <b>caused FloodWait of {} on method</b> <code>{}</code>"
                    .format(
                        utils.escape_html(message.message),
                        fw_time,
                        type(exc.request).__name__,
                    )
                )
            else:
                txt = (
                    "<emoji document_id=5877477244938489129>🚫</emoji> <b>Call</b> <code>{}</code> <b>failed due to RPC error:</b> <code>{}</code>"
                    .format(
                        utils.escape_html(message.message),
                        utils.escape_html(str(exc)),
                    )
                )
        else:
            txt = (
                "<emoji document_id=5877477244938489129>🚫</emoji><b> Call</b>"
                f" <code>{utils.escape_html(message.message)}</code><b>"
                " failed!</b>"
            )

        with contextlib.suppress(Exception):
            await message.reply(txt)

    async def watcher_exc(self, *_):
        logger.exception("Error running watcher", extra={"stack": inspect.stack()})

    async def _handle_tags(
        self,
        event: typing.Union[events.NewMessage, events.MessageDeleted],
        func: callable,
    ) -> bool:
        return bool(await self._handle_tags_ext(event, func))

    async def _handle_tags_ext(
        self,
        event: typing.Union[events.NewMessage, events.MessageDeleted],
        func: callable,
    ) -> str:
        """
        Handle tags.
        :param event: The event to handle.
        :param func: The function to handle.
        :return: The reason for the tag to fail.
        """
        m = event if isinstance(event, Message) else getattr(event, "message", event)

        reverse_mapping = {
            "out": lambda: getattr(m, "out", True),
            "in": lambda: not getattr(m, "out", True),
            "only_messages": lambda: isinstance(m, Message),
            "editable": (
                lambda: not getattr(m, "out", False)
                and not getattr(m, "fwd_from", False)
                and not getattr(m, "sticker", False)
                and not getattr(m, "via_bot_id", False)
            ),
            "no_media": lambda: (
                not isinstance(m, Message) or not getattr(m, "media", False)
            ),
            "only_media": lambda: isinstance(m, Message) and getattr(m, "media", False),
            "only_photos": lambda: utils.mime_type(m).startswith("image/"),
            "only_videos": lambda: utils.mime_type(m).startswith("video/"),
            "only_audios": lambda: utils.mime_type(m).startswith("audio/"),
            "only_stickers": lambda: getattr(m, "sticker", False),
            "only_docs": lambda: getattr(m, "document", False),
            "only_channels": lambda: (
                getattr(m, "is_channel", False) and not getattr(m, "is_group", False)
            ),
            "no_channels": lambda: not getattr(m, "is_channel", False),
            "no_groups": (
                lambda: not getattr(m, "is_group", False)
                or getattr(m, "private", False)
                or getattr(m, "is_channel", False)
            ),
            "only_groups": (
                lambda: getattr(m, "is_group", False)
                or not getattr(m, "private", False)
                and not getattr(m, "is_channel", False)
            ),
            "no_pm": lambda: not getattr(m, "private", False),
            "only_pm": lambda: getattr(m, "private", False),
            "no_stickers": lambda: not getattr(m, "sticker", False),
            "no_docs": lambda: not getattr(m, "document", False),
            "no_audios": lambda: not utils.mime_type(m).startswith("audio/"),
            "no_videos": lambda: not utils.mime_type(m).startswith("video/"),
            "no_photos": lambda: not utils.mime_type(m).startswith("image/"),
            "no_forwards": lambda: not getattr(m, "fwd_from", False),
            "no_reply": lambda: not getattr(m, "reply_to_msg_id", False),
            "only_forwards": lambda: getattr(m, "fwd_from", False),
            "only_reply": lambda: getattr(m, "reply_to_msg_id", False),
            "mention": lambda: getattr(m, "mentioned", False),
            "no_mention": lambda: not getattr(m, "mentioned", False),
            "startswith": lambda: (
                isinstance(m, Message) and m.raw_text.startswith(func.startswith)
            ),
            "endswith": lambda: (
                isinstance(m, Message) and m.raw_text.endswith(func.endswith)
            ),
            "contains": lambda: isinstance(m, Message) and func.contains in m.raw_text,
            "filter": lambda: callable(func.filter) and func.filter(m),
            "from_id": lambda: getattr(m, "sender_id", None) == func.from_id,
            "chat_id": lambda: utils.get_chat_id(m)
            == (
                func.chat_id
                if not str(func.chat_id).startswith("-100")
                else int(str(func.chat_id)[4:])
            ),
            "regex": lambda: (
                isinstance(m, Message) and re.search(func.regex, m.raw_text)
            ),
        }

        return (
            "no_commands"
            if getattr(func, "no_commands", False)
            and await self._handle_command(event, watcher=True)
            else (
                "only_commands"
                if getattr(func, "only_commands", False)
                and not await self._handle_command(event, watcher=True)
                else next(
                    (
                        tag
                        for tag in ALL_TAGS
                        if getattr(func, tag, False)
                        and tag in reverse_mapping
                        and not reverse_mapping[tag]()
                    ),
                    None,
                )
            )
        )

    async def handle_incoming(
        self,
        event: typing.Union[events.NewMessage, events.MessageDeleted],
    ):
        """Handle all incoming messages"""
        message = utils.censor(getattr(event, "message", event))

        for func in self._modules.watchers:
            # Avoid weird AttributeErrors in weird dochub modules by settings placeholder
            # of attributes
            for placeholder in {"text", "raw_text", "out"}:
                try:
                    if not hasattr(message, placeholder):
                        setattr(message, placeholder, "")
                except UnicodeDecodeError:
                    pass

            # Run watcher via ensure_future so in case user has a lot
            # of watchers with long actions, they can run simultaneously
            asyncio.ensure_future(
                self.future_dispatcher(
                    func,
                    message,
                    self.watcher_exc,
                )
            )

    async def future_dispatcher(
        self,
        func: callable,
        message: typing.Union[Message, events.Event],
        exception_handler: callable,
        *args,
    ):
        user_id = None
        if isinstance(message, Message):
            user_id = message.sender_id
        else:
            user_id = getattr(message, "sender_id", None) or getattr(message, "user_id", None)

        if user_id is not None:
            if not await self._handle_ratelimit_api(user_id):
                logger.debug("Too many api requests, skipping")
                return

        try:
            await func(message)
        except Exception as e:
            await exception_handler(e, message, *args)
