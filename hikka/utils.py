"""Utilities"""

# 🌟 Hikka, Friendly Telegram

# Maintainers  | Dan Gazizullin, codrago
# Years Active | 2018 - 2024
# Repository   | https://github.com/hikariatama/Hikka


import asyncio
import atexit as _atexit
import functools
import inspect
import io
import json
import logging
import os
import random
import re
import shlex
import signal
import time
import typing
from datetime import timedelta
from urllib.parse import urlparse

import git
import grapheme
import hikkatl
import requests
from hikkatl import hints
from hikkatl.tl.custom.message import Message
from hikkatl.tl.functions.account import UpdateNotifySettingsRequest
from hikkatl.tl.functions.channels import (
    EditPhotoRequest,
)
from hikkatl.tl.types import (
    Channel,
    Chat,
    InputDocument,
    InputPeerNotifySettings,
    MessageEntityBankCard,
    MessageEntityBlockquote,
    MessageEntityBold,
    MessageEntityBotCommand,
    MessageEntityCashtag,
    MessageEntityCode,
    MessageEntityEmail,
    MessageEntityHashtag,
    MessageEntityItalic,
    MessageEntityMention,
    MessageEntityMentionName,
    MessageEntityPhone,
    MessageEntityPre,
    MessageEntitySpoiler,
    MessageEntityStrike,
    MessageEntityTextUrl,
    MessageEntityUnderline,
    MessageEntityUnknown,
    MessageEntityUrl,
    MessageMediaWebPage,
    PeerChannel,
    PeerChat,
    PeerUser,
    User,
)

from .tl_cache import CustomTelegramClient
from .types import ListLike, Module

FormattingEntity = typing.Union[
    MessageEntityUnknown,
    MessageEntityMention,
    MessageEntityHashtag,
    MessageEntityBotCommand,
    MessageEntityUrl,
    MessageEntityEmail,
    MessageEntityBold,
    MessageEntityItalic,
    MessageEntityCode,
    MessageEntityPre,
    MessageEntityTextUrl,
    MessageEntityMentionName,
    MessageEntityPhone,
    MessageEntityCashtag,
    MessageEntityUnderline,
    MessageEntityStrike,
    MessageEntityBlockquote,
    MessageEntityBankCard,
    MessageEntitySpoiler,
]

emoji_pattern = re.compile(
    "["
    "\U0001f600-\U0001f64f"  # emoticons
    "\U0001f300-\U0001f5ff"  # symbols & pictographs
    "\U0001f680-\U0001f6ff"  # transport & map symbols
    "\U0001f1e0-\U0001f1ff"  # flags (iOS)
    "]+",
    flags=re.UNICODE,
)

parser = hikkatl.utils.sanitize_parse_mode("html")
logger = logging.getLogger(__name__)


def get_args(message: typing.Union[Message, str]) -> typing.List[str]:
    if not (message := getattr(message, "message", message)):
        return False
    if len(message := message.split(maxsplit=1)) <= 1:
        return []
    message = message[1]

    try:
        split = shlex.split(message)
    except ValueError:
        return message  # Cannot split, let's assume that it's just one long message
    return list(filter(lambda x: len(x) > 0, split))


def get_args_raw(message: typing.Union[Message, str]) -> str:
    """Get the parameters to the command as a raw string (not split)"""
    if not (message := getattr(message, "message", message)):
        return False
    return args[1] if len(args := message.split(maxsplit=1)) > 1 else ""


def get_args_html(message: Message) -> str:
    """Get the parameters to the command as string with HTML (not split)"""
    prefix = "."

    if not (message := message.text):
        return False
    if prefix not in message:
        return message
    raw_text, entities = parser.parse(message)

    raw_text = parser._add_surrogate(raw_text)

    try:
        command = raw_text[
            raw_text.index(prefix) : raw_text.index(" ", raw_text.index(prefix) + 1)
        ]
    except ValueError:
        return ""
    command_len = len(command) + 1

    return parser.unparse(
        parser._del_surrogate(raw_text[command_len:]),
        relocate_entities(entities, -command_len, raw_text[command_len:]),
    )


def get_args_split_by(
    message: typing.Union[Message, str],
    separator: str,
) -> typing.List[str]:
    """Split args with a specific separator"""
    return [
        section.strip() for section in get_args_raw(message).split(separator) if section
    ]


def get_chat_id(message: Message) -> int:
    """Get the chat ID, but without -100 if its a channel"""
    return hikkatl.utils.resolve_id(
        getattr(message, "chat_id", None)
        or getattr(getattr(message, "chat", None), "id", None)
    )[0]


def get_entity_id(entity: hints.Entity) -> int:
    """Get entity ID"""
    return hikkatl.utils.get_peer_id(entity)


def escape_html(text: str, /) -> str:  # sourcery skip
    """Pass all untrusted/potentially corrupt input here"""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def escape_quotes(text: str, /) -> str:
    """Escape quotes to html quotes"""
    return escape_html(text).replace('"', "&quot;")


def get_base_dir() -> str:
    """Get directory of current file"""
    return os.path.dirname(os.path.abspath(__file__))


async def get_user(message: Message) -> typing.Optional[User]:
    """Get user who sent message, searching if not found easily"""
    try:
        return await message.get_sender()
    except ValueError:  # Not in database. Lets go looking for them.
        logger.warning("User not in session cache. Searching...")
    if isinstance(message.peer_id, PeerUser):
        await message.client.get_dialogs()
        return await message.get_sender()
    if isinstance(message.peer_id, (PeerChannel, PeerChat)):
        async for user in message.client.iter_participants(
            message.peer_id,
            aggressive=True,
        ):
            if user.id == message.sender_id:
                return user
        logger.error("User isn't in the group where they sent the message")
        return None
    logger.error("`peer_id` is not a user, chat or channel")
    return None


def run_sync(func, *args, **kwargs):
    """Run a non-async function in a new thread and return an awaitable"""
    return asyncio.get_event_loop().run_in_executor(
        None,
        functools.partial(func, *args, **kwargs),
    )


def run_async(loop: asyncio.AbstractEventLoop, coro: typing.Awaitable) -> typing.Any:
    """Run an async function as a non-async function, blocking till it's done"""
    return asyncio.run_coroutine_threadsafe(coro, loop).result()


def censor(
    obj: typing.Any,
    to_censor: typing.Optional[typing.Iterable[str]] = None,
    replace_with: str = "redacted_{count}_chars",
):
    """May modify the original object, but don't rely on it"""
    if to_censor is None:
        to_censor = ["phone"]
    for k, v in vars(obj).items():
        if k in to_censor:
            setattr(obj, k, replace_with.format(count=len(v)))
        elif k[0] != "_" and hasattr(v, "__dict__"):
            setattr(obj, k, censor(v, to_censor, replace_with))
    return obj


def relocate_entities(
    entities: typing.List[FormattingEntity],
    offset: int,
    text: typing.Optional[str] = None,
) -> typing.List[FormattingEntity]:
    """Move all entities by offset (truncating at text)"""
    length = len(text) if text is not None else 0

    for ent in entities.copy() if entities else ():
        ent.offset += offset
        if ent.offset < 0:
            ent.length += ent.offset
            ent.offset = 0
        if text is not None and ent.offset + ent.length > length:
            ent.length = length - ent.offset
        if ent.length <= 0:
            entities.remove(ent)
    return entities


async def answer_file(
    message: Message,
    file: typing.Union[str, bytes, io.IOBase, InputDocument],
    caption: typing.Optional[str] = None,
    **kwargs,
):
    """
    Use this to answer a message with a document

    :example:
        >>> await utils.answer_file(message, "test.txt")
        >>> await utils.answer_file(
            message,
            "artai.jpg",
            "This is the cool module, check it out!",
        )
    """
    if topic := get_topic(message):
        kwargs.setdefault("reply_to", topic)
    try:
        response = await message.client.send_file(
            message.peer_id,
            file,
            caption=caption,
            **kwargs,
        )
    except Exception:
        if caption:
            logger.warning(
                "Failed to send file, sending plain text instead", exc_info=True
            )
            return await answer(message, caption, **kwargs)
        raise
    return response


async def answer(
    message: Message,
    response: str,
    **kwargs,
) -> Message:
    """Use this to give the response to a command"""

    if isinstance(message, list) and message:
        message = message[0]
    kwargs.setdefault("link_preview", False)

    if not (edit := (message.out and not message.fwd_from)):
        kwargs.setdefault(
            "reply_to",
            getattr(message, "reply_to_msg_id", None),
        )
    elif "reply_to" in kwargs:
        kwargs.pop("reply_to")
    parse_mode = hikkatl.utils.sanitize_parse_mode(
        kwargs.pop(
            "parse_mode",
            message.client.parse_mode,
        )
    )

    if isinstance(response, str) and not kwargs.pop("asfile", False):
        text, entities = parse_mode.parse(response)

        if len(text) >= 4096 and not hasattr(message, "hikka_grepped"):
            file = io.BytesIO(text.encode("utf-8"))
            file.name = "command_result.txt"

            result = await message.client.send_file(
                message.peer_id,
                file,
                caption="too long",
                reply_to=kwargs.get("reply_to") or get_topic(message),
            )

            return result
        result = await (message.edit if edit else message.respond)(
            text,
            parse_mode=lambda t: (t, entities),
            **kwargs,
        )
    elif isinstance(response, Message):
        if message.media is None and (
            response.media is None or isinstance(response.media, MessageMediaWebPage)
        ):
            result = await message.edit(
                response.message,
                parse_mode=lambda t: (t, response.entities or []),
                link_preview=isinstance(response.media, MessageMediaWebPage),
            )
        else:
            result = await message.respond(response, **kwargs)
    else:
        if isinstance(response, bytes):
            response = io.BytesIO(response)
        elif isinstance(response, str):
            response = io.BytesIO(response.encode("utf-8"))
        if name := kwargs.pop("filename", None):
            response.name = name
        if message.media is not None and edit:
            await message.edit(file=response, **kwargs)
        else:
            kwargs.setdefault(
                "reply_to",
                getattr(message, "reply_to_msg_id", get_topic(message)),
            )
            result = await message.client.send_file(message.peer_id, response, **kwargs)
    return result


async def get_target(message: Message, arg_no: int = 0) -> typing.Optional[int]:
    """Get target from message"""

    if any(
        isinstance(entity, MessageEntityMentionName)
        for entity in (message.entities or [])
    ):
        e = sorted(
            filter(lambda x: isinstance(x, MessageEntityMentionName), message.entities),
            key=lambda x: x.offset,
        )[0]
        return e.user_id
    if len(get_args(message)) > arg_no:
        user = get_args(message)[arg_no]
    elif message.is_reply:
        return (await message.get_reply_message()).sender_id
    elif hasattr(message.peer_id, "user_id"):
        user = message.peer_id.user_id
    else:
        return None
    try:
        entity = await message.client.get_entity(user)
    except ValueError:
        return None
    else:
        if isinstance(entity, User):
            return entity.id


def merge(a: dict, b: dict, /) -> dict:
    """Merge with replace dictionary a to dictionary b"""
    for key in a:
        if key in b:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                b[key] = merge(a[key], b[key])
            elif isinstance(a[key], list) and isinstance(b[key], list):
                b[key] = list(set(b[key] + a[key]))
            else:
                b[key] = a[key]
        b[key] = a[key]
    return b


async def set_avatar(
    client: CustomTelegramClient,
    peer: hints.Entity,
    avatar: str,
) -> bool:
    """Sets an entity avatar"""
    if isinstance(avatar, str) and check_url(avatar):
        f = (
            await run_sync(
                requests.get,
                avatar,
            )
        ).content
    elif isinstance(avatar, bytes):
        f = avatar
    else:
        return False
    await client(
        EditPhotoRequest(
            channel=peer,
            photo=await client.upload_file(f, file_name="photo.png"),
        )
    )

    return True


async def dnd(
    client: CustomTelegramClient,
    peer: hints.Entity,
    archive: bool = True,
) -> bool:
    """Mutes and optionally archives peer"""
    try:
        await client(
            UpdateNotifySettingsRequest(
                peer=peer,
                settings=InputPeerNotifySettings(
                    show_previews=False,
                    silent=True,
                    mute_until=2**31 - 1,
                ),
            )
        )

        if archive:
            await client.edit_folder(peer, 1)
    except Exception:
        logger.exception("utils.dnd error")
        return False
    return True


def get_link(user: typing.Union[User, Channel], /) -> str:
    """Get telegram permalink to entity"""
    return (
        f"tg://user?id={user.id}"
        if isinstance(user, User)
        else (
            f"tg://resolve?domain={user.username}"
            if getattr(user, "username", None)
            else ""
        )
    )


def chunks(_list: ListLike, n: int, /) -> typing.List[typing.List[typing.Any]]:
    """Split provided `_list` into chunks of `n`"""
    return [_list[i : i + n] for i in range(0, len(_list), n)]


def uptime() -> str:
    """Returns formatted uptime including days if applicable."""
    total_seconds = round(time.perf_counter() - init_ts)
    days, remainder = divmod(total_seconds, 86400)
    time_formatted = str(timedelta(seconds=remainder))
    if days > 0:
        return f"{days} day(s), {time_formatted}"
    return time_formatted


def array_sum(
    array: typing.List[typing.List[typing.Any]], /
) -> typing.List[typing.Any]:
    """Performs basic sum operation on array"""
    result = []
    for item in array:
        result += item
    return result


def rand(size: int, /) -> str:
    """Return random string of len `size`"""
    return "".join(
        [random.choice("abcdefghijklmnopqrstuvwxyz1234567890") for _ in range(size)]
    )


def smart_split(
    text: str,
    entities: typing.List[FormattingEntity],
    length: int = 4096,
    split_on: ListLike = ("\n", " "),
    min_length: int = 1,
) -> typing.Iterator[str]:
    """
    Split the message into smaller messages.
    A grapheme will never be broken. Entities will be displaced to match the right location. No inputs will be mutated.
    The end of each message except the last one is stripped of characters from [split_on]

    :example:
        >>> utils.smart_split(
            *hikkatl.extensions.html.parse(
                "<b>Hello, world!</b>"
            )
        )
        <<< ["<b>Hello, world!</b>"]
    """

    encoded = text.encode("utf-16le")
    pending_entities = entities
    text_offset = 0
    bytes_offset = 0
    text_length = len(text)
    bytes_length = len(encoded)

    while text_offset < text_length:
        if bytes_offset + length * 2 >= bytes_length:
            yield parser.unparse(
                text[text_offset:],
                list(sorted(pending_entities, key=lambda x: x.offset)),
            )
            break
        codepoint_count = len(
            encoded[bytes_offset : bytes_offset + length * 2].decode(
                "utf-16le",
                errors="ignore",
            )
        )

        for search in split_on:
            search_index = text.rfind(
                search,
                text_offset + min_length,
                text_offset + codepoint_count,
            )
            if search_index != -1:
                break
        else:
            search_index = text_offset + codepoint_count
        split_index = grapheme.safe_split_index(text, search_index)

        split_offset_utf16 = (
            len(text[text_offset:split_index].encode("utf-16le"))
        ) // 2
        exclude = 0

        while (
            split_index + exclude < text_length
            and text[split_index + exclude] in split_on
        ):
            exclude += 1
        current_entities = []
        entities = pending_entities.copy()
        pending_entities = []

        for entity in entities:
            if (
                entity.offset < split_offset_utf16
                and entity.offset + entity.length > split_offset_utf16 + exclude
            ):
                # spans boundary

                current_entities.append(
                    _copy_tl(
                        entity,
                        length=split_offset_utf16 - entity.offset,
                    )
                )
                pending_entities.append(
                    _copy_tl(
                        entity,
                        offset=0,
                        length=entity.offset
                        + entity.length
                        - split_offset_utf16
                        - exclude,
                    )
                )
            elif entity.offset < split_offset_utf16 < entity.offset + entity.length:
                # overlaps boundary

                current_entities.append(
                    _copy_tl(
                        entity,
                        length=split_offset_utf16 - entity.offset,
                    )
                )
            elif entity.offset < split_offset_utf16:
                # wholly left

                current_entities.append(entity)
            elif (
                entity.offset + entity.length
                > split_offset_utf16 + exclude
                > entity.offset
            ):
                # overlaps right boundary

                pending_entities.append(
                    _copy_tl(
                        entity,
                        offset=0,
                        length=entity.offset
                        + entity.length
                        - split_offset_utf16
                        - exclude,
                    )
                )
            elif entity.offset + entity.length > split_offset_utf16 + exclude:
                # wholly right

                pending_entities.append(
                    _copy_tl(
                        entity,
                        offset=entity.offset - split_offset_utf16 - exclude,
                    )
                )
        current_text = text[text_offset:split_index]
        yield parser.unparse(
            current_text,
            list(sorted(current_entities, key=lambda x: x.offset)),
        )

        text_offset = split_index + exclude
        bytes_offset += len(current_text.encode("utf-16le"))


def _copy_tl(o, **kwargs):
    d = o.to_dict()
    del d["_"]
    d.update(kwargs)
    return o.__class__(**d)


def check_url(url: str) -> bool:
    """Statically checks url for validity"""
    try:
        return bool(urlparse(url).netloc)
    except Exception:
        return False


def get_git_hash() -> typing.Union[str, bool]:
    """Get current Her git hash"""
    try:
        return git.Repo().head.commit.hexsha
    except Exception:
        return False


def is_serializable(x: typing.Any, /) -> bool:
    """Checks if object is JSON-serializable"""
    try:
        json.dumps(x)
        return True
    except Exception:
        return False


def get_entity_url(
    entity: typing.Union[User, Channel],
    openmessage: bool = False,
) -> str:
    """Get link to object, if available"""
    return (
        (
            f"tg://openmessage?id={entity.id}"
            if openmessage
            else f"tg://user?id={entity.id}"
        )
        if isinstance(entity, User)
        else (
            f"tg://resolve?domain={entity.username}"
            if getattr(entity, "username", None)
            else ""
        )
    )


async def get_message_link(
    message: Message,
    chat: typing.Optional[typing.Union[Chat, Channel]] = None,
) -> str:
    """Get link to message"""
    if message.is_private:
        return (
            f"tg://openmessage?user_id={get_chat_id(message)}&message_id={message.id}"
        )
    if not chat and not (chat := message.chat):
        chat = await message.get_chat()
    topic_affix = (
        f"?topic={message.reply_to.reply_to_msg_id}"
        if getattr(message.reply_to, "forum_topic", False)
        else ""
    )

    return (
        f"https://t.me/{chat.username}/{message.id}{topic_affix}"
        if getattr(chat, "username", False)
        else f"https://t.me/c/{chat.id}/{message.id}{topic_affix}"
    )


def remove_html(text: str, escape: bool = False, keep_emojis: bool = False) -> str:
    """Removes HTML tags from text"""
    return (escape_html if escape else str)(
        re.sub(
            (
                r"(<\/?a.*?>|<\/?b>|<\/?i>|<\/?u>|<\/?strong>|<\/?em>|<\/?code>|<\/?strike>|<\/?del>|<\/?pre.*?>)"
                if keep_emojis
                else r"(<\/?a.*?>|<\/?b>|<\/?i>|<\/?u>|<\/?strong>|<\/?em>|<\/?code>|<\/?strike>|<\/?del>|<\/?pre.*?>|<\/?emoji.*?>)"
            ),
            "",
            text,
        )
    )


def get_kwargs() -> typing.Dict[str, typing.Any]:
    """Get kwargs of function, in which is called"""
    keys, _, _, values = inspect.getargvalues(inspect.currentframe().f_back)
    return {key: values[key] for key in keys if key != "self"}


def mime_type(message: Message) -> str:
    """Get mime type of document in message"""
    return (
        ""
        if not isinstance(message, Message) or not getattr(message, "media", False)
        else getattr(getattr(message, "media", False), "mime_type", False) or ""
    )


def find_caller(
    stack: typing.Optional[typing.List[inspect.FrameInfo]] = None,
) -> typing.Any:
    """Attempts to find command in stack"""
    caller = next(
        (
            frame_info
            for frame_info in stack or inspect.stack()
            if hasattr(frame_info, "function")
            and any(
                inspect.isclass(cls_)
                and issubclass(cls_, Module)
                and cls_ is not Module
                for cls_ in frame_info.frame.f_globals.values()
            )
        ),
        None,
    )

    if not caller:
        return next(
            (
                frame_info.frame.f_locals["func"]
                for frame_info in stack or inspect.stack()
                if hasattr(frame_info, "function")
                and frame_info.function == "future_dispatcher"
                and (
                    "CommandDispatcher"
                    in getattr(getattr(frame_info, "frame", None), "f_globals", {})
                )
            ),
            None,
        )
    return next(
        (
            getattr(cls_, caller.function, None)
            for cls_ in caller.frame.f_globals.values()
            if inspect.isclass(cls_) and issubclass(cls_, Module)
        ),
        None,
    )


def validate_html(html: str) -> str:
    """Removes broken tags from html"""
    text, entities = hikkatl.extensions.html.parse(html)
    return hikkatl.extensions.html.unparse(escape_html(text), entities)


def iter_attrs(obj: typing.Any, /) -> typing.List[typing.Tuple[str, typing.Any]]:
    """Returns list of attributes of object"""
    return ((attr, getattr(obj, attr)) for attr in dir(obj))


def atexit(
    func: typing.Callable,
    use_signal: typing.Optional[int] = None,
    *args,
    **kwargs,
) -> None:
    """Calls function on exit"""
    if use_signal:
        signal.signal(use_signal, lambda *_: func(*args, **kwargs))
        return
    _atexit.register(functools.partial(func, *args, **kwargs))


def get_topic(message: Message) -> typing.Optional[int]:
    """Get topic id of message"""
    return (
        (message.reply_to.reply_to_top_id or message.reply_to.reply_to_msg_id)
        if (
            isinstance(message, Message)
            and message.reply_to
            and message.reply_to.forum_topic
        )
        else None
    )


def get_ram_usage() -> float:
    """Returns current process tree memory usage in MB"""
    try:
        import psutil

        current_process = psutil.Process(os.getpid())
        mem = current_process.memory_info()[0] / 2.0**20
        for child in current_process.children(recursive=True):
            mem += child.memory_info()[0] / 2.0**20
        return round(mem, 1)
    except Exception:
        return 0


init_ts = time.perf_counter()
