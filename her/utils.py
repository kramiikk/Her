import io
import json
import os
import re
import typing

import hikkatl
from hikkatl.tl.custom.message import Message

from hikkatl.tl.types import MessageMediaWebPage


def get_args_raw(message: typing.Union[Message, str]) -> str:
    """Get the parameters to the command as a raw string (not split)"""
    if not (message := getattr(message, "message", message)):
        return False
    return args[1] if len(args := message.split(maxsplit=1)) > 1 else ""


def get_chat_id(message: Message) -> int:
    """Get the chat ID, but without -100 if its a channel"""
    return hikkatl.utils.resolve_id(
        getattr(message, "chat_id", None)
        or getattr(getattr(message, "chat", None), "id", None)
    )[0]


def escape_html(text: str, /) -> str:  # sourcery skip
    """Pass all untrusted/potentially corrupt input here"""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def get_base_dir() -> str:
    """Get directory of current file"""
    return os.path.dirname(os.path.abspath(__file__))


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


def is_serializable(x: typing.Any, /) -> bool:
    """Checks if object is JSON-serializable"""
    try:
        json.dumps(x)
        return True
    except Exception:
        return False


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


def iter_attrs(obj: typing.Any, /) -> typing.List[typing.Tuple[str, typing.Any]]:
    """Returns list of attributes of object"""
    return ((attr, getattr(obj, attr)) for attr in dir(obj))


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
