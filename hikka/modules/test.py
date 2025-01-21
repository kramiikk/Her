# ©️ Dan Gazizullin, codrago 2021-2024
# This file is a part of Her
# 🌐 https://github.com/hikariatama/Hikka
# You can redistribute it and/or modify it under the terms of the GNU AGPLv3
# 🔑 https://www.gnu.org/licenses/agpl-3.0.html

import inspect
import asyncio
import io
import json
import logging
import random
import time
import typing
from io import BytesIO

from hikkatl.tl.types import Message
from hikkatl.tl import functions
from hikkatl.tl.tlobject import TLRequest
from hikkatl.utils import is_list_like

from .. import loader, main, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

GROUPS = [
    "auth",
    "account",
    "users",
    "contacts",
    "messages",
    "updates",
    "photos",
    "upload",
    "help",
    "channels",
    "bots",
    "payments",
    "stickers",
    "phone",
    "langpack",
    "folders",
    "stats",
]

CONSTRUCTORS = {
    (lambda x: x[0].lower() + x[1:])(
        method.__class__.__name__.rsplit("Request", 1)[0]
    ): method.CONSTRUCTOR_ID
    for method in utils.array_sum(
        [
            [
                method
                for method in dir(getattr(functions, group))
                if isinstance(method, TLRequest)
            ]
            for group in GROUPS
        ]
    )
}


@loader.tds
class TestMod(loader.Module):
    """Perform operations based on userbot self-testing"""

    strings = {
        "name": "Tester",
    }

    def __init__(self):
        self._memory = {}
        self._ratelimiter: typing.List[tuple] = []
        self._suspend_until = 0
        self._lock = False
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "time_sample",
                15,
                lambda: self.strings("_cfg_time_sample"),
                validator=loader.validators.Integer(minimum=1),
            ),
            loader.ConfigValue(
                "threshold",
                100,
                lambda: self.strings("_cfg_threshold"),
                validator=loader.validators.Integer(minimum=10),
            ),
            loader.ConfigValue(
                "local_floodwait",
                30,
                lambda: self.strings("_cfg_local_floodwait"),
                validator=loader.validators.Integer(minimum=10, maximum=3600),
            ),
            loader.ConfigValue(
                "forbidden_methods",
                ["joinChannel", "importChatInvite"],
                lambda: self.strings("_cfg_forbidden_methods"),
                validator=loader.validators.MultiChoice(
                    [
                        "sendReaction",
                        "joinChannel",
                        "importChatInvite",
                    ]
                ),
                on_change=lambda: self._client.forbid_constructors(
                    map(
                        lambda x: CONSTRUCTORS[x],
                        self.config["forbidden_constructors"],
                    )
                ),
            ),
        )

    async def client_ready(self):
        asyncio.ensure_future(self._install_protection())

    async def _install_protection(self):
        await asyncio.sleep(13)  # Restart lock
        if hasattr(self._client._call, "_old_call_rewritten"):
            raise loader.SelfUnload("Already installed")

        old_call = self._client._call

        async def new_call(
            sender: "MTProtoSender",  # type: ignore  # noqa: F821
            request: TLRequest,
            ordered: bool = False,
            flood_sleep_threshold: int = None,
        ):
            await asyncio.sleep(random.randint(1, 5) / 100)
            req = (request,) if not is_list_like(request) else request
            for r in req:
                if (
                    time.perf_counter() > self._suspend_until
                    and (
                        r.__module__.rsplit(".", maxsplit=1)[1]
                        in {"messages", "account", "channels"}
                    )
                ):
                    request_name = type(r).__name__
                    self._ratelimiter += [(request_name, time.perf_counter())]

                    self._ratelimiter = list(
                        filter(
                            lambda x: time.perf_counter() - x[1]
                            < int(self.config["time_sample"]),
                            self._ratelimiter,
                        )
                    )

                    if (
                        len(self._ratelimiter) > int(self.config["threshold"])
                        and not self._lock
                    ):
                        self._lock = True
                        report = io.BytesIO(
                            json.dumps(
                                self._ratelimiter,
                                indent=4,
                            ).encode()
                        )
                        report.name = "local_fw_report.json"

                        await self.inline.bot.send_document(
                            self.tg_id,
                            report,
                            caption=self.inline.sanitise_text(
                                self.strings("warning").format(
                                    self.config["local_floodwait"],
                                    prefix=utils.escape_html(self.get_prefix()),
                                )
                            )
                        )

                        # It is intented to use time.sleep instead of asyncio.sleep
                        time.sleep(int(self.config["local_floodwait"]))
                        self._lock = False

            return await old_call(sender, request, ordered, flood_sleep_threshold)

        self._client._call = new_call
        self._client._old_call_rewritten = old_call
        self._client._call._hikka_overwritten = True

    async def on_unload(self):
        if hasattr(self._client, "_old_call_rewritten"):
            self._client._call = self._client._old_call_rewritten
            delattr(self._client, "_old_call_rewritten")

    @loader.command()
    async def clearlogs(self, message: Message):
        for handler in logging.getLogger().handlers:
            handler.buffer = []
            handler.handledbuffer = []
            handler.tg_buff = ""

        await utils.answer(message, self.strings("logs_cleared"))

    @loader.command()
    async def logs(
        self,
        message: typing.Union[Message, InlineCall],
    ):
        """Displays all logs without confirmation"""
        logs = "\n\n".join(
            [
                "\n".join(
                    handler.dumps(0, client_id=self._client.tg_id)
                    if "client_id" in inspect.signature(handler.dumps).parameters
                    else handler.dumps(0)
                )
                for handler in logging.getLogger().handlers
            ]
        )

        logs = self.lookup("evaluator").censor(logs)
        logs = BytesIO(logs.encode("utf-16"))
        logs.name = "her-logs.txt"

        ghash = utils.get_git_hash()

        other = (
            *main.__version__,
            (
                " <a"
                f' href="https://github.com/kramiikk/Her/commit/{ghash}">@{ghash[:8]}</a>'
                if ghash
                else ""
            ),
        )

        await self._client.send_file(
            message.form["chat"],
            logs,
            caption=self.strings("logs_caption").format("ALL", *other),
            reply_to=message.form["top_msg_id"],
        )
