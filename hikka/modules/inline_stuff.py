# ©️ Dan Gazizullin, 2021-2023
# This file is a part of Her
# 🌐 https://github.com/hikariatama/Hikka
# You can redistribute it and/or modify it under the terms of the GNU AGPLv3
# 🔑 https://www.gnu.org/licenses/agpl-3.0.html

import logging
import re
import string

from hikkatl.errors.rpcerrorlist import YouBlockedUserError
from hikkatl.tl.functions.contacts import UnblockRequest
from hikkatl.tl.types import Message

from .. import loader, utils
from ..inline.types import BotInlineMessage

logger = logging.getLogger(__name__)

@loader.tds
class InlineStuff(loader.Module):
    """Provides support for inline stuff"""

    strings = {"name": "InlineStuff"}

    @loader.watcher(
        "out",
        "only_inline",
        contains="This message will be deleted automatically",
    )

    @loader.watcher("out", "only_inline", contains="Opening gallery...")
    async def gallery_watcher(self, message: Message):
        logger.info(f"Получено сообщение: {message.raw_text}")
        if message.via_bot_id != self.inline.bot_id:
            logger.info("Сообщение не от inline бота, пропуск.")
            return

        match = re.search(r"#id: ([a-zA-Z0-9]+)", message.raw_text)
        if not match:
            logger.error("Не удалось извлечь ID из сообщения.")
            return

        id_ = match[1]
        logger.info(f"Извлеченный ID: {id_}")
        logger.info(f"Ключи в self.inline._custom_map: {self.inline._custom_map.keys()}")

        if id_ not in self.inline._custom_map:
            logger.error(f"ID '{id_}' не найден в self.inline._custom_map")
            return

        m = await message.respond("🪐", reply_to=utils.get_topic(message))

        await self.inline.gallery(
            message=m,
            next_handler=self.inline._custom_map[id_]["handler"],
            caption=self.inline._custom_map[id_].get("caption", ""),
            force_me=self.inline._custom_map[id_].get("force_me", False),
            disable_security=self.inline._custom_map[id_].get(
                "disable_security", False
            ),
            silent=True,
        )

    async def _check_bot(self, username: str) -> bool:
        async with self._client.conversation("@BotFather", exclusive=False) as conv:
            try:
                m = await conv.send_message("/token")
            except YouBlockedUserError:
                await self._client(UnblockRequest(id="@BotFather"))
                m = await conv.send_message("/token")

            r = await conv.get_response()

            if not hasattr(r, "reply_markup") or not hasattr(r.reply_markup, "rows"):
                return False

            for row in r.reply_markup.rows:
                for button in row.buttons:
                    if username != button.text.strip("@"):
                        continue

                    m = await conv.send_message("/cancel")
                    r = await conv.get_response()

                    return True

    @loader.command()
    async def ch_her_bot(self, message: Message):
        args = utils.get_args_raw(message).strip("@")
        if (
            not args
            or not args.lower().endswith("bot")
            or len(args) <= 4
            or any(
                litera not in (string.ascii_letters + string.digits + "_")
                for litera in args
            )
        ):
            await utils.answer(message, self.strings("bot_username_invalid"))
            return

        try:
            await self._client.get_entity(f"@{args}")
        except ValueError:
            pass
        else:
            if not await self._check_bot(args):
                await utils.answer(message, self.strings("bot_username_occupied"))
                return

        self._db.set("hikka.inline", "custom_bot", args)
        self._db.set("hikka.inline", "bot_token", None)
        await utils.answer(message, self.strings("bot_updated"))

    async def aiogram_watcher(self, message: BotInlineMessage):
        if message.text != "/start":
            return

        await message.answer_photo(
            "https://imgur.com/a/0gmlFYI.png",
            caption=self.strings("this_is_hikka"),
        )
