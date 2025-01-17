# ©️ Dan Gazizullin, 2021-2023
# This file is a part of Her
# 🌐 https://github.com/hikariatama/Hikka
# You can redistribute it and/or modify it under the terms of the GNU AGPLv3
# 🔑 https://www.gnu.org/licenses/agpl-3.0.html

import asyncio
import logging
import re
import random

from hikkatl.errors.rpcerrorlist import YouBlockedUserError
from hikkatl.tl.functions.contacts import UnblockRequest

from .. import utils
from .._internal import fw_protect
from .types import InlineUnit

logger = logging.getLogger(__name__)


class TokenObtainment(InlineUnit):
    async def _create_bot(self):
        logger.info("User doesn't have bot, attempting creating new one")
        async with self._client.conversation("@BotFather", exclusive=False) as conv:
            await asyncio.sleep(random.uniform(13, 30))
            await fw_protect()
            await conv.send_message("/newbot")
            r = await conv.get_response()

            if "20" in r.raw_text:
                return False

            await fw_protect()

            if self._db.get("hikka.inline", "custom_bot", False):
                username = self._db.get("hikka.inline", "custom_bot").strip("@")
                username = f"@{username}"
                try:
                    await self._client.get_entity(username)
                except ValueError:
                    pass
                else:
                    username = f"@h{utils.rand(2)}er{utils.rand(2)}bot"
            else:
                username = f"@h{utils.rand(2)}er{utils.rand(2)}bot"

            for msg in [
                f"🪐 Her userbot"[:64],
                username,
            ]:
                await asyncio.sleep(random.uniform(13, 33))
                await fw_protect()
                await conv.send_message(msg)
                r = await conv.get_response()

        return await self._assert_token(False)

    async def _assert_token(
        self,
        create_new_if_needed: bool = True,
        revoke_token: bool = False,
    ) -> bool:
        if self._token:
            return True

        logger.info("Bot token not found in db, attempting search in BotFather")

        if not self._db.get(__name__, "no_mute", False):
            await utils.dnd(
                self._client,
                await self._client.get_entity("@BotFather"),
                True,
            )
            self._db.set(__name__, "no_mute", True)

        async with self._client.conversation("@BotFather", exclusive=False) as conv:
            try:
                await asyncio.sleep(random.uniform(13, 33))
                await fw_protect()
                m = await conv.send_message("/token")
            except YouBlockedUserError:
                await self._client(UnblockRequest(id="@BotFather"))
                await asyncio.sleep(random.uniform(13, 33))
                await fw_protect()
                m = await conv.send_message("/token")

            r = await conv.get_response()

            if not hasattr(r, "reply_markup") or not hasattr(r.reply_markup, "rows"):
                await conv.cancel_all()

                return await self._create_bot() if create_new_if_needed else False

            for row in r.reply_markup.rows:
                for button in row.buttons:
                    if self._db.get(
                        "hikka.inline", "custom_bot", False
                    ) and self._db.get(
                        "hikka.inline", "custom_bot", False
                    ) != button.text.strip("@"):
                        continue

                    if not self._db.get(
                        "hikka.inline",
                        "custom_bot",
                        False,
                    ) and not re.search(r"@h[0-9a-zA-Z]{2}er[0-9a-zA-Z]{2}bot", button.text):
                        continue

                    await asyncio.sleep(random.uniform(13, 33))
                    await fw_protect()

                    await conv.send_message(button.text)
                    r = await conv.get_response()

                    if revoke_token:
                        await asyncio.sleep(random.uniform(13, 33))
                        await fw_protect()

                        await conv.send_message("/revoke")
                        await conv.get_response()


                        await asyncio.sleep(random.uniform(13, 33))
                        await fw_protect()

                        await conv.send_message(button.text)
                        r = await conv.get_response()

                    token = r.raw_text.splitlines()[1]

                    self._db.set("hikka.inline", "bot_token", token)
                    self._token = token

                    for msg in [
                        "/setinline",
                        button.text,
                        "user@her:~$",
                        "/setinlinefeedback",
                        button.text,
                        "Enabled",
                    ]:
                        await asyncio.sleep(random.uniform(13, 33))
                        await fw_protect()
                        await conv.send_message(msg)
                        await conv.get_response()

                    return True

        return await self._create_bot() if create_new_if_needed else False

    async def _reassert_token(self):
        is_token_asserted = await self._assert_token(revoke_token=True)
        if not is_token_asserted:
            self.init_complete = False
        else:
            await self.register_manager(ignore_token_checks=True)

    async def _dp_revoke_token(self, already_initialised: bool = True):
        if already_initialised:
            await self._stop()
            logger.error("Got polling conflict. Attempting token revocation...")

        self._db.set("hikka.inline", "bot_token", None)
        self._token = None
        if already_initialised:
            asyncio.ensure_future(self._reassert_token())
        else:
            return await self._reassert_token()
