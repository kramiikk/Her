# ©️ Dan Gazizullin, codrago 2021-2024
# This file is a part of Her
# 🌐 https://github.com/hikariatama/Hikka
# You can redistribute it and/or modify it under the terms of the GNU AGPLv3
# 🔑 https://www.gnu.org/licenses/agpl-3.0.html

import asyncio
import contextlib
import datetime
import io
import json
import logging
import time

from hikkatl.tl.types import Message

from .. import loader, utils
from ..inline.types import BotInlineCall

logger = logging.getLogger(__name__)


@loader.tds
class HerBackupMod(loader.Module):
    """Handles database and modules backups"""

    strings = {"name": "HerBackup"}

    async def client_ready(self):
        self._backup_channel, _ = await utils.asset_channel(
            self._client,
            "her-backups",
            "📼 Your database backups will appear here",
            silent=True,
            archive=True,
            avatar="https://raw.githubusercontent.com/coddrago/Heroku/refs/heads/master/assets/heroku-backups.png",
            _folder="hikka",
            invite_bot=True,
        )

        self.set("period", 168 * 60 * 60)
        self.set("last_backup", round(time.time()))

    @loader.loop(interval=1, autostart=True)
    async def handler(self):
        try:
            if self.get("period") == "disabled":
                raise loader.StopLoop

            if not self.get("period"):
                await asyncio.sleep(3)
                return

            if not self.get("last_backup"):
                self.set("last_backup", round(time.time()))
                await asyncio.sleep(self.get("period"))
                return

            await asyncio.sleep(
                self.get("last_backup") + self.get("period") - time.time()
            )

            backup = io.BytesIO(json.dumps(self._db).encode())
            backup.name = (
                f"her-db-backup-{datetime.datetime.now():%d-%m-%Y-%H-%M}.json"
            )

            await self.inline.bot.send_document(
                int(f"-100{self._backup_channel.id}"),
                backup,
                reply_markup=self.inline.generate_markup(
                    [
                        [
                            {
                                "text": "↪️ Restore this",
                                "data": "hikka/backup/restore/confirm",
                            }
                        ]
                    ]
                )
            )

            self.set("last_backup", round(time.time()))
        except loader.StopLoop:
            raise
        except Exception:
            logger.exception("HerBackup failed")
            await asyncio.sleep(60)

    @loader.callback_handler()
    async def restore(self, call: BotInlineCall):
        if not call.data.startswith("hikka/backup/restore"):
            return

        if call.data == "hikka/backup/restore/confirm":
            await utils.answer(
                call,
                "❓ <b>Are you sure?</b>",
                reply_markup={
                    "text": "✅ Yes",
                    "data": "hikka/backup/restore",
                },
            )
            return

        file = await (
            await self._client.get_messages(
                self._backup_channel, call.message.message_id
            )
        )[0].download_media(bytes)

        decoded_text = json.loads(file.decode())

        with contextlib.suppress(KeyError):
            decoded_text["hikka.inline"].pop("bot_token")

        if not self._db.process_db_autofix(decoded_text):
            raise RuntimeError("Attempted to restore broken database")

        self._db.clear()
        self._db.update(**decoded_text)
        self._db.save()

        await call.answer(self.strings("db_restored"), show_alert=True)
        await self.invoke("restart", peer=call.message.peer_id)

    @loader.command()
    async def backupdb(self, message: Message):
        """| save backup of your bd"""
        txt = io.BytesIO(json.dumps(self._db).encode())
        txt.name = f"db-backup-{datetime.datetime.now():%d-%m-%Y-%H-%M}.json"
        await self._client.send_file(
            "me",
            txt,
            caption=self.strings("backup_caption").format(
                prefix=utils.escape_html(self.get_prefix())
            ),
        )
        await utils.answer(message, self.strings("backup_sent"))

    @loader.command()
    async def restoredb(self, message: Message):
        """[reply] | restore your bd"""
        if not (reply := await message.get_reply_message()) or not reply.media:
            await utils.answer(
                message,
                self.strings("reply_to_file"),
            )
            return

        file = await reply.download_media(bytes)
        decoded_text = json.loads(file.decode())

        with contextlib.suppress(KeyError):
            decoded_text["hikka.inline"].pop("bot_token")

        if not self._db.process_db_autofix(decoded_text):
            raise RuntimeError("Attempted to restore broken database")

        self._db.clear()
        self._db.update(**decoded_text)
        self._db.save()

        await utils.answer(message, self.strings("db_restored"))
        await self.invoke("restart", peer=message.peer_id)
