# ©️ Dan Gazizullin, codrago 2021-2024
# This file is a part of Her
# 🌐 https://github.com/hikariatama/Hikka
# You can redistribute it and/or modify it under the terms of the GNU AGPLv3
# 🔑 https://www.gnu.org/licenses/agpl-3.0.html

import re
import string
import logging

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
        if hasattr(message, 'via_bot_id') and message.via_bot_id == self.inline.bot_id:
            match = re.search(r"#id: ([a-zA-Z0-9]+)", message.raw_text)
            if not match:
                return

            id_ = match[1]

            if id_ not in self.inline._custom_map:
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

        self._db.set("hikka.inline", "custom_bot", args)
        self._db.set("hikka.inline", "bot_token", None)
        await utils.answer(message, self.strings("bot_updated"))

    async def aiogram_watcher(self, message: BotInlineMessage):
        """
        Watches for bot interactions and forwards messages to owner
        """
        # Get bot instance
        bot = self.inline.bot
        
        # Escape special characters for MarkdownV2
        def escape_markdown(text):
            escape_chars = r'_*[]()~`>#+-=|{}.!'
            return ''.join(f'\\{c}' if c in escape_chars else c for c in str(text))
        
        # Create user info text with escaped characters
        user_name = escape_markdown(message.from_user.full_name)
        username = escape_markdown(f"@{message.from_user.username}" if message.from_user.username else "No username")
        msg_text = escape_markdown(message.text)
        
        user_info = (
            f"👤 User: {user_name} \\({username}\\)\n"
            f"📱 User ID: `{message.from_user.id}`\n"
            f"💬 Message: {msg_text}\n"
        )
        
        # If this is a /start command, send the welcome message to user
        if message.text == "/start":
            await bot.send_photo(
                chat_id=message.from_user.id,
                photo="https://i.imgur.com/iv1aMNA.jpeg",
                caption=self.strings("this_is")
            )
        
        # Forward info to owner
        try:
            await bot.send_message(
                chat_id=self.tg_id,
                text=user_info,
                parse_mode="MarkdownV2"
            )
        except Exception as e:
            logger.error(f"Failed to forward message to owner: {e}", exc_info=True)
        
        # Handle owner replies to forwarded messages
        if message.reply_to_message and message.from_user.id == self.tg_id:
            try:
                # Extract original user's ID from the replied message
                replied_text = message.reply_to_message.text
                user_id_match = re.search(r"User ID: `(\d+)`", replied_text)
                
                if user_id_match:
                    target_user_id = int(user_id_match.group(1))
                    # Forward owner's reply to the user using bot
                    await bot.send_message(
                        chat_id=target_user_id,
                        text=message.text
                    )
            except Exception as e:
                logger.error(f"Failed to send reply to user: {e}", exc_info=True)
