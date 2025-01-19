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
        
        # Enhanced escape function for MarkdownV2
        def escape_markdown(text):
            # Characters that need escaping in MarkdownV2
            escape_chars = '_*[]()~`>#+-=|{}.!'
            return ''.join(f'\\{c}' if c in escape_chars else c for c in str(text))
        
        try:
            # Create user info text with properly escaped characters
            user_name = escape_markdown(message.from_user.full_name)
            username = message.from_user.username
            username_text = escape_markdown(f"@{username}" if username else "No username")
            msg_text = escape_markdown(message.text or "")
            
            user_info = (
                f"👤 User: {user_name} \\({username_text}\\)\n"
                f"📱 User ID: `{message.from_user.id}`\n"
                f"💬 Message: {msg_text}\n"
            )
            
            # If this is a /start command, send the welcome message to user
            if message.text == "/start":
                # For the welcome message, use plain text without special formatting
                await bot.send_photo(
                    chat_id=message.from_user.id,
                    photo="https://i.imgur.com/iv1aMNA.jpeg",
                    caption=self.strings("this_is"),
                    parse_mode=None  # Use plain text to avoid parsing issues
                )
            
            await bot.send_message(
                chat_id=self.tg_id,
                text=user_info,
                parse_mode="MarkdownV2"
            )
            
            # Handle owner replies to forwarded messages
            if message.reply_to_message and message.from_user.id == self.tg_id:
                replied_text = message.reply_to_message.text
                user_id_match = re.search(r"User ID: `(\d+)`", replied_text)
                
                if user_id_match:
                    target_user_id = int(user_id_match.group(1))
                    await bot.send_message(
                        chat_id=target_user_id,
                        text=message.text,
                        parse_mode=None  # Use plain text for replies to avoid formatting issues
                    )
                    
        except Exception as e:
            logger.error(f"Failed to process message: {e}", exc_info=True)
