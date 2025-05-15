import asyncio
import html
import logging
import random
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Set, Tuple

from hikkatl.errors import (
    FloodWaitError,
    SlowModeWaitError,
)

from .. import loader, utils

logger = logging.getLogger(__name__)


class RateLimiter:
    """Rate limiting implementation"""

    def __init__(self):
        self.tokens = 5
        self.last_update = datetime.now()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = datetime.now()

            time_passed = (now - self.last_update).total_seconds()
            self.tokens = min(5, self.tokens + int(time_passed * 5 / 60))

            wait_time = 0
            if self.tokens <= 0:
                wait_time = 16 + random.uniform(3, 7)
            else:
                self.tokens -= 1
            self.last_update = now
        if wait_time > 0:
            await asyncio.sleep(wait_time)


@dataclass
class AnnouncementConfig:
    """Configuration for announcement replies"""

    active: bool = False
    chats: Dict[int, Set[int]] = field(default_factory=lambda: defaultdict(set))
    announcement_text: str = "📢 Default announcement message"
    interval: Tuple[int, int] = (10, 15)
    excluded_chats: Set[int] = field(default_factory=set)
    last_announcement: Dict[int, float] = field(default_factory=dict)


class AnnouncementMod(loader.Module):
    """Module that automatically replies to new messages with an announcement at specified intervals."""

    strings = {
        "name": "AutoAnnouncement",
        "config_saved": "✅ Configuration saved",
        "announcement_set": "📝 Announcement text set",
        "interval_set": "⏱️ Interval set to {}-{} minutes",
        "activated": "✅ Auto-announcement activated for code '{}'",
        "deactivated": "⏸️ Auto-announcement deactivated for code '{}'",
        "status": "📊 Status: {}\n⏱️ Interval: {}-{} minutes\n🗣️ Active in {} chats\n📝 Message: {}",
        "chat_added": "➕ Chat added to announcement list",
        "chat_removed": "➖ Chat removed from announcement list",
        "code_created": "🆕 Created new announcement code '{}'",
        "code_deleted": "🗑️ Deleted announcement code '{}'",
        "code_not_found": "❌ Announcement code '{}' not found",
        "list_codes": "📋 Announcement codes:\n{}",
    }

    def __init__(self):
        self.configs = {}
        self.db = None
        self.client = None
        self.rate_limiter = None
        self._config_lock = None

    async def client_ready(self, client):
        """Called when the client is ready to start functioning"""
        self.client = client
        self.rate_limiter = RateLimiter()
        self._config_lock = asyncio.Lock()
        await self.load_config()

    async def load_config(self):
        """Load configuration from database"""
        data = self.db.get("AutoAnnouncement", "configs", {})

        for code, config_data in data.items():
            config = AnnouncementConfig()
            config.active = config_data.get("active", False)
            config.announcement_text = config_data.get(
                "announcement_text", "📢 Default announcement message"
            )
            config.interval = tuple(config_data.get("interval", (10, 15)))
            config.excluded_chats = set(config_data.get("excluded_chats", []))

            chats = defaultdict(set)
            for chat_id, topic_ids in config_data.get("chats", {}).items():
                chats[int(chat_id)] = set(map(int, topic_ids))
            config.chats = chats

            config.last_announcement = {
                int(chat_id): timestamp
                for chat_id, timestamp in config_data.get(
                    "last_announcement", {}
                ).items()
            }

            self.configs[code] = config

    async def save_config(self):
        """Save configuration to database"""
        data = {}

        for code, config in self.configs.items():
            config_data = {
                "active": config.active,
                "announcement_text": config.announcement_text,
                "interval": list(config.interval),
                "excluded_chats": list(config.excluded_chats),
                "chats": {
                    str(chat_id): list(topic_ids)
                    for chat_id, topic_ids in dict(config.chats).items()
                },
                "last_announcement": {
                    str(chat_id): timestamp
                    for chat_id, timestamp in config.last_announcement.items()
                },
            }
            data[code] = config_data
        self.db.set("AutoAnnouncement", "configs", data)

    async def _handle_command(self, message):
        """Handle command from message text"""
        text = message.text or message.raw_text
        if not text or not text.startswith("нa"):
            return False
        args = text[3:].strip().split()
        if not args:
            return False
        command = args[0].lower()

        if command == "list":
            await self._handle_list(message)
            return True
        if command in ["create", "new"] and len(args) > 1:
            code = args[1].lower()
            async with self._config_lock:
                if code in self.configs:
                    await utils.answer(message, f"❌ Code '{code}' already exists")
                    return True
                self.configs[code] = AnnouncementConfig()
                await self.save_config()
            await utils.answer(message, self.strings["code_created"].format(code))
            return True
        if len(args) < 2:
            return False
        if command == "delete" and len(args) > 1:
            code = args[1].lower()
            async with self._config_lock:
                if code not in self.configs:
                    await utils.answer(
                        message, self.strings["code_not_found"].format(code)
                    )
                    return True
                del self.configs[code]
                await self.save_config()
            await utils.answer(message, self.strings["code_deleted"].format(code))
            return True
        code = args[1].lower()
        config = self.configs.get(code)
        if not config:
            await utils.answer(message, self.strings["code_not_found"].format(code))
            return True
        if command == "set" and len(args) > 2:
            announcement_text = " ".join(args[2:])
            async with self._config_lock:
                config.announcement_text = announcement_text
                await self.save_config()
            await utils.answer(message, self.strings["announcement_set"])
        elif command == "interval" and len(args) > 3:
            try:
                min_interval = int(args[2])
                max_interval = int(args[3])

                if min_interval < 1 or max_interval < min_interval:
                    await utils.answer(
                        message,
                        "⚠️ Invalid interval values. Min must be at least 1 and max must be >= min.",
                    )
                    return True
                async with self._config_lock:
                    config.interval = (min_interval, max_interval)
                    await self.save_config()
                await utils.answer(
                    message,
                    self.strings["interval_set"].format(min_interval, max_interval),
                )
            except ValueError:
                await utils.answer(message, "⚠️ Invalid interval values")
        elif command == "on":
            async with self._config_lock:
                config.active = True
                await self.save_config()
            await utils.answer(message, self.strings["activated"].format(code))
        elif command == "off":
            async with self._config_lock:
                config.active = False
                await self.save_config()
            await utils.answer(message, self.strings["deactivated"].format(code))
        elif command == "status":
            status = "Active ✅" if config.active else "Inactive ⏸️"
            total_chats = sum(len(topics) for topics in config.chats.values())

            await utils.answer(
                message,
                self.strings["status"].format(
                    status,
                    config.interval[0],
                    config.interval[1],
                    total_chats,
                    config.announcement_text,
                ),
            )
        elif command == "sos":
            chat_id = message.chat_id
            topic_id = utils.get_topic(message) or 0

            async with self._config_lock:
                config.chats[chat_id].add(topic_id)
                await self.save_config()
            await utils.answer(message, self.strings["chat_added"])
        elif command == "call":
            chat_id = message.chat_id
            topic_id = utils.get_topic(message) or 0

            async with self._config_lock:
                if chat_id in config.chats and topic_id in config.chats[chat_id]:
                    config.chats[chat_id].discard(topic_id)

                    if not config.chats[chat_id]:
                        del config.chats[chat_id]
                    await self.save_config()
        return True

    async def _handle_list(self, message):
        """Handle the list command"""
        if not self.configs:
            await utils.answer(message, "📋 No announcement codes configured")
            return
        codes_info = []
        for code, config in self.configs.items():
            status = "✅" if config.active else "⏸️"
            total_chats = sum(len(topics) for topics in config.chats.values())
            codes_info.append(f"• <code>{code}</code> {status} - {total_chats} chats")
        await utils.answer(
            message, self.strings["list_codes"].format("\n".join(codes_info))
        )

    async def _handle_flood_wait(self, e: FloodWaitError):
        """Handle FloodWait by waiting the required time"""
        logger.warning(f"FloodWait: {e.seconds}s")
        await asyncio.sleep(e.seconds + 181)
        return True

    async def _send_announcement(
        self, chat_id: int, config: AnnouncementConfig, msg_id: int
    ):
        """Send an announcement to a chat as a reply to specific message"""
        try:
            await self.rate_limiter.acquire()

            await self.client.send_message(
                entity=chat_id,
                message=html.unescape(config.announcement_text),
                parse_mode="html",
                reply_to=msg_id,
            )

            async with self._config_lock:
                config.last_announcement[chat_id] = time.time()
                await self.save_config()
            return True
        except FloodWaitError as e:
            await self._handle_flood_wait(e)
            return False
        except Exception as e:
            logger.error(f"Error in _send_announcement to {chat_id}: {e}")
            return False

    async def watcher(self, message):
        """Watch for incoming messages and reply with announcement if conditions are met"""
        if not message or not hasattr(message, "chat_id"):
            return
        if getattr(message, "out", False):
            if await self._handle_command(message):
                return
            return
        chat_id = message.chat_id
        topic_id = utils.get_topic(message) or 0
        msg_id = message.id

        for code, config in self.configs.items():
            try:
                if not config.active:
                    continue
                if chat_id in config.excluded_chats:
                    continue
                if chat_id not in config.chats or topic_id not in config.chats[chat_id]:
                    continue
                current_time = time.time()
                last_time = config.last_announcement.get(chat_id, 0)
                time_elapsed = current_time - last_time

                interval_seconds = random.uniform(
                    config.interval[0] * 60, config.interval[1] * 60
                )

                if time_elapsed >= interval_seconds:
                    await asyncio.sleep(random.uniform(1, 3))
                    await self._send_announcement(chat_id, config, topic_id, msg_id)
            except Exception as e:
                logger.error(f"Error in watcher for code {code}: {e}", exc_info=True)
