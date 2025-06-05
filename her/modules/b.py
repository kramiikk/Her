import asyncio
import html
import logging
import random
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Set, Tuple, List, Optional

from hikkatl.errors import FloodWaitError

from .. import loader, utils

logger = logging.getLogger(__name__)


class RateLimiter:
    """Rate limiting implementation with token bucket algorithm"""

    def __init__(self, max_tokens: int = 5, refill_rate: float = 5 / 60):
        self.max_tokens = max_tokens
        self.tokens = max_tokens
        self.refill_rate = refill_rate  # tokens per second
        self.last_update = datetime.now()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Acquire a token for rate limiting"""
        async with self._lock:
            now = datetime.now()
            time_passed = (now - self.last_update).total_seconds()

            # Add tokens based on time passed

            self.tokens = min(
                self.max_tokens, self.tokens + int(time_passed * self.refill_rate)
            )

            wait_time = 0
            if self.tokens <= 0:
                # If no tokens available, wait longer

                wait_time = 16 + random.uniform(3, 7)
                logger.info(f"Rate limit exceeded, waiting {wait_time:.1f}s")
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
    announcement_text: str = "@buygrp"
    interval: Tuple[int, int] = (10, 15)  # minutes
    excluded_chats: Set[int] = field(default_factory=set)
    last_announcement: Dict[int, float] = field(default_factory=dict)
    recent_users: Dict[int, List[int]] = field(
        default_factory=lambda: defaultdict(list)
    )
    max_recent_users: int = 3
    recent_user_probability: float = 0.4  # Probability to skip recent users


class AnnouncementMod(loader.Module):
    """Module that automatically replies to new messages with an announcement at specified intervals."""

    strings = {
        "name": "Announcement",
        "config_saved": "✅ Configuration saved",
        "announcement_set": "📝 Announcement text updated",
        "interval_set": "⏱️ Interval set to {}-{} minutes",
        "activated": "✅ Configuration '{}' activated",
        "deactivated": "⏸️ Configuration '{}' deactivated",
        "status": "📊 Status: {}\n⏱️ Interval: {}-{} min\n🗣️ Active chats: {}\n📝 Text: {}",
        "code_created": "🆕 Configuration '{}' created",
        "code_deleted": "🗑️ Configuration '{}' deleted",
        "code_not_found": "❌ Configuration '{}' not found",
        "list_codes": "📋 Available configurations:\n{}",
        "chat_added": "✅ Chat added to configuration '{}'",
        "chat_removed": "❌ Chat removed from configuration '{}'",
        "help_text": """
📋 <b>Announcement Module Commands</b>

<b>Configuration Management:</b>
• <code>нa list</code> - Show all configurations
• <code>нa create [code]</code> - Create new configuration
• <code>нa delete [code]</code> - Delete configuration

<b>Configuration Settings:</b>
• <code>нa set [code] [text]</code> - Set announcement text
• <code>нa interval [code] [min] [max]</code> - Set interval in minutes
• <code>нa recent [code] [number]</code> - Set max recent users to track
• <code>нa on [code]</code> - Activate configuration
• <code>нa off [code]</code> - Deactivate configuration
• <code>нa status [code]</code> - Show configuration status

<b>Chat Management:</b>
• <code>нa sos [code]</code> - Add current chat to configuration
• <code>нa call [code]</code> - Remove current chat from configuration

<b>Example Usage:</b>
1. <code>нa create promo</code>
2. <code>нa set promo Join our group @example</code>
3. <code>нa interval promo 10 20</code>
4. <code>нa sos promo</code> (run in target chat)
5. <code>нa on promo</code>
        """.strip(),
    }

    def __init__(self):
        self.configs: Dict[str, AnnouncementConfig] = {}
        self.db = None
        self.client = None
        self.rate_limiter: Optional[RateLimiter] = None
        self._config_lock: Optional[asyncio.Lock] = None

    async def client_ready(self, client):
        """Called when the client is ready to start functioning"""
        self.client = client
        self.rate_limiter = RateLimiter()
        self._config_lock = asyncio.Lock()
        await self.load_config()
        logger.info("AnnouncementMod initialized successfully")

    async def load_config(self) -> None:
        """Load configuration from database"""
        try:
            data = self.db.get("AutoAnnouncement", "configs", {})

            for code, config_data in data.items():
                config = AnnouncementConfig()
                config.active = config_data.get("active", False)
                config.announcement_text = config_data.get(
                    "announcement_text", "@buygrp"
                )
                config.interval = tuple(config_data.get("interval", (10, 15)))
                config.excluded_chats = set(config_data.get("excluded_chats", []))
                config.max_recent_users = config_data.get("max_recent_users", 3)
                config.recent_user_probability = config_data.get(
                    "recent_user_probability", 0.4
                )

                # Load chats

                chats = defaultdict(set)
                for chat_id, topic_ids in config_data.get("chats", {}).items():
                    chats[int(chat_id)] = set(map(int, topic_ids))
                config.chats = chats

                # Load last announcement times

                config.last_announcement = {
                    int(chat_id): timestamp
                    for chat_id, timestamp in config_data.get(
                        "last_announcement", {}
                    ).items()
                }

                # Load recent users

                recent_users = defaultdict(list)
                for chat_id, users in config_data.get("recent_users", {}).items():
                    recent_users[int(chat_id)] = list(map(int, users))
                config.recent_users = recent_users

                self.configs[code] = config
            logger.info(f"Loaded {len(self.configs)} announcement configurations")
        except Exception as e:
            logger.error(f"Error loading config: {e}", exc_info=True)

    async def save_config(self) -> None:
        """Save configuration to database"""
        try:
            data = {}
            for code, config in self.configs.items():
                config_data = {
                    "active": config.active,
                    "announcement_text": config.announcement_text,
                    "interval": list(config.interval),
                    "excluded_chats": list(config.excluded_chats),
                    "max_recent_users": config.max_recent_users,
                    "recent_user_probability": config.recent_user_probability,
                    "chats": {
                        str(chat_id): list(topic_ids)
                        for chat_id, topic_ids in dict(config.chats).items()
                    },
                    "last_announcement": {
                        str(chat_id): timestamp
                        for chat_id, timestamp in config.last_announcement.items()
                    },
                    "recent_users": {
                        str(chat_id): list(map(int, users))
                        for chat_id, users in config.recent_users.items()
                    },
                }
                data[code] = config_data
            self.db.set("AutoAnnouncement", "configs", data)
            logger.debug("Configuration saved to database")
        except Exception as e:
            logger.error(f"Error saving config: {e}", exc_info=True)

    async def _handle_command(self, message) -> bool:
        """Handle command from message text"""
        text = message.text or message.raw_text
        if not text or not text.startswith("нa"):
            return False
        args = text[3:].strip().split()
        if not args:
            await utils.answer(message, self.strings["help_text"])
            return True
        command = args[0].lower()

        # Commands that don't require a code

        if command == "list":
            await self._handle_list(message)
            return True
        elif command in ["create", "new"] and len(args) > 1:
            return await self._handle_create(message, args[1].lower())
        elif command == "delete" and len(args) > 1:
            return await self._handle_delete(message, args[1].lower())
        elif command in ["help", "h"]:
            await utils.answer(message, self.strings["help_text"])
            return True
        # Commands that require a code

        if len(args) < 2:
            await utils.answer(message, "❌ Configuration code required")
            return True
        code = args[1].lower()
        config = self.configs.get(code)

        if not config:
            await utils.answer(message, self.strings["code_not_found"].format(code))
            return True
        # Handle specific commands

        if command == "set" and len(args) > 2:
            return await self._handle_set_text(message, config, code, args[2:])
        elif command == "interval" and len(args) > 3:
            return await self._handle_set_interval(
                message, config, code, args[2], args[3]
            )
        elif command == "recent" and len(args) > 2:
            return await self._handle_set_recent(message, config, code, args[2])
        elif command == "on":
            return await self._handle_toggle(message, config, code, True)
        elif command == "off":
            return await self._handle_toggle(message, config, code, False)
        elif command == "status":
            return await self._handle_status(message, config)
        elif command == "sos":
            return await self._handle_add_chat(message, config, code)
        elif command == "call":
            return await self._handle_remove_chat(message, config, code)
        return True

    async def _handle_create(self, message, code: str) -> bool:
        """Handle configuration creation"""
        async with self._config_lock:
            if code in self.configs:
                await utils.answer(message, f"❌ Configuration '{code}' already exists")
                return True
            self.configs[code] = AnnouncementConfig()
            await self.save_config()
        await utils.answer(message, self.strings["code_created"].format(code))
        return True

    async def _handle_delete(self, message, code: str) -> bool:
        """Handle configuration deletion"""
        async with self._config_lock:
            if code not in self.configs:
                await utils.answer(message, self.strings["code_not_found"].format(code))
                return True
            del self.configs[code]
            await self.save_config()
        await utils.answer(message, self.strings["code_deleted"].format(code))
        return True

    async def _handle_set_text(
        self, message, config: AnnouncementConfig, code: str, args: List[str]
    ) -> bool:
        """Handle setting announcement text"""
        announcement_text = " ".join(args)
        async with self._config_lock:
            config.announcement_text = announcement_text
            await self.save_config()
        await utils.answer(message, self.strings["announcement_set"])
        return True

    async def _handle_set_interval(
        self, message, config: AnnouncementConfig, code: str, min_str: str, max_str: str
    ) -> bool:
        """Handle setting interval"""
        try:
            min_interval = int(min_str)
            max_interval = int(max_str)

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
                message, self.strings["interval_set"].format(min_interval, max_interval)
            )
        except ValueError:
            await utils.answer(message, "⚠️ Invalid interval values. Use integers only.")
        return True

    async def _handle_set_recent(
        self, message, config: AnnouncementConfig, code: str, value: str
    ) -> bool:
        """Handle setting max recent users"""
        try:
            max_recent = int(value)
            if max_recent < 1:
                await utils.answer(message, "⚠️ Value must be at least 1")
                return True
            async with self._config_lock:
                config.max_recent_users = max_recent
                await self.save_config()
            await utils.answer(message, f"📊 Max recent users set to {max_recent}")
        except ValueError:
            await utils.answer(message, "⚠️ Invalid value. Use integer only.")
        return True

    async def _handle_toggle(
        self, message, config: AnnouncementConfig, code: str, active: bool
    ) -> bool:
        """Handle activating/deactivating configuration"""
        async with self._config_lock:
            config.active = active
            await self.save_config()
        status_msg = self.strings["activated" if active else "deactivated"].format(code)
        await utils.answer(message, status_msg)
        return True

    async def _handle_status(self, message, config: AnnouncementConfig) -> bool:
        """Handle status display"""
        status = "Active ✅" if config.active else "Inactive ⏸️"
        total_chats = sum(len(topics) for topics in config.chats.values())

        await utils.answer(
            message,
            self.strings["status"].format(
                status,
                config.interval[0],
                config.interval[1],
                total_chats,
                html.escape(config.announcement_text),
            ),
        )
        return True

    async def _handle_add_chat(
        self, message, config: AnnouncementConfig, code: str
    ) -> bool:
        """Handle adding current chat to configuration"""
        chat_id = message.chat_id
        topic_id = utils.get_topic(message) or 0

        async with self._config_lock:
            config.chats[chat_id].add(topic_id)
            await self.save_config()
        await utils.answer(message, self.strings["chat_added"].format(code))
        return True

    async def _handle_remove_chat(
        self, message, config: AnnouncementConfig, code: str
    ) -> bool:
        """Handle removing current chat from configuration"""
        chat_id = message.chat_id
        topic_id = utils.get_topic(message) or 0

        async with self._config_lock:
            if chat_id in config.chats and topic_id in config.chats[chat_id]:
                config.chats[chat_id].discard(topic_id)
                if not config.chats[chat_id]:
                    del config.chats[chat_id]
                await self.save_config()
        await utils.answer(message, self.strings["chat_removed"].format(code))
        return True

    async def _handle_list(self, message) -> None:
        """Handle the list command"""
        if not self.configs:
            await utils.answer(message, "📋 No announcement configurations found")
            return
        codes_info = []
        for code, config in self.configs.items():
            status = "✅" if config.active else "⏸️"
            total_chats = sum(len(topics) for topics in config.chats.values())
            codes_info.append(f"• <code>{code}</code> {status} - {total_chats} chats")
        await utils.answer(
            message, self.strings["list_codes"].format("\n".join(codes_info))
        )

    async def _handle_flood_wait(self, e: FloodWaitError) -> bool:
        """Handle FloodWait by waiting the required time"""
        wait_time = e.seconds + random.uniform(1, 5)
        logger.warning(f"FloodWait: waiting {wait_time:.1f}s")
        await asyncio.sleep(wait_time)
        return True

    async def _should_respond_to_user(
        self, config: AnnouncementConfig, chat_id: int, user_id: int
    ) -> bool:
        """Determine if we should respond to this user based on recent interaction history"""
        if chat_id not in config.recent_users or not config.recent_users[chat_id]:
            return True
        if user_id in config.recent_users[chat_id]:
            # Skip recent users with configured probability

            if random.random() < config.recent_user_probability:
                logger.debug(f"Skipping recent user {user_id} in chat {chat_id}")
                return False
        return True

    async def _update_recent_users(
        self, config: AnnouncementConfig, chat_id: int, user_id: int
    ) -> None:
        """Update the list of recent users we've responded to"""
        if chat_id not in config.recent_users:
            config.recent_users[chat_id] = []
        # Remove user if already in list

        if user_id in config.recent_users[chat_id]:
            config.recent_users[chat_id].remove(user_id)
        # Add user to the beginning of the list

        config.recent_users[chat_id].insert(0, user_id)

        # Keep only the configured number of recent users

        config.recent_users[chat_id] = config.recent_users[chat_id][
            : config.max_recent_users
        ]

    async def _send_announcement(
        self, chat_id: int, config: AnnouncementConfig, topic_id: int, msg_id: int
    ) -> bool:
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
            logger.debug(f"Sent announcement to chat {chat_id}")
            return True
        except FloodWaitError as e:
            await self._handle_flood_wait(e)
            return False
        except Exception as e:
            logger.error(f"Error sending announcement to {chat_id}: {e}")
            return False

    async def watcher(self, message):
        """Watch for incoming messages and reply with announcement if conditions are met"""
        try:
            if not message or not hasattr(message, "chat_id"):
                return
            if getattr(message, "out", False):
                # Handle our own commands

                if await self._handle_command(message):
                    return
                return
            chat_id = message.chat_id
            topic_id = utils.get_topic(message) or 0
            msg_id = message.id
            user_id = message.sender_id

            if not user_id:
                return
            for code, config in self.configs.items():
                try:
                    if not config.active:
                        continue
                    if chat_id in config.excluded_chats:
                        continue
                    if (
                        chat_id not in config.chats
                        or topic_id not in config.chats[chat_id]
                    ):
                        continue
                    current_time = time.time()
                    last_time = config.last_announcement.get(chat_id, 0)
                    time_elapsed = current_time - last_time

                    interval_seconds = random.uniform(
                        config.interval[0] * 60, config.interval[1] * 60
                    )

                    if time_elapsed >= interval_seconds:
                        should_respond = await self._should_respond_to_user(
                            config, chat_id, user_id
                        )

                        if should_respond:
                            # Add small random delay to make it more natural

                            await asyncio.sleep(random.uniform(1, 3))

                            success = await self._send_announcement(
                                chat_id, config, topic_id, msg_id
                            )

                            if success:
                                await self._update_recent_users(
                                    config, chat_id, user_id
                                )
                                await self.save_config()
                except Exception as e:
                    logger.error(
                        f"Error processing message for config {code}: {e}",
                        exc_info=True,
                    )
        except Exception as e:
            logger.error(f"Error in watcher: {e}", exc_info=True)
