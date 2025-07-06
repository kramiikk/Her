import asyncio
import html
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Dict, Set, Tuple, List, Optional, Any

from hikkatl.errors import FloodWaitError

from .. import loader, utils

logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, max_tokens: int = 2, refill_rate: float = 1 / 120):
        self.max_tokens = max_tokens
        self.tokens = max_tokens
        self.refill_rate = refill_rate
        self.last_update = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.time()
            time_passed = now - self.last_update

            self.tokens = min(
                self.max_tokens, self.tokens + (time_passed * self.refill_rate)
            )
            self.last_update = now

            if self.tokens < 1:
                wait_time = 120 + random.uniform(30, 60)
                logger.info(f"Rate limit exceeded, waiting {wait_time:.1f}s")
                await asyncio.sleep(wait_time)

                now = time.time()
                time_passed = now - self.last_update
                self.tokens = min(
                    self.max_tokens, self.tokens + (time_passed * self.refill_rate)
                )
                self.last_update = now
            self.tokens -= 1
            await asyncio.sleep(random.uniform(8, 20))


@dataclass
class AnnouncementConfig:
    active: bool = False
    chats: Dict[int, Set[int]] = field(default_factory=dict)
    announcement_text: str = "@ABACEKC"
    interval: Tuple[int, int] = (45, 90)
    excluded_chats: Set[int] = field(default_factory=set)
    last_announcement: Dict[int, float] = field(default_factory=dict)
    recent_users: Dict[int, List[Tuple[int, float]]] = field(default_factory=dict)
    max_recent_users: int = 8
    recent_user_probability: float = 0.75
    _locks: Dict[int, asyncio.Lock] = field(default_factory=dict)
    _max_locks: int = 100

    def get_lock(self, chat_id: int) -> asyncio.Lock:
        if chat_id not in self._locks:
            if len(self._locks) >= self._max_locks:
                oldest_keys = sorted(self._locks.keys())[:10]
                for key in oldest_keys:
                    del self._locks[key]
            self._locks[chat_id] = asyncio.Lock()
        return self._locks[chat_id]

    def cleanup_old_users(self, chat_id: int, max_age: float = 3600):
        if chat_id not in self.recent_users:
            return
        try:
            current_time = time.time()
            self.recent_users[chat_id] = [
                (user_id, timestamp)
                for user_id, timestamp in self.recent_users[chat_id]
                if current_time - timestamp < max_age
            ]
        except Exception as e:
            logger.error(f"Error cleaning up users for chat {chat_id}: {e}")
            self.recent_users[chat_id] = []

    def cleanup_old_announcements(self, max_age: float = 86400):
        try:
            current_time = time.time()
            to_remove = [
                chat_id
                for chat_id, timestamp in self.last_announcement.items()
                if current_time - timestamp > max_age
            ]
            for chat_id in to_remove:
                del self.last_announcement[chat_id]
        except Exception as e:
            logger.error(f"Error cleaning up announcements: {e}")

    def get_chats_set(self, chat_id: int) -> Set[int]:
        """Безопасный доступ к множеству чатов"""
        if chat_id not in self.chats:
            self.chats[chat_id] = set()
        return self.chats[chat_id]

    def get_recent_users_list(self, chat_id: int) -> List[Tuple[int, float]]:
        """Безопасный доступ к списку пользователей"""
        if chat_id not in self.recent_users:
            self.recent_users[chat_id] = []
        return self.recent_users[chat_id]


class AnnouncementMod(loader.Module):
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
    }

    def __init__(self):
        self.configs: Dict[str, AnnouncementConfig] = {}
        self.db = None
        self.client = None
        self.rate_limiter: Optional[RateLimiter] = None
        self._config_lock: Optional[asyncio.Lock] = None
        self._cleanup_task: Optional[asyncio.Task] = None

    async def client_ready(self, client, db):
        try:
            self.client = client
            self.db = db
            self.rate_limiter = RateLimiter()
            self._config_lock = asyncio.Lock()

            await self.load_config()

            try:
                self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
            except Exception as e:
                logger.error(f"Error creating cleanup task: {e}")
            logger.info("AnnouncementMod initialized successfully")
        except Exception as e:
            logger.error(f"Error in client_ready: {e}", exc_info=True)

    async def on_unload(self):
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await asyncio.wait_for(self._cleanup_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            except Exception as e:
                logger.error(f"Error during cleanup task shutdown: {e}")

    async def _periodic_cleanup(self):
        while True:
            try:
                await asyncio.sleep(1800)

                if self._config_lock is None:
                    continue
                async with self._config_lock:
                    for config in self.configs.values():
                        try:
                            config.cleanup_old_announcements()
                            chat_ids = list(config.recent_users.keys())
                            for chat_id in chat_ids:
                                config.cleanup_old_users(chat_id)
                        except Exception as e:
                            logger.error(f"Error in config cleanup: {e}")
                    try:
                        await self.save_config()
                    except Exception as e:
                        logger.error(f"Error saving config during cleanup: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}", exc_info=True)

    def _validate_config_data(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """Валидация данных конфигурации"""
        validated = {}

        validated["active"] = bool(config_data.get("active", False))
        validated["announcement_text"] = str(
            config_data.get("announcement_text", "@buygrp")
        )

        interval = config_data.get("interval", (45, 90))
        if isinstance(interval, (list, tuple)) and len(interval) == 2:
            try:
                min_int, max_int = int(interval[0]), int(interval[1])
                if min_int >= 30 and max_int >= min_int:
                    validated["interval"] = (min_int, max_int)
                else:
                    validated["interval"] = (45, 90)
            except (ValueError, TypeError):
                validated["interval"] = (45, 90)
        else:
            validated["interval"] = (45, 90)
        validated["excluded_chats"] = list(config_data.get("excluded_chats", []))
        validated["max_recent_users"] = max(
            1, min(20, int(config_data.get("max_recent_users", 8)))
        )
        validated["recent_user_probability"] = max(
            0.0, min(1.0, float(config_data.get("recent_user_probability", 0.75)))
        )

        return validated

    async def load_config(self) -> None:
        try:
            data = self.db.get("AutoAnnouncement", "configs", {})
            if not isinstance(data, dict):
                data = {}
            for code, config_data in data.items():
                if not isinstance(config_data, dict):
                    continue
                try:
                    validated_data = self._validate_config_data(config_data)

                    config = AnnouncementConfig()
                    config.active = validated_data["active"]
                    config.announcement_text = validated_data["announcement_text"]
                    config.interval = validated_data["interval"]
                    config.excluded_chats = set(validated_data["excluded_chats"])
                    config.max_recent_users = validated_data["max_recent_users"]
                    config.recent_user_probability = validated_data[
                        "recent_user_probability"
                    ]

                    chats = {}
                    for chat_id, topic_ids in config_data.get("chats", {}).items():
                        try:
                            chat_id_int = int(chat_id)
                            if isinstance(topic_ids, (list, set)):
                                chats[chat_id_int] = set(
                                    int(tid) for tid in topic_ids if str(tid).isdigit()
                                )
                        except (ValueError, TypeError):
                            continue
                    config.chats = chats

                    last_announcement = {}
                    for chat_id, timestamp in config_data.get(
                        "last_announcement", {}
                    ).items():
                        try:
                            last_announcement[int(chat_id)] = float(timestamp)
                        except (ValueError, TypeError):
                            continue
                    config.last_announcement = last_announcement

                    recent_users = {}
                    for chat_id, users_data in config_data.get(
                        "recent_users", {}
                    ).items():
                        try:
                            chat_id_int = int(chat_id)
                            if isinstance(users_data, list) and users_data:
                                recent_users[chat_id_int] = [
                                    (int(uid), float(ts))
                                    for uid, ts in users_data
                                    if str(uid).isdigit()
                                    and isinstance(ts, (int, float))
                                ]
                        except (ValueError, TypeError):
                            continue
                    config.recent_users = recent_users

                    self.configs[code] = config
                except Exception as e:
                    logger.error(f"Error loading config '{code}': {e}")
                    continue
            logger.info(f"Loaded {len(self.configs)} announcement configurations")
        except Exception as e:
            logger.error(f"Error loading config: {e}", exc_info=True)

    async def save_config(self) -> None:
        try:
            data = {}
            for code, config in self.configs.items():
                try:
                    config_data = {
                        "active": config.active,
                        "announcement_text": config.announcement_text,
                        "interval": list(config.interval),
                        "excluded_chats": list(config.excluded_chats),
                        "max_recent_users": config.max_recent_users,
                        "recent_user_probability": config.recent_user_probability,
                        "chats": {
                            str(chat_id): list(topic_ids)
                            for chat_id, topic_ids in config.chats.items()
                        },
                        "last_announcement": {
                            str(chat_id): timestamp
                            for chat_id, timestamp in config.last_announcement.items()
                        },
                        "recent_users": {
                            str(chat_id): list(users_data)
                            for chat_id, users_data in config.recent_users.items()
                        },
                    }
                    data[code] = config_data
                except Exception as e:
                    logger.error(f"Error preparing config '{code}' for save: {e}")
            self.db.set("AutoAnnouncement", "configs", data)
        except Exception as e:
            logger.error(f"Error saving config: {e}", exc_info=True)

    def _safe_get_message_attributes(
        self, message
    ) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
        """Безопасно извлекает атрибуты сообщения"""
        try:
            chat_id = getattr(message, "chat_id", None)
            topic_id = utils.get_topic(message) if hasattr(utils, "get_topic") else 0
            msg_id = getattr(message, "id", None)
            user_id = getattr(message, "sender_id", None)
            return chat_id, topic_id or 0, msg_id, user_id
        except Exception as e:
            logger.error(f"Error getting message attributes: {e}")
            return None, None, None, None

    async def _handle_command(self, message) -> bool:
        try:
            text = getattr(message, "text", None) or getattr(message, "raw_text", None)
            if not text or not text.startswith("нa"):
                return False
            args = text[3:].strip().split()
            if not args:
                return True
            command = args[0].lower()

            if command == "list":
                await self._handle_list(message)
                return True
            elif command in ["create", "new"] and len(args) > 1:
                return await self._handle_create(message, args[1].lower())
            elif command == "delete" and len(args) > 1:
                return await self._handle_delete(message, args[1].lower())
            if len(args) < 2:
                await utils.answer(message, "❌ Configuration code required")
                return True
            code = args[1].lower()
            config = self.configs.get(code)

            if not config:
                await utils.answer(message, self.strings["code_not_found"].format(code))
                return True
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
            return True
        except Exception as e:
            logger.error(f"Error handling command: {e}", exc_info=True)
            return True

    async def _handle_create(self, message, code: str) -> bool:
        try:
            if not self._config_lock:
                return True
            async with self._config_lock:
                if code in self.configs:
                    await utils.answer(
                        message, f"❌ Configuration '{code}' already exists"
                    )
                    return True
                config = AnnouncementConfig()
                self.configs[code] = config
                await self.save_config()
            await utils.answer(message, self.strings["code_created"].format(code))
        except Exception as e:
            logger.error(f"Error creating config: {e}")
            await utils.answer(message, "❌ Error creating configuration")
        return True

    async def _handle_delete(self, message, code: str) -> bool:
        try:
            if not self._config_lock:
                return True
            async with self._config_lock:
                if code not in self.configs:
                    await utils.answer(
                        message, self.strings["code_not_found"].format(code)
                    )
                    return True
                del self.configs[code]
                await self.save_config()
            await utils.answer(message, self.strings["code_deleted"].format(code))
        except Exception as e:
            logger.error(f"Error deleting config: {e}")
            await utils.answer(message, "❌ Error deleting configuration")
        return True

    async def _handle_set_text(
        self, message, config: AnnouncementConfig, code: str, args: List[str]
    ) -> bool:
        try:
            announcement_text = " ".join(args)
            if not self._config_lock:
                return True
            async with self._config_lock:
                config.announcement_text = announcement_text
                await self.save_config()
            await utils.answer(message, self.strings["announcement_set"])
        except Exception as e:
            logger.error(f"Error setting text: {e}")
            await utils.answer(message, "❌ Error setting announcement text")
        return True

    async def _handle_set_interval(
        self, message, config: AnnouncementConfig, code: str, min_str: str, max_str: str
    ) -> bool:
        try:
            min_interval = int(min_str)
            max_interval = int(max_str)

            if min_interval < 30 or max_interval < min_interval:
                await utils.answer(
                    message,
                    "⚠️ Invalid interval values. Min must be at least 30 minutes and max must be >= min.",
                )
                return True
            if not self._config_lock:
                return True
            async with self._config_lock:
                config.interval = (min_interval, max_interval)
                await self.save_config()
            await utils.answer(
                message, self.strings["interval_set"].format(min_interval, max_interval)
            )
        except ValueError:
            await utils.answer(message, "⚠️ Invalid interval values. Use integers only.")
        except Exception as e:
            logger.error(f"Error setting interval: {e}")
            await utils.answer(message, "❌ Error setting interval")
        return True

    async def _handle_set_recent(
        self, message, config: AnnouncementConfig, code: str, value: str
    ) -> bool:
        try:
            max_recent = int(value)
            if max_recent < 1 or max_recent > 20:
                await utils.answer(message, "⚠️ Value must be between 1 and 20")
                return True
            if not self._config_lock:
                return True
            async with self._config_lock:
                config.max_recent_users = max_recent
                await self.save_config()
            await utils.answer(message, f"📊 Max recent users set to {max_recent}")
        except ValueError:
            await utils.answer(message, "⚠️ Invalid value. Use integer only.")
        except Exception as e:
            logger.error(f"Error setting recent users: {e}")
            await utils.answer(message, "❌ Error setting recent users")
        return True

    async def _handle_toggle(
        self, message, config: AnnouncementConfig, code: str, active: bool
    ) -> bool:
        try:
            if not self._config_lock:
                return True
            async with self._config_lock:
                config.active = active
                await self.save_config()
            status_msg = self.strings["activated" if active else "deactivated"].format(
                code
            )
            await utils.answer(message, status_msg)
        except Exception as e:
            logger.error(f"Error toggling config: {e}")
            await utils.answer(message, "❌ Error toggling configuration")
        return True

    async def _handle_status(self, message, config: AnnouncementConfig) -> bool:
        try:
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
        except Exception as e:
            logger.error(f"Error showing status: {e}")
            await utils.answer(message, "❌ Error showing status")
        return True

    async def _handle_add_chat(
        self, message, config: AnnouncementConfig, code: str
    ) -> bool:
        try:
            chat_id, topic_id, _, _ = self._safe_get_message_attributes(message)
            if chat_id is None or topic_id is None:
                return True
            if not self._config_lock:
                return True
            async with self._config_lock:
                config.get_chats_set(chat_id).add(topic_id)
                await self.save_config()
            try:
                await message.delete()
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Error adding chat: {e}")
        return True

    async def _handle_list(self, message) -> None:
        try:
            if not self.configs:
                await utils.answer(message, "📋 No announcement configurations found")
                return
            codes_info = []
            for code, config in self.configs.items():
                try:
                    status = "✅" if config.active else "⏸️"
                    total_chats = sum(len(topics) for topics in config.chats.values())
                    codes_info.append(
                        f"• <code>{code}</code> {status} - {total_chats} chats"
                    )
                except Exception as e:
                    logger.error(f"Error processing config '{code}': {e}")
                    codes_info.append(f"• <code>{code}</code> ❌ - Error")
            await utils.answer(
                message, self.strings["list_codes"].format("\n".join(codes_info))
            )
        except Exception as e:
            logger.error(f"Error listing configs: {e}")
            await utils.answer(message, "❌ Error listing configurations")

    async def _handle_flood_wait(self, e: FloodWaitError) -> bool:
        try:
            seconds = getattr(e, "seconds", 60)
            wait_time = max(seconds, 60) + random.uniform(10, 30)
            logger.warning(f"FloodWait: waiting {wait_time:.1f}s")
            await asyncio.sleep(wait_time)
            return True
        except Exception as error:
            logger.error(f"Error handling flood wait: {error}")
            return False

    async def _should_respond_to_user(
        self, config: AnnouncementConfig, chat_id: int, user_id: int
    ) -> bool:
        try:
            recent_users = config.get_recent_users_list(chat_id)
            if not recent_users:
                return True
            current_time = time.time()
            for stored_user_id, timestamp in recent_users:
                if stored_user_id == user_id:
                    time_diff = current_time - timestamp
                    if time_diff < 3600:  # 1 час
                        if random.random() < config.recent_user_probability:
                            return False
                    break
            return True
        except Exception as e:
            logger.error(f"Error checking user response: {e}")
            return True

    async def _update_recent_users(
        self, config: AnnouncementConfig, chat_id: int, user_id: int
    ) -> None:
        try:
            current_time = time.time()
            recent_users = config.get_recent_users_list(chat_id)

            config.recent_users[chat_id] = [
                (uid, ts) for uid, ts in recent_users if uid != user_id
            ]

            config.recent_users[chat_id].insert(0, (user_id, current_time))

            config.recent_users[chat_id] = config.recent_users[chat_id][
                : config.max_recent_users
            ]
        except Exception as e:
            logger.error(f"Error updating recent users: {e}")

    async def _send_announcement(
        self, chat_id: int, config: AnnouncementConfig, topic_id: int, msg_id: int
    ) -> bool:
        try:
            async with config.get_lock(chat_id):
                current_time = time.time()
                last_time = config.last_announcement.get(chat_id, 0)

                interval_seconds = random.uniform(
                    config.interval[0] * 60, config.interval[1] * 60
                )

                if current_time - last_time < interval_seconds:
                    return False
                if not self.rate_limiter or not self.client:
                    return False
                await self.rate_limiter.acquire()

                await self.client.send_message(
                    entity=chat_id,
                    message=html.unescape(config.announcement_text),
                    parse_mode="html",
                    reply_to=msg_id,
                )

                config.last_announcement[chat_id] = current_time
                logger.info(f"Sent announcement to chat {chat_id}")
                return True
        except FloodWaitError as e:
            await self._handle_flood_wait(e)
            return False
        except Exception as e:
            logger.error(f"Error sending announcement to {chat_id}: {e}")
            return False

    async def watcher(self, message):
        try:
            if not message or not hasattr(message, "chat_id"):
                return
            if getattr(message, "out", False):
                await self._handle_command(message)
                return
            chat_id, topic_id, msg_id, user_id = self._safe_get_message_attributes(
                message
            )

            if not all([chat_id, msg_id, user_id]):
                return
            for config in self.configs.values():
                try:
                    if not config.active or chat_id in config.excluded_chats:
                        continue
                    if (
                        chat_id not in config.chats
                        or topic_id not in config.get_chats_set(chat_id)
                    ):
                        continue
                    current_time = time.time()
                    last_time = config.last_announcement.get(chat_id, 0)
                    interval_seconds = random.uniform(
                        config.interval[0] * 60, config.interval[1] * 60
                    )

                    if current_time - last_time >= interval_seconds:
                        if await self._should_respond_to_user(config, chat_id, user_id):
                            await asyncio.sleep(random.uniform(5, 15))

                            if await self._send_announcement(
                                chat_id, config, topic_id, msg_id
                            ):
                                await self._update_recent_users(
                                    config, chat_id, user_id
                                )

                                if self._config_lock:
                                    async with self._config_lock:
                                        await self.save_config()
                                break
                except Exception as e:
                    logger.error(f"Error processing config in watcher: {e}")
                    continue
        except Exception as e:
            logger.error(f"Error in watcher: {e}", exc_info=True)
