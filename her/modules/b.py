import asyncio
import html
import logging
import random
import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from hikkatl.tl.types import (
    Message,
    MessageMediaWebPage,
    ChatBannedRights,
)
from hikkatl.tl.functions.messages import GetDialogFiltersRequest
from hikkatl.errors import (
    FloodWaitError,
    SlowModeWaitError,
)
from hikkatl.tl.functions.channels import (
    GetFullChannelRequest,
    EditBannedRequest,
)

from .. import loader, utils
from ..tl_cache import CustomTelegramClient

logger = logging.getLogger(__name__)


class RateLimiter:
    """Rate limiting implementation"""

    def __init__(self):
        self.tokens = 5
        self.last_update = datetime.now()

    async def acquire(self):
        now = datetime.now()

        time_passed = (now - self.last_update).total_seconds()
        self.tokens = min(5, self.tokens + int(time_passed * 5 / 60))

        if self.tokens <= 0:
            wait_time = 16 + random.uniform(3, 7)
            await asyncio.sleep(wait_time)
        self.tokens -= 1
        self.last_update = now


class SimpleCache:
    def __init__(self, ttl: int = 7200, max_size: int = 5):
        self._active = True
        self.cache = OrderedDict()
        self.ttl = ttl
        self.max_size = max_size

    async def clean_expired(self):
        current_time = time.time()
        expired = [
            k
            for k, (expire_time, _) in self.cache.items()
            if current_time > expire_time
        ]
        for key in expired:
            del self.cache[key]
        while len(self.cache) > self.max_size:
            self.cache.popitem(last=False)

    async def get(self, key: tuple):
        entry = self.cache.get(key)
        if not entry:
            return None
        expire_time, value = entry
        if time.time() > expire_time:
            del self.cache[key]
            return None
        self.cache.move_to_end(key)
        return value

    async def set(self, key: tuple, value, expire: Optional[int] = None):
        ttl = expire if expire is not None else self.ttl
        expire_time = time.time() + ttl

        if key in self.cache:
            del self.cache[key]
        self.cache[key] = (expire_time, value)

        while len(self.cache) > self.max_size:
            self.cache.popitem(last=False)

    async def start_auto_cleanup(self):
        while self._active:
            await self.clean_expired()
            try:
                await asyncio.sleep(self.ttl)
            except asyncio.CancelledError:
                break


class BroadcastMod(loader.Module):
    """."""

    def __init__(self):
        self.manager = None

    async def client_ready(self):
        self.manager = BroadcastManager(self.client, self.db, self.tg_id)
        await self.manager.load_config()

        self.manager.adaptive_interval_task = asyncio.create_task(
            self.manager.start_adaptive_interval_adjustment()
        )
        self.manager.cache_cleanup_task = asyncio.create_task(
            self.manager._message_cache.start_auto_cleanup()
        )

        for code_name, code in self.manager.codes.items():
            if code._active and code.messages and code.chats:
                self.manager.broadcast_tasks[code_name] = asyncio.create_task(
                    self.manager._broadcast_loop(code_name)
                )

    async def on_unload(self):
        if not hasattr(self, "manager"):
            return
        self.manager._active = False

        tasks = []
        tasks.extend(self.manager.broadcast_tasks.values())

        if self.manager.adaptive_interval_task:
            tasks.append(self.manager.adaptive_interval_task)
        if self.manager.cache_cleanup_task:
            tasks.append(self.manager.cache_cleanup_task)
        for task in tasks:
            if task and not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if hasattr(self.manager, "_message_cache"):
            await self.manager._message_cache.clean_expired()

    async def watcher(self, message):
        """Watcher method to handle incoming messages"""
        try:
            if (
                not hasattr(message, "text")
                or not isinstance(message.text, str)
                or not message.out
            ):
                return
            cmd_prefixes = (".b", "(kickall)", "💫")
            if not any(message.text.startswith(prefix) for prefix in cmd_prefixes):
                return
            if message.text.startswith(".b"):
                try:
                    await self.manager.handle_command(message)
                except Exception as e:
                    logger.error(f"Error handling command: {e}", exc_info=True)
                    await utils.answer(message, f"❌ Error: {str(e)}")
                return
            if message.text.startswith("(kickall)"):
                if not message.is_group and not message.is_channel:
                    await utils.answer(
                        message,
                        "⚠️ <b>This command can only be used in groups or channels!</b>",
                    )
                    return
                chat = await message.get_chat()
                if not chat.admin_rights or not chat.admin_rights.ban_users:
                    await utils.answer(
                        message,
                        "❌ <b>I need admin rights with ban users permission!</b>",
                    )
                    return
                await utils.answer(
                    message, "🔄 <b>Starting to remove all members...</b>"
                )

                kicked_count = 0
                failed_count = 0
                start_time = time.time()

                try:
                    my_id = self.tg_id
                    creator_id = None

                    if hasattr(chat, "creator") and chat.creator:
                        try:
                            full_chat = await self.client(
                                GetFullChannelRequest(channel=chat.id)
                            )
                            if hasattr(full_chat.full_chat, "creator_id"):
                                creator_id = full_chat.full_chat.creator_id
                        except Exception as e:
                            logger.error(f"Failed to get chat creator: {e}")
                    async for participant in self.client.iter_participants(chat):
                        if (
                            participant.id == my_id
                            or participant.bot
                            or participant.id == creator_id
                            or (hasattr(participant, "creator") and participant.creator)
                        ):
                            continue
                        try:
                            await self.client(
                                EditBannedRequest(
                                    chat.id,
                                    participant.id,
                                    ChatBannedRights(
                                        until_date=None,
                                        view_messages=True,
                                        send_messages=True,
                                        send_media=True,
                                        send_stickers=True,
                                        send_gifs=True,
                                        send_games=True,
                                        send_inline=True,
                                        embed_links=True,
                                    ),
                                )
                            )
                            kicked_count += 1

                            if kicked_count % 10 == 0 or time.time() - start_time > 5:
                                await utils.answer(
                                    message,
                                    f"🔄 <b>Kicking members in progress...</b>\n"
                                    f"✅ Kicked: {kicked_count}\n"
                                    f"❌ Failed: {failed_count}",
                                )
                                start_time = time.time()
                            await asyncio.sleep(0.5)
                        except Exception as e:
                            logger.error(f"Failed to kick {participant.id}: {e}")
                            failed_count += 1
                    await utils.answer(
                        message,
                        f"✅ <b>Operation completed!</b>\n"
                        f"👢 Total kicked: {kicked_count}\n"
                        f"❌ Failed: {failed_count}",
                    )
                except Exception as e:
                    await utils.answer(
                        message, f"❌ <b>Error getting participants:</b> {str(e)}"
                    )
                return
            if message.text.startswith("💫"):
                parts = message.text.split()
                code_name = parts[0][1:].lower()

                if code_name.isalnum():
                    chat_id = message.chat_id
                    code = self.manager.codes.get(code_name)

                    if code and sum(len(v) for v in code.chats.values()) < 250:
                        await asyncio.sleep(random.uniform(1.5, 5.5))
                        await self.client.get_entity(chat_id)

                        topic_id = utils.get_topic(message) or 0

                        code.chats[chat_id].add(topic_id)

                        new_chat_count = sum(len(v) for v in code.chats.values())
                        safe_min, safe_max = self.manager._calculate_safe_interval(
                            new_chat_count
                        )
                        if code.interval[0] < safe_min:
                            code.interval = (safe_min, safe_max)
                            code.original_interval = code.interval
                        await self.manager.save_config()
        except Exception as e:
            logger.error(f"Ошибка ватчера: {e}", exc_info=True)


@dataclass
class Broadcast:
    chats: Dict[int, Set[int]] = field(default_factory=lambda: defaultdict(set))
    messages: Set[Tuple[int, int]] = field(default_factory=set)
    interval: Tuple[int, int] = (11, 16)
    _active: bool = field(default=False, init=False)
    groups: List[List[Tuple[int, int]]] = field(default_factory=list)
    last_group_chats: Dict[int, Set[int]] = field(
        default_factory=lambda: defaultdict(set)
    )
    original_interval: Tuple[int, int] = (11, 16)


class BroadcastManager:
    """Manages broadcast operations and state."""

    def __init__(self, client: CustomTelegramClient, db, tg_id):
        self.client = client
        self.db = db
        self.tg_id = tg_id
        self._active = True
        self.adaptive_interval_task = None
        self.codes: Dict[str, Broadcast] = {}
        self.broadcast_tasks: Dict[str, asyncio.Task] = {}
        self._message_cache = SimpleCache(ttl=7200, max_size=5)
        self.global_backoff_multiplier = 1.0
        self.pause_event = asyncio.Event()
        self.rate_limiter = RateLimiter()
        self.cache_cleanup_task = None
        self.pause_event.clear()
        self.last_flood_time = 0
        self.flood_wait_times = []

    async def _broadcast_loop(self, code_name: str):
        code = self.codes.get(code_name)
        if not code or not code.messages or not code.chats:
            return
        await asyncio.sleep(random.uniform(code.interval[0], code.interval[1]) * 60)
        while self._active and code._active and not self.pause_event.is_set():
            if not code.messages or not code.chats:
                return
            try:
                current_chats = defaultdict(
                    set, {k: set(v) for k, v in code.chats.items()}
                )
                if code.last_group_chats != current_chats:
                    code.last_group_chats = current_chats.copy()
                    chats = [
                        (chat_id, topic_id)
                        for chat_id, topic_ids in code.chats.items()
                        for topic_id in topic_ids
                    ]
                    random.shuffle(chats)
                    code.groups = [chats[i : i + 5] for i in range(0, len(chats), 5)]
                    code.last_group_chats = current_chats
                msg_tuple = random.choice(tuple(code.messages))
                message = await self._fetch_message(*msg_tuple)
                if not message:
                    code.messages.remove(msg_tuple)
                    await self.save_config()
                    continue
                total_chats = sum(len(v) for v in code.chats.values())
                cycle_minutes = random.uniform(
                    code.interval[0] * self.global_backoff_multiplier,
                    code.interval[1] * self.global_backoff_multiplier,
                )
                cycle_seconds = cycle_minutes * 60

                time_per_chat = cycle_seconds / total_chats

                start_time = time.monotonic()
                chats_processed = 0

                for group in code.groups:
                    tasks = []
                    for chat_data in group:
                        chat_id, topic_id = chat_data
                        tasks.append(self._send_message(chat_id, message, topic_id))
                    await asyncio.gather(*tasks)
                    chats_processed += len(group)

                    if chats_processed < total_chats:
                        target_elapsed = time_per_chat * chats_processed
                        actual_elapsed = time.monotonic() - start_time

                        if actual_elapsed < target_elapsed:
                            await asyncio.sleep(target_elapsed - actual_elapsed)
                elapsed = time.monotonic() - start_time
                if elapsed < cycle_seconds:
                    await asyncio.sleep(cycle_seconds - elapsed)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("⚠️ [%s] Ошибка: %s", code_name, str(e), exc_info=True)

    def _calculate_safe_interval(self, total_chats: int) -> Tuple[int, int]:
        if total_chats <= 2:
            safe_min = 11
        elif total_chats >= 250:
            safe_min = 16
        else:
            safe_min = 11 + (total_chats - 2) * 5 / 245
            safe_min = int(round(safe_min))
        variance = max(5, int(safe_min * 0.2))
        safe_max = safe_min + variance
        safe_max = min(safe_max, 1440)
        return (safe_min, safe_max)

    async def _check_and_adjust_intervals(self):
        """c"""
        if not self.flood_wait_times or self.last_flood_time == 0:
            return
        time_since_last_flood = time.time() - self.last_flood_time
        if time_since_last_flood > 43200:
            for code in self.codes.values():
                code.interval = code.original_interval
            self.flood_wait_times = []
        else:
            for code in self.codes.values():
                new_min = max(11, int(code.interval[0] * 0.85))
                new_max = max(min(int(code.interval[1] * 0.85), 1440), new_min + 5)
                code.interval = (new_min, new_max)
        await self.save_config()

    async def _fetch_message(self, chat_id: int, message_id: int):
        cache_key = (chat_id, message_id)

        if cached := await self._message_cache.get(cache_key):
            return cached
        try:
            await asyncio.sleep(random.uniform(1.5, 5.5))
            msg = await self.client.get_messages(entity=chat_id, ids=message_id)
            if not msg:
                return None
            await self._message_cache.set(cache_key, msg, expire=3600)
            return msg
        except Exception as e:
            logger.error(f"Ошибка получения: {e}")
            return None

    async def _generate_stats_report(self) -> str:
        """.b l"""
        if not self.codes:
            return "😶‍🌫️ Нет активных рассылок"
        report = ["🎩 <strong>Статистика рассылок</strong>"]
        for code_name, code in self.codes.items():
            report.append(
                f"\n▸ <code>{code_name}</code> {'✨' if code._active else '🧊'}\n"
                f"├ Сообщений: {len(code.messages)}\n"
                f"├ Интервал: {code.interval[0]}-{code.interval[1]} мин\n"
                f"└ Цели: {sum(len(v) for v in code.chats.values())}\n"
            )
        return "".join(report)

    async def _handle_add(self, message, code, code_name, args) -> str:
        """.b a [code]"""
        reply = await message.get_reply_message()
        if not reply:
            return "🫵 Ответьте на сообщение"
        if not code:
            code = Broadcast()
            self.codes[code_name] = code
        key = (reply.chat_id, reply.id)
        if key in code.messages:
            return "ℹ️ Сообщение уже добавлено"
        code.messages.add(key)
        await self._message_cache.set(key, reply)
        await self.save_config()

        return f"🍑 <code>{code_name}</code> | Сообщений: {len(code.messages)}"

    async def _handle_add_chat(self, message, code, code_name, args) -> str:
        """.b ac [code] [@chat] [topic_id]"""
        if len(args) < 3:
            return "🫵 Укажите чат"
        target = args[2]
        topic_id = int(args[3]) if len(args) > 3 else None

        chat_id = await self._parse_chat_identifier(target)
        if not chat_id:
            return "🫵 Неверный формат чата"
        try:
            if topic_id:
                await asyncio.sleep(random.uniform(1.5, 5.5))
                await self.client.get_messages(chat_id, ids=topic_id)
        except Exception:
            return "🫵 Топик не существует или недоступен"
        code.chats[chat_id].add(topic_id or 0)

        await self.save_config()
        return f"🪴 +1 {'топик' if topic_id else 'чат'} | Всего: {sum(len(v) for v in code.chats.values())}"

    async def _handle_delete(self, message, code, code_name, args) -> str:
        """.b d [code]"""
        if code_name in self.broadcast_tasks:
            self.broadcast_tasks[code_name].cancel()
        del self.codes[code_name]
        await self.save_config()
        return f"🗑 {code_name} удалена"

    async def _handle_interval(self, message, code, code_name, args) -> str:
        """Handle interval setting with safe interval check"""
        if len(args) < 4:
            return "Укажите мин/макс"
        try:
            requested_min = int(args[2])
            requested_max = int(args[3])
            if requested_min >= requested_max:
                return "🛑 Минимум должен быть меньше максимума"
            if requested_max > 1440:
                return "🛑 Максимальный интервал не может превышать 1440 минут"
            safe_min, safe_max = self._calculate_safe_interval(len(code.chats))

            if requested_min < safe_min:
                requested_min = safe_min
                requested_max = safe_max
        except ValueError:
            return "Некорректные значения"
        code.interval = (requested_min, requested_max)
        code.original_interval = code.interval
        self.flood_wait_times = []
        await self.save_config()
        return f"⏱️ Интервал для {code_name}: {requested_min}-{requested_max} мин"

    async def _handle_flood_wait(self, e: FloodWaitError, chat_id: int):
        """FloodWait"""
        if self.pause_event.is_set():
            return False
        self.last_flood_time = time.time()
        self.pause_event.set()
        avg_wait = (
            sum(self.flood_wait_times[-3:]) / len(self.flood_wait_times[-3:])
            if self.flood_wait_times
            else 0
        )
        wait_time = min(max(e.seconds + 5, avg_wait * 1.5), 7200)

        self.flood_wait_times.append(wait_time)

        if len(self.flood_wait_times) > 10:
            self.global_backoff_multiplier *= 1.5
            self.flood_wait_times = self.flood_wait_times[-10:]
        await self.client.send_message(
            self.tg_id,
            f"🚨 Обнаружен FloodWait {e.seconds}s! Все рассылки приостановлены на {wait_time}s",
        )

        tasks_to_cancel = list(self.broadcast_tasks.values())
        self.broadcast_tasks.clear()

        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()
        if tasks_to_cancel:
            for task in tasks_to_cancel:
                try:
                    if not task.done():
                        await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                except Exception as ex:
                    logger.error(f"Error during task cancellation: {ex}")
        await asyncio.sleep(wait_time)

        try:
            await asyncio.sleep(random.uniform(1.5, 5.5))
            await self.client.get_entity(chat_id)
        except Exception as e:
            logger.error(f"Failed to get entity for chat {chat_id}: {e}")
        self.pause_event.clear()
        await self._restart_all_broadcasts()
        await self.client.send_message(
            self.tg_id,
            "🐈 Глобальная пауза снята. Рассылки возобновлены",
        )

        for code in self.codes.values():
            code.interval = (
                min(code.interval[0] * 2, 120),
                min(code.interval[1] * 2, 240),
            )
            if not hasattr(code, "original_interval"):
                code.original_interval = code.interval
        await self.save_config()

    async def _handle_permanent_error(
        self, chat_id: int, topic_id: Optional[int] = None
    ):
        """d"""
        modified = False
        for code in self.codes.values():
            if chat_id in code.chats:
                if topic_id in code.chats[chat_id]:
                    code.chats[chat_id].discard(topic_id)
                    modified = True

                    if not code.chats[chat_id]:
                        del code.chats[chat_id]
                code.last_group_chats = defaultdict(set)
        if modified:
            await self.save_config()

    async def _handle_remove(self, message, code, code_name, args) -> str:
        """.b r [code]"""
        reply = await message.get_reply_message()
        if not reply:
            return "🫵 Ответьте на сообщение"
        key = (reply.chat_id, reply.id)
        if key not in code.messages:
            return "🫵 Сообщение не найдено"
        code.messages.remove(key)
        await self._message_cache.set(key, None)
        await self.save_config()
        return f"🐀 Удалено | Осталось: {len(code.messages)}"

    async def _handle_remove_chat(self, message, code, code_name, args) -> str:
        """.b rc [code] [@chat]"""
        if len(args) < 3:
            return "🫵 Укажите чат для удаления"
        target = args[2]
        chat_id = await self._parse_chat_identifier(target)

        if not chat_id:
            return "🫵 Неверный формат чата или чат недоступен"
        if chat_id in code.chats:
            del code.chats[chat_id]
            await self.save_config()
            return f"🐲 -1 чат | Осталось: {sum(len(v) for v in code.chats.values())}"
        if str(chat_id).startswith("-100"):
            alternative_id = int(str(chat_id)[4:])
        else:
            alternative_id = int(f"-100{chat_id}")
        if alternative_id in code.chats:
            del code.chats[alternative_id]
            await self.save_config()
            return f"🐲 -1 чат | Осталось: {sum(len(v) for v in code.chats.values())}"
        return "ℹ️ Чат не найден"

    async def _handle_start(self, message, code, code_name, args) -> str:
        """.b s [code]"""
        if not code.messages:
            return "🫵 Нет сообщений для отправки"
        if not code.chats:
            return "🫵 Нет чатов для рассылки"
        if code._active:
            return "ℹ️ Рассылка уже активна"
        code._active = True
        self.broadcast_tasks[code_name] = asyncio.create_task(
            self._broadcast_loop(code_name)
        )

        await self.save_config()

        return f"🚀 {code_name} запущена | Чатов: {len(code.chats)}"

    async def _handle_stop(self, message, code, code_name, args) -> str:
        """.b x [code]"""
        if not code._active:
            return "ℹ️ Рассылка не активна"
        code._active = False
        if code_name in self.broadcast_tasks:
            self.broadcast_tasks[code_name].cancel()
        await self.save_config()

        return f"🧊 {code_name} остановлена"

    async def _parse_chat_identifier(self, identifier) -> Optional[int]:
        """p"""
        try:
            if isinstance(identifier, str):
                identifier = identifier.strip()

                if identifier.startswith(("https://t.me/", "t.me/")):
                    parts = identifier.rstrip("/").split("/")
                    identifier = parts[-1]
                if identifier.replace("-", "").isdigit():
                    chat_id = int(identifier)

                    await asyncio.sleep(random.uniform(1.5, 5.5))
                    try:
                        await self.client.get_entity(chat_id, exp=3600)
                        return chat_id
                    except Exception:
                        if not str(chat_id).startswith("-100") and str(
                            chat_id
                        ).startswith("-"):
                            try:
                                modified_id = int(f"-100{str(chat_id)[1:]}")
                                await self.client.get_entity(modified_id, exp=3600)
                                return modified_id
                            except Exception:
                                pass
            await asyncio.sleep(random.uniform(1.5, 5.5))
            entity = await self.client.get_entity(identifier, exp=3600)

            if hasattr(entity, "__class__") and entity.__class__.__name__ == "Channel":
                return int(f"-100{entity.id}")
            return entity.id
        except Exception as e:
            logger.error(f"Error parsing chat identifier '{identifier}': {e}")
            return None

    async def _restart_all_broadcasts(self):
        for code_name, code in self.codes.items():
            if code._active:
                if task := self.broadcast_tasks.get(code_name):
                    if not task.done() and not task.cancelled():
                        task.cancel()
                        try:
                            await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
                        except (asyncio.CancelledError, asyncio.TimeoutError):
                            pass
                        except Exception as e:
                            logger.error(f"Ошибка задачи {code_name}: {e}")
                self.broadcast_tasks[code_name] = asyncio.create_task(
                    self._broadcast_loop(code_name)
                )
                active = sum(1 for code in self.codes.values() if code._active)

    async def _scan_folders_for_chats(self):
        """s"""
        try:
            await asyncio.sleep(random.uniform(1.5, 5.5))

            stats = {
                "processed": 0,
                "added": 0,
            }

            try:
                folders = await self.client(GetDialogFiltersRequest())
            except Exception as e:
                logger.error(f"🚨 Ошибка получения папок: {e}")
                return "❌ Не удалось получить список папок"
            for folder in folders:
                folder_title = getattr(folder, "title", "").strip()

                if not folder_title.lower().endswith("$"):
                    continue
                folder_id = getattr(folder, "id", None)
                if not folder_id or not isinstance(folder_id, int):
                    continue
                stats["processed"] += 1

                try:
                    if hasattr(folder, "include_peers") and folder.include_peers:
                        peers = []
                        for peer in folder.include_peers:
                            try:
                                entity = await self.client.get_entity(peer)
                                peers.append(entity)
                            except Exception as e:
                                logger.error(
                                    f"Не удалось получить сущность для peer {peer}: {e}"
                                )
                    added = 0
                    for peer in peers:
                        if self._process_peer(peer, folder_title):
                            added += 1
                    await self.save_config()
                    stats["added"] += added
                except Exception as e:
                    logger.error(f"🔥 Ошибка: {e}", exc_info=True)
            report = [
                "📊 Итоги сканирования:",
                f"• Всего папок: {len(folders)}",
                f"• Обработано: {stats['processed']}",
                f"• Добавлено чатов: {stats['added']}",
            ]
            return "\n".join(report)
        except Exception as e:
            logger.critical(f"💥 Фатальная ошибка: {e}", exc_info=True)
            return f"🚨 Критическая ошибка: {e}"

    def _process_peer(self, peer, folder_title: str) -> bool:
        """p"""
        try:
            if hasattr(peer, "broadcast") and peer.broadcast:
                return False
            if hasattr(peer, "forum") and peer.forum:
                return False
            if hasattr(peer, "__class__") and peer.__class__.__name__ == "Channel":
                if not getattr(peer, "megagroup", False):
                    return False
            folder_parts = folder_title[:-1].strip().lower().split()
            if not folder_parts:
                return False
            code_name = folder_parts[0]

            interval_min = None
            for part in folder_parts:
                if part.endswith("m") and part[:-1].isdigit():
                    interval_min = int(part[:-1])
                    break
            if code_name not in self.codes:
                self.codes[code_name] = Broadcast()
            original_id = peer.id
            if hasattr(peer, "__class__") and peer.__class__.__name__ == "Channel":
                chat_id = int(f"-100{original_id}")
            else:
                chat_id = original_id
            if not hasattr(self.codes[code_name], "chats"):
                self.codes[code_name].chats = defaultdict(set)
            if (
                chat_id not in self.codes[code_name].chats
                or 0 not in self.codes[code_name].chats[chat_id]
            ):
                if chat_id not in self.codes[code_name].chats:
                    self.codes[code_name].chats[chat_id] = set()
                self.codes[code_name].chats[chat_id].add(0)

                if interval_min is not None:
                    total_chats = sum(
                        len(v) for v in self.codes[code_name].chats.values()
                    )
                    safe_min, safe_max = self._calculate_safe_interval(total_chats)

                    self.codes[code_name].interval = (
                        max(interval_min, safe_min),
                        max(interval_min + 5, safe_max),
                    )
                    self.codes[code_name].original_interval = self.codes[
                        code_name
                    ].interval
                return True
            return False
        except Exception as e:
            logger.error(
                f"⚠️ Ошибка обработки чата {getattr(peer, 'id', 'Unknown')}: {e}",
                exc_info=True,
            )
            return False

    async def _send_message(
        self, chat_id: int, msg: Message, topic_id: Optional[int] = None
    ) -> bool:
        """f"""
        if self.pause_event.is_set():
            return False
        try:
            await self.rate_limiter.acquire()
            await asyncio.sleep(random.uniform(1.5, 5.5))

            send_args = {"entity": chat_id}
            if topic_id not in (None, 0):
                send_args["reply_to"] = topic_id
            message = html.unescape(msg.text) if msg.text else None

            if msg.media and not isinstance(msg.media, MessageMediaWebPage):
                await self.client.send_file(
                    file=msg.media,
                    caption=message,
                    parse_mode="html",
                    **send_args,
                )
            else:
                await self.client.send_message(
                    message=message,
                    parse_mode="html",
                    **send_args,
                )
            return True
        except FloodWaitError as e:
            await self._handle_flood_wait(e, chat_id)
            return False
        except SlowModeWaitError as e:
            logger.error("⌛ [%d] SlowModeWait %d сек.", chat_id, e.seconds)
            return False
        except Exception as e:
            logger.error(f"Unexpected error in chat {chat_id}: {repr(e)}")
            await self._handle_permanent_error(chat_id, topic_id)
            return False

    async def _tog(self, args) -> str:
        """a"""
        try:
            result = await self._scan_folders_for_chats()
            return f"🐺 Папки просканированы\n\n{result}"
        except Exception as e:
            logger.error(f"Ошибка при сканировании папок: {e}", exc_info=True)
            return f"🐺 Ошибка сканирования: {str(e)}"

    async def handle_command(self, message):
        """p"""
        response = None
        args = message.text.split()[1:]

        if not args:
            response = "🫵 Недостаточно аргументов"
        else:
            action = args[0].lower()

            if action == "l":
                response = await self._generate_stats_report()
            elif action == "w":
                await utils.answer(message, "💫")
                response = await self._tog(args)
            else:
                code_name = args[1].lower() if len(args) > 1 else None
                if not code_name:
                    response = "🫵 Укажите код рассылки"
                else:
                    code = self.codes.get(code_name)
                    handler_map = {
                        "a": self._handle_add,
                        "d": self._handle_delete,
                        "r": self._handle_remove,
                        "ac": self._handle_add_chat,
                        "rc": self._handle_remove_chat,
                        "i": self._handle_interval,
                        "s": self._handle_start,
                        "x": self._handle_stop,
                    }

                    if action not in handler_map:
                        response = "🫵 Неизвестная команда"
                    elif action != "a" and not code:
                        response = f"🫵 Рассылка {code_name} не найдена"
                    else:
                        try:
                            handler = handler_map[action]
                            result = await handler(message, code, code_name, args)
                            response = result
                        except Exception as e:
                            response = f"🚨 Ошибка: {str(e)}"
        await utils.answer(message, response)

    async def load_config(self):
        """l"""
        try:
            raw_config = self.db.get("broadcast", "config") or {}

            for code_name, code_data in raw_config.get("codes", {}).items():
                try:
                    chats = defaultdict(set)
                    for chat_id, topic_ids in code_data.get("chats", {}).items():
                        chats[int(chat_id)] = set(map(int, topic_ids))
                    last_group_chats = defaultdict(set)
                    for chat_id, topic_ids in code_data.get(
                        "last_group_chats", {}
                    ).items():
                        last_group_chats[int(chat_id)] = set(map(int, topic_ids))
                    code = Broadcast(
                        chats=chats,
                        messages={
                            (int(msg["chat_id"]), int(msg["message_id"]))
                            for msg in code_data.get("messages", [])
                        },
                        interval=tuple(map(int, code_data.get("interval", (11, 16)))),
                        original_interval=tuple(
                            map(int, code_data.get("original_interval", (11, 16)))
                        ),
                        last_group_chats=last_group_chats,
                    )

                    code.groups = [
                        [tuple(map(int, chat_data)) for chat_data in group]
                        for group in code_data.get("groups", [])
                    ]

                    code._active = code_data.get("active", False)
                    self.codes[code_name] = code
                except Exception as e:
                    logger.error(f"Ошибка загрузки {code_name}: {str(e)}")
                    continue
            for code_name, code in self.codes.items():
                if code._active and (not code.messages or not code.chats):
                    code._active = False
        except Exception as e:
            logger.error(f"Критическая ошибка загрузки: {str(e)}", exc_info=True)
            self.codes = {}

    async def save_config(self):
        """s"""
        try:
            config = {
                "codes": {
                    name: {
                        "chats": {
                            int(chat_id): list(topic_ids)
                            for chat_id, topic_ids in dict(code.chats).items()
                        },
                        "messages": [
                            {"chat_id": cid, "message_id": mid}
                            for cid, mid in code.messages
                        ],
                        "interval": list(code.interval),
                        "original_interval": list(code.original_interval),
                        "active": code._active,
                        "groups": [
                            [list(chat_data) for chat_data in group]
                            for group in code.groups
                        ],
                        "last_group_chats": {
                            int(k): list(v)
                            for k, v in dict(code.last_group_chats).items()
                        },
                    }
                    for name, code in self.codes.items()
                }
            }
            try:
                self.db.set("broadcast", "config", config)
            except Exception as e:
                logger.error(f"Database error during save: {e}")
                raise
        except Exception as e:
            logger.error(f"Critical error during save: {e}")
            raise

    async def start_adaptive_interval_adjustment(self):
        """p"""
        while self._active:
            try:
                await asyncio.sleep(1800)
                await self._check_and_adjust_intervals()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в адаптивной регулировке: {e}", exc_info=True)
