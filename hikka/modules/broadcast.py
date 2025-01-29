import asyncio
import logging
import random
import time
from collections import deque, OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union

from telethon.tl.types import Message
from telethon.errors import (
    ChatWriteForbiddenError,
    UserBannedInChannelError,
    FloodWaitError,
)

from .. import loader, utils, _internal

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class RateLimiter:
    def __init__(self, max_requests: int, time_window: int):
        self._lock = asyncio.Lock()
        self.max_requests = max_requests
        self.time_window = time_window
        self.semaphore = asyncio.Semaphore(max_requests)
        self.timestamps = deque(maxlen=max_requests * 2)

    async def acquire(self):
        async with self._lock:
            current_time = time.monotonic()

            while (
                self.timestamps
                and self.timestamps[0] <= current_time - self.time_window
            ):
                self.timestamps.popleft()
            if len(self.timestamps) >= self.max_requests:
                wait_time = self.timestamps[0] + self.time_window - current_time
                await asyncio.sleep(wait_time)
                current_time = time.monotonic()
                while (
                    self.timestamps
                    and self.timestamps[0] <= current_time - self.time_window
                ):
                    self.timestamps.popleft()
            self.timestamps.append(current_time)


class SimpleCache:
    def __init__(self, ttl: int = 7200, max_size: int = 50):
        self.cache = OrderedDict()
        self.ttl = ttl
        self.max_size = max_size
        self._lock = asyncio.Lock()

    async def get(self, key: tuple) -> Optional[Any]:
        """
        Get a value from cache using a tuple key with improved type checking
        """
        if not isinstance(key, tuple):
            raise ValueError("Cache key must be a tuple")
            
        async with self._lock:
            entry = self.cache.get(key)
            if not entry:
                return None
                
            expire_time, value = entry
            if time.time() > expire_time:
                del self.cache[key]
                return None
                
            # Move to end (LRU behavior)
            self.cache.move_to_end(key)
            return value

    async def set(self, key: tuple, value: Any, expire: Optional[int] = None) -> None:
        """
        Set a value in cache using a tuple key with improved validation
        """
        if not isinstance(key, tuple):
            raise ValueError("Cache key must be a tuple")
            
        async with self._lock:
            if expire is not None and expire <= 0:
                return
                
            ttl = expire if expire is not None else self.ttl
            expire_time = time.time() + ttl
            
            # Remove existing entry if present
            if key in self.cache:
                del self.cache[key]
                
            # Add new entry
            self.cache[key] = (expire_time, value)
            
            # Maintain size limit (LRU eviction)
            while len(self.cache) > self.max_size:
                self.cache.popitem(last=False)

    async def clean_expired(self, force: bool = False) -> None:
        """Clean expired entries with improved efficiency"""
        async with self._lock:
            if not force and len(self.cache) < self.max_size // 2:
                return
                
            current_time = time.time()
            expired_keys = [
                k for k, (expire_time, _) in self.cache.items()
                if current_time > expire_time
            ]
            
            for key in expired_keys:
                del self.cache[key]
                
            if expired_keys:
                logger.debug(f"Cleaned {len(expired_keys)} expired entries from cache")

    async def start_auto_cleanup(self):
        """Запускает фоновую задачу для периодической очистки кэша"""
        while True:
            await self.clean_expired()
            logger.debug("Периодическая очистка выполнена")
            await asyncio.sleep(self.ttl)


class BroadcastMod(loader.Module):
    """Модуль для массовой рассылки."""

    strings = {"name": "Broadcast"}

    async def brcmd(self, message):
        """Команда для управления рассылкой."""
        await self.manager.handle_command(message)

    async def client_ready(self):
        """Initialization sequence"""
        self.manager = BroadcastManager(self._client, self.db, self._client.tg_id)
        self.manager._message_cache = SimpleCache(ttl=14400, max_size=200)
        try:
            await asyncio.wait_for(self.manager._load_config(), timeout=30)
            await self.manager.start_cache_cleanup()
            self.manager.adaptive_interval_task = asyncio.create_task(
                self.manager.start_adaptive_interval_adjustment()
            )
            self._initialized = True
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            self._initialized = False

    async def on_unload(self):
        self._active = False

        for task in self.manager.broadcast_tasks.values():
            if not task.done():
                task.cancel()
        results = await asyncio.gather(
            *self.manager.broadcast_tasks.values(), return_exceptions=True
        )

        if self.manager.adaptive_interval_task:
            self.manager.adaptive_interval_task.cancel()
        if self.manager.cache_cleanup_task:
            self.manager.cache_cleanup_task.cancel()

    async def watcher(self, message: Message):
        """Автоматически добавляет чаты в рассылку."""
        if not hasattr(self, "manager") or self.manager is None:
            return
        if not self.manager.watcher_enabled:
            return
        if not (message and message.text and message.text.startswith("!")):
            return
        if message.sender_id != self.tg_id:
            return
        parts = message.text.split()
        code_name = parts[0][1:]
        if not code_name.isalnum():
            return
        chat_id = message.chat_id
        code = self.manager.codes.get(code_name)
        if not code:
            return
        if len(code.chats) >= 500:
            return
        if chat_id not in code.chats:
            code.chats.add(chat_id)
            await self.manager.save_config()


@dataclass
class Broadcast:
    """Основной класс для управления рассылкой"""

    chats: Set[int] = field(default_factory=set)
    messages: set = field(default_factory=set)
    interval: Tuple[int, int] = (10, 13)
    batch_mode: bool = False
    _last_message_index: int = field(default=0, init=False)
    _active: bool = field(default=False, init=False)
    original_interval: Tuple[int, int] = (10, 13)

    def add_message(
        self, chat_id: int, message_id: int, grouped_ids: List[int] = None
    ) -> bool:
        if grouped_ids:
            grouped_ids = sorted(list(set(grouped_ids)))
            if len(grouped_ids) == 1:
                grouped_ids = None
        key = (chat_id, message_id, tuple(grouped_ids) if grouped_ids else ())
        if key in self.messages:
            return False
        self.messages.add(key)
        return True

    def get_next_message_index(self):
        if not self.messages:
            raise ValueError("No messages")
        self._last_message_index = (self._last_message_index + 1) % len(self.messages)
        return self._last_message_index

    def is_valid_interval(self) -> bool:
        """Проверяет корректность интервала"""
        min_val, max_val = self.interval
        return (
            isinstance(min_val, int)
            and isinstance(max_val, int)
            and 0 < min_val < max_val <= 1440
        )

    def remove_message(
        self, 
        chat_id: int, 
        message_id: int, 
        grouped_ids: Optional[int] = None
    ) -> bool:
        removed_count = 0
        keys_to_remove = []

        for key in list(self.messages):
            msg_chat_id, msg_message_id, msg_grouped_ids = key

            if msg_chat_id != chat_id or msg_message_id != message_id:
                continue

            if grouped_ids:
                if msg_grouped_ids and msg_grouped_ids[0] == grouped_ids:
                    keys_to_remove.append(key)
            else:
                if not msg_grouped_ids:
                    keys_to_remove.append(key)

        if keys_to_remove:
            for key in keys_to_remove:
                self.messages.remove(key)
                removed_count += 1

        return removed_count > 0


class BroadcastManager:
    """Manages broadcast operations and state."""

    MAX_BATCH_SIZE = 25
    GLOBAL_LIMITER = RateLimiter(max_requests=20, time_window=60)

    class MediaPermissions:
        NONE = 0
        TEXT_ONLY = 1
        FULL_MEDIA = 2

    def __init__(self, client, db, tg_id):
        self.client = client
        self.db = db
        self.codes: Dict[str, Broadcast] = {}
        self.broadcast_tasks: Dict[str, asyncio.Task] = {}
        self._message_cache = SimpleCache(ttl=7200, max_size=50)
        self.valid_chats_cache = SimpleCache(ttl=7200, max_size=500)
        self._active = True
        self._lock = asyncio.Lock()
        self.watcher_enabled = False
        self.cache_cleanup_task = None
        self.tg_id = tg_id
        self._semaphore = asyncio.Semaphore(3)
        self.pause_event = asyncio.Event()
        self.pause_event.clear()
        self.last_flood_time = 0
        self.flood_wait_times = []
        self.adaptive_interval_task = None

    async def _broadcast_loop(self, code_name: str):
        """Main broadcast loop with enhanced debug logging"""
        code = self.codes.get(code_name)
        if not code or not code.messages:
            logger.error(f"Нет сообщений или кода для {code_name}")
            return
        await _internal.fw_protect()
        await asyncio.sleep(
            random.uniform(code.interval[0] * 60, code.interval[1] * 60)
        )
        while self._active and code._active and not self.pause_event.is_set():
            if self.pause_event.is_set() or not self._active:
                break
            start_time = time.time()
            deleted_messages = []
            messages_to_send = []

            try:
                current_messages = list(code.messages)
                if not current_messages:
                    await asyncio.sleep(30)
                    continue
                if not current_messages:
                    batches = []
                batches = [
                    current_messages[i : i + 5]
                    for i in range(0, len(current_messages), 5)
                ]

                async with self._semaphore:
                    for batch in batches:
                        batch_messages, deleted = await self._process_message_batch(
                            batch
                        )
                        messages_to_send.extend(batch_messages)
                        deleted_messages.extend(deleted)
                    if deleted_messages:
                        code.messages -= set(deleted_messages)
                        await self.save_config()
                if not messages_to_send:
                    logger.error(
                        f"[{code_name}] Нет валидных сообщений для отправки. Проверьте исходные сообщения и кэш."
                    )
                    await asyncio.sleep(30)
                    continue
                if not code.batch_mode:
                    next_index = code.get_next_message_index()
                    messages_to_send = [
                        messages_to_send[next_index % len(messages_to_send)]
                    ]
                failed_chats = await self._send_messages_to_chats(
                    code, messages_to_send, code_name
                )

                if failed_chats:
                    await self._handle_failed_chats(code_name, failed_chats)
                elapsed = time.time() - start_time
                min_interval = max(0, code.interval[0] * 60 - elapsed)
                max_interval = max(2, code.interval[1] * 60 - elapsed)

                await _internal.fw_protect()
                await asyncio.sleep(random.uniform(min_interval, max_interval))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"[{code_name}] Error in broadcast loop: {str(e)}",
                    exc_info=True,
                )
                await asyncio.sleep(30)
                continue

    async def _check_and_adjust_intervals(self):
        """Проверка условий для восстановления интервалов"""
        async with self._lock:
            if not self.flood_wait_times:
                return
            if (time.time() - self.last_flood_time) > 43200:
                for code in self.codes.values():
                    code.interval = code.original_interval
                    if not code.is_valid_interval():
                        code.interval = (10, 13)
                self.flood_wait_times = []
                await self.save_config()
                await self.client.send_message(
                    self.tg_id,
                    "🔄 12 часов без ошибок! Интервалы восстановлены до исходных",
                )
            else:
                for code_name, code in self.codes.items():
                    new_min = max(1, int(code.interval[0] * 0.85))
                    new_max = max(2, int(code.interval[1] * 0.85))

                    new_max = max(new_max, new_min + 1)
                    new_max = min(new_max, 1440)

                    code.interval = (new_min, new_max)
                    if not code.is_valid_interval():
                        code.interval = code.original_interval
                        logger.error(
                            f"Invalid interval for {code_name}, reset to original"
                        )
                    await self.client.send_message(
                        self.tg_id,
                        f"⏱ Автокоррекция интервалов для {code_name}: {new_min}-{new_max} минут",
                    )
                await self.save_config()

    async def _fetch_messages(self, msg_data: dict) -> Optional[Message]:
        """
        Fetch a message from cache or Telegram with improved error handling and caching
        """
        try:
            chat_id = msg_data["chat_id"]
            message_id = msg_data["message_id"]
            
            # Use tuple as cache key
            cache_key = (chat_id, message_id)
            
            # Try to get from cache first
            cached_msg = await self._message_cache.get(cache_key)
            if cached_msg is not None:
                return cached_msg

            # If not in cache, fetch from Telegram
            try:
                msg = await self.client.get_messages(entity=chat_id, ids=message_id)
                if msg:
                    # Store in cache using tuple key
                    await self._message_cache.set(cache_key, msg)
                    logger.debug(f"Message {chat_id}:{message_id} cached successfully")
                    return msg
                else:
                    logger.error(f"Message {chat_id}:{message_id} not found")
                    return None
            except ValueError as e:
                logger.error(f"Chat/message does not exist: {chat_id} {message_id}: {e}")
                return None

        except Exception as e:
            logger.error(f"Error in _fetch_messages: {e}", exc_info=True)
            return None

    async def _get_chat_id(self, chat_identifier: str) -> Optional[int]:
        """Получает ID чата из разных форматов (ссылка, юзернейм, ID)"""
        try:
            if chat_identifier.lstrip("-").isdigit():
                return int(chat_identifier)
            clean_username = chat_identifier.lower()
            for prefix in ["https://", "http://", "t.me/", "@", "telegram.me/"]:
                clean_username = clean_username.replace(prefix, "")
            entity = await self.client.get_entity(clean_username)
            return entity.id
        except Exception:
            return None

    async def _handle_flood_wait(self, e: FloodWaitError, chat_id: int):
        """Глобальная обработка FloodWait с остановкой всех рассылок"""
        async with self._lock:
            if self.pause_event.is_set():
                return False
            self.pause_event.set()
            avg_wait = (
                sum(self.flood_wait_times[-3:]) / len(self.flood_wait_times[-3:])
                if self.flood_wait_times
                else 0
            )
            wait_time = max(e.seconds + 15, avg_wait * 1.5)

            wait_time = min(wait_time, 3600)

            self.last_flood_time = time.time()
            self.flood_wait_times.append(wait_time)
            if len(self.flood_wait_times) > 10:
                self.flood_wait_times = self.flood_wait_times[-10:]
            await self.client.send_message(
                self.tg_id,
                f"🚨 Обнаружен FloodWait {e.seconds}s! Все рассылки приостановлены на {wait_time}s",
            )
            logger.warning(
                f"🚨 FloodWait {e.seconds} сек. в чате {chat_id}. Среднее время ожидания: {avg_wait:.1f} сек. "
                f"Всего FloodWait за последние 12 часов: {len(self.flood_wait_times)}"
            )

            tasks = list(self.broadcast_tasks.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await _internal.fw_protect()
            await asyncio.sleep(wait_time)

            self.pause_event.clear()
            await self._restart_all_broadcasts()

            await self.client.send_message(
                self.tg_id,
                "✅ Глобальная пауза снята. Рассылки возобновлены",
            )

            for code in self.codes.values():
                code.interval = (
                    min(code.interval[0] * 2, 120),
                    min(code.interval[1] * 2, 240),
                )
                if not hasattr(code, "original_interval"):
                    code.original_interval = code.interval
            await self.save_config()

    async def _handle_permanent_error(self, chat_id: int):
        async with self._lock:
            for code in self.codes.values():
                code.chats.discard(chat_id)
                logger.warning(f"🚫 Ошибка в чате {chat_id}. Удален из всех рассылок.")
        await self.save_config()

    async def _handle_add_command(self, message: Message, code: Optional[Broadcast], code_name: str):
        async with self._lock:
            reply = await message.get_reply_message()
            if not reply:
                await utils.answer(message, "❌ Ответьте на сообщение для добавления")
                return
            try:
                is_new = code is None
                if is_new:
                    code = Broadcast()
                    self.codes[code_name] = code
                    
                grouped_ids = []
                if hasattr(reply, "grouped_id") and reply.grouped_id:
                    try:
                        reply_grouped_id = reply.grouped_id
                        album_messages = []
                        
                        # Get messages around the reply
                        messages = await self.client.get_messages(
                            reply.chat_id, 
                            limit=20,  # Увеличиваем лимит для надежности
                            max_id=reply.id + 10,
                            min_id=reply.id - 10
                        )
                        
                        # Filter messages with the same grouped_id
                        for msg in messages:
                            if msg and getattr(msg, "grouped_id", None) == reply_grouped_id:
                                album_messages.append(msg)
                                # Cache each album message
                                cache_key = (msg.chat_id, msg.id)
                                await self._message_cache.set(cache_key, msg)
                        
                        grouped_ids = sorted([msg.id for msg in album_messages])
                        logger.debug(f"Найдено сообщений в альбоме: {len(grouped_ids)}")
                        
                    except Exception as e:
                        logger.error(f"Ошибка получения альбома: {e}")
                        grouped_ids = []

                success = code.add_message(
                    chat_id=reply.chat_id,
                    message_id=reply.id,
                    grouped_ids=grouped_ids if grouped_ids else None
                )

                if not success:
                    await utils.answer(message, "❌ Сообщение уже существует")
                    return
                
                # Cache the main message
                cache_key = (reply.chat_id, reply.id)
                await self._message_cache.set(cache_key, reply)
                
                await self.save_config()

                if code_name in self.codes and len(self.codes[code_name].messages) > 0:
                    await utils.answer(
                        message,
                        f"✅ {'Создана рассылка' if is_new else 'Обновлена'} | "
                        f"Сообщений: {len(code.messages)}\n"
                        f"Групповых ID: {len(grouped_ids)}",
                    )
                else:
                    await utils.answer(message, "⚠️ Ошибка сохранения конфигурации!")

            except Exception as e:
                if is_new and code_name in self.codes:
                    del self.codes[code_name]
                await utils.answer(
                    message, f"🚨 Ошибка! Лог: {e.__class__.__name__}: {str(e)}"
                )

    async def _handle_addchat_command(
        self, message: Message, code: Broadcast, args: list
    ):
        """Обработчик команды addchat"""
        if len(args) > 2:
            chat_id = await self._get_chat_id(args[2])
            if not chat_id:
                await utils.answer(
                    message, "❌ Не удалось получить ID чата. Проверьте ссылку/юзернейм"
                )
                return
        else:
            chat_id = message.chat_id
        if len(code.chats) >= 500:
            await utils.answer(message, f"❌ Достигнут лимит чатов 500")
            return
        if chat_id in code.chats:
            await utils.answer(message, "❌ Этот чат уже добавлен в рассылку")
            return
        code.chats.add(chat_id)
        await self.save_config()
        await utils.answer(message, "✅ Чат добавлен в рассылку")

    async def _handle_allmsgs_command(
        self, message: Message, code: Broadcast, args: list
    ):
        """Обработчик команды allmsgs"""
        if len(args) < 3:
            await utils.answer(message, "❌ Укажите on или off")
            return
        mode = args[2].lower()
        if mode not in ["on", "off"]:
            await utils.answer(message, "❌ Укажите on или off")
            return
        code.batch_mode = mode == "on"
        await self.save_config()
        await utils.answer(
            message,
            f"✅ Отправка всех сообщений {'включена' if code.batch_mode else 'выключена'}",
        )

    async def _handle_delete_command(self, message: Message, code_name: str):
        """Обработчик команды delete"""
        task = self.broadcast_tasks.get(code_name)
        if task and not task.done():
            self.broadcast_tasks[code_name].cancel()
        del self.codes[code_name]
        await self.save_config()
        await utils.answer(message, f"✅ Рассылка {code_name} удалена")

    async def _handle_interval_command(
        self, message: Message, code: Broadcast, args: list
    ):
        """Обработчик команды int"""
        if len(args) < 4:
            await utils.answer(
                message, "❌ Укажите минимальный и максимальный интервал в минутах"
            )
            return
        try:
            min_val = int(args[2])
            max_val = int(args[3])
        except ValueError:
            await utils.answer(message, "❌ Интервалы должны быть числами")
            return
        code.interval = (min_val, max_val)
        if not code.is_valid_interval():
            await utils.answer(
                message, "❌ Некорректный интервал (0 < min < max <= 1440)"
            )
            return
        code.original_interval = code.interval
        await self.save_config()
        await utils.answer(message, f"✅ Установлен интервал {min_val}-{max_val} минут")

    async def _handle_list_command(self, message: Message):
        """Enhanced handler for list command with detailed stats"""
        if not self.codes:
            await utils.answer(message, "❌ Нет активных рассылок")
            return

        stats = []

        for name, code in self.codes.items():
            is_running = (
                name in self.broadcast_tasks
                and not self.broadcast_tasks[name].done()
            )

            total_messages = len(code.messages)
            grouped_messages = sum(
                1 for msg in code.messages if msg[2]
            )

            interval_modified = (
                code.interval != code.original_interval
                if hasattr(code, 'original_interval')
                else False
            )

            status_emoji = "✅" if code._active and is_running else "❌"
            pause_emoji = "⏸️" if self.pause_event.is_set() else ""
            flood_emoji = "🌊" if interval_modified else ""

            current_interval = f"{code.interval[0]}-{code.interval[1]}"
            original_interval = (
                f" (изн. {code.original_interval[0]}-{code.original_interval[1]})"
                if interval_modified
                else ""
            )

            stats.append(
                f"📊 {name}: {status_emoji}{pause_emoji}{flood_emoji}\n"
                f"├ Чатов: {len(code.chats)}\n"
                f"├ Сообщений: {total_messages} (альбомы: {grouped_messages})\n"
                f"├ Интервал: {current_interval} мин{original_interval}\n"
                f"└ Режим: {'все сразу' if code.batch_mode else 'поочерёдно'}"
            )

        global_status = []
        if self.pause_event.is_set():
            global_status.append("🚫 Глобальная пауза активна")
        if self.flood_wait_times:
            latest_flood = time.time() - self.last_flood_time
            if latest_flood < 43200:
                hours = latest_flood // 3600
                minutes = (latest_flood % 3600) // 60
                global_status.append(
                    f"⚠️ Последний флуд: {int(hours)}ч {int(minutes)}м назад"
                )

        response = (
            "📬 Состояние рассылок:\n"
            + ("\n" + "\n".join(global_status) + "\n" if global_status else "")
            + "\n"
            + "\n\n".join(stats)
        )

        await utils.answer(message, response)

        async def _handle_remove_command(self, message: Message, code: Broadcast):
            """Обработчик команды remove"""
            reply = await message.get_reply_message()
            if not reply:
                await utils.answer(message, "❌ Ответьте на сообщение для удаления")
                return
            grouped_id_for_removal = None
            if hasattr(reply, "grouped_id") and reply.grouped_id:
                grouped_id_for_removal = reply.grouped_id

            removed_album = code.remove_message(
                chat_id=reply.chat_id, message_id=reply.id, grouped_ids=grouped_id_for_removal
            )

            if removed_album:
                await self.save_config()
                await utils.answer(message, f"✅ {'Альбом' if grouped_id_for_removal else 'Сообщение'} удалено из рассылки")
            else:
                await utils.answer(message, "❌ Сообщение не найдено в рассылке")

    async def _handle_rmchat_command(
        self, message: Message, code: Broadcast, args: list
    ):
        """Обработчик команды rmchat"""
        if len(args) > 2:
            chat_id = await self._get_chat_id(args[2])
            if not chat_id:
                await utils.answer(
                    message, "❌ Не удалось получить ID чата. Проверьте ссылку/юзернейм"
                )
                return
        else:
            chat_id = message.chat_id
        if chat_id not in code.chats:
            await utils.answer(message, "❌ Этот чат не найден в рассылке")
            return
        code.chats.remove(chat_id)
        await self.save_config()
        await utils.answer(message, "✅ Чат удален из рассылки")

    async def _handle_start_command(
        self, message: Message, code: Broadcast, code_name: str
    ):
        """Обработчик команды start"""
        if not code.messages:
            await utils.answer(message, "❌ Добавьте хотя бы одно сообщение в рассылку")
            return
        if not code.chats:
            await utils.answer(message, "❌ Добавьте хотя бы один чат в рассылку")
            return
        if (
            code_name in self.broadcast_tasks
            and self.broadcast_tasks[code_name]
            and not self.broadcast_tasks[code_name].done()
        ):
            self.broadcast_tasks[code_name].cancel()
            try:
                await self.broadcast_tasks[code_name]
            except asyncio.CancelledError:
                pass
        code._active = True
        self.broadcast_tasks[code_name] = asyncio.create_task(
            self._broadcast_loop(code_name)
        )
        await self.save_config()
        await utils.answer(message, f"✅ Рассылка {code_name} запущена")

    async def _handle_stop_command(
        self, message: Message, code: Broadcast, code_name: str
    ):
        """Обработчик команды stop"""
        code._active = False
        if (
            code_name in self.broadcast_tasks
            and not self.broadcast_tasks[code_name].done()
        ):
            self.broadcast_tasks[code_name].cancel()
            try:
                await self.broadcast_tasks[code_name]
            except asyncio.CancelledError:
                pass
        await self.save_config()
        await utils.answer(message, f"✅ Рассылка {code_name} остановлена")

    async def _handle_watcher_command(self, message: Message, args: list):
        """Обработчик команды watcher"""
        if len(args) < 2:
            status = "включен" if self.watcher_enabled else "выключен"
            await utils.answer(
                message,
                "ℹ️ Автодобавление чатов\n"
                f"Текущий статус: {status}\n\n"
                "Использование: .br watcher on/off",
            )
            return
        mode = args[1].lower()
        if mode not in ["on", "off"]:
            await utils.answer(message, "❌ Укажите on или off")
            return
        self.watcher_enabled = mode == "on"
        await utils.answer(
            message,
            f"✅ Автодобавление чатов {'включено' if self.watcher_enabled else 'выключено'}",
        )

    async def _handle_failed_chats(
        self, code_name: str, failed_chats: Set[int]
    ) -> None:
        """Обрабатывает чаты, в которые не удалось отправить сообщения."""
        if not failed_chats:
            return
        try:
            async with self._lock:
                code = self.codes.get(code_name)
                if not code:
                    return
                code.chats -= failed_chats
                for chat_id in failed_chats:
                    await self.valid_chats_cache.set(chat_id, None)
                await self.save_config()

                chat_groups = [
                    ", ".join(
                        str(chat_id) for chat_id in tuple(failed_chats)[i : i + 30]
                    )
                    for i in range(0, len(failed_chats), 30)
                ]

                base_message = (
                    f"⚠️ Рассылка '{code_name}':\n"
                    f"Не удалось отправить сообщения в {len(failed_chats)} чат(ов).\n"
                    f"Чаты удалены из рассылки.\n\n"
                    f"ID чатов:\n"
                )

                for group in chat_groups:
                    await self.client.send_message(
                        self.tg_id,
                        base_message + group,
                        schedule=datetime.now() + timedelta(seconds=60),
                    )
                    await _internal.fw_protect()
        except Exception as e:
            logger.error(f"Ошибка обработки неудачных чатов для {code_name}: {e}")

    async def _is_chat_valid(self, chat_id: int) -> bool:
        if cached := await self.valid_chats_cache.get(chat_id):
            return cached
        try:
            await self.client.get_entity(chat_id)
            await self.valid_chats_cache.set(chat_id, True, expire=3600)
            return True
        except Exception:
            await self.valid_chats_cache.set(chat_id, False, expire=600)
            return False

    async def _load_config(self):
        try:
            config = self.db.get("broadcast", "config", {})
            if not config or "codes" not in config:
                return
            for code_name, code_data in config.get("codes", {}).items():
                original_interval = tuple(
                    code_data.get(
                        "original_interval", code_data.get("interval", (10, 13))
                    )
                )

                loaded_messages = []
                for msg_entry in code_data.get("messages", []):
                    grouped_ids = tuple(sorted(msg_entry.get("grouped_ids", [])))
                    loaded_messages.append(
                        (msg_entry["chat_id"], msg_entry["message_id"], grouped_ids)
                    )
                broadcast = Broadcast(
                    chats=set(code_data.get("chats", [])),
                    messages=set(loaded_messages),
                    interval=tuple(code_data.get("interval", (10, 13))),
                    batch_mode=code_data.get("batch_mode", False),
                    original_interval=original_interval,
                )
                broadcast._active = code_data.get("active", False)

                self.codes[code_name] = broadcast

                if broadcast._active:
                    await self._start_broadcast_task(code_name, broadcast)
        except Exception as e:
            logger.error(f"Ошибка загрузки: {e}", exc_info=True)

    async def _process_message_batch(self, messages: List[tuple]):
        """Обрабатывает батч сообщений для проверки их валидности"""
        valid_messages = []
        deleted_messages = []

        for msg_tuple in messages:
            try:
                if not isinstance(msg_tuple, tuple) or len(msg_tuple) != 3:
                    logger.error(f"Некорректный формат кортежа: {msg_tuple}")
                    deleted_messages.append(msg_tuple)
                    continue

                chat_id, message_id, grouped_ids = msg_tuple
                
                # Проверяем базовые типы данных
                if not isinstance(chat_id, int) or not isinstance(message_id, int):
                    logger.error(f"Некорректные типы ID: chat_id={chat_id}, message_id={message_id}")
                    deleted_messages.append(msg_tuple)
                    continue
                    
                # Проверяем grouped_ids
                if grouped_ids and not isinstance(grouped_ids, (tuple, list)):
                    logger.error(f"Некорректный формат grouped_ids: {grouped_ids}")
                    deleted_messages.append(msg_tuple)
                    continue

                # Проверяем наличие сообщения в кэше или возможность его получить
                msg = await self._fetch_messages({
                    "chat_id": chat_id,
                    "message_id": message_id
                })
                
                if msg is None:
                    logger.error(f"Сообщение недоступно: chat_id={chat_id}, message_id={message_id}")
                    deleted_messages.append(msg_tuple)
                    continue

                # Если есть grouped_ids, проверяем каждое сообщение альбома
                if grouped_ids:
                    all_msgs_valid = True
                    for album_msg_id in grouped_ids:
                        album_msg = await self._fetch_messages({
                            "chat_id": chat_id,
                            "message_id": album_msg_id
                        })
                        if album_msg is None:
                            logger.error(f"Сообщение альбома недоступно: chat_id={chat_id}, message_id={album_msg_id}")
                            all_msgs_valid = False
                            break
                    
                    if not all_msgs_valid:
                        deleted_messages.append(msg_tuple)
                        continue

                valid_messages.append(msg_tuple)

            except Exception as e:
                logger.error(f"Ошибка обработки сообщения {msg_tuple}: {str(e)}", exc_info=True)
                deleted_messages.append(msg_tuple)

        return valid_messages, deleted_messages

    async def _restart_all_broadcasts(self):
        async with self._lock:
            for code_name, code in self.codes.items():
                if code._active and code_name not in self.broadcast_tasks:
                    if self.broadcast_tasks.get(code_name):
                        self.broadcast_tasks[code_name].cancel()
                    self.broadcast_tasks[code_name] = asyncio.create_task(
                        self._broadcast_loop(code_name)
                    )

    async def _start_broadcast_task(self, code_name: str, code: Broadcast):
        if code_name in self.broadcast_tasks:
            task = self.broadcast_tasks[code_name]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        code._active = True
        self.broadcast_tasks[code_name] = asyncio.create_task(
            self._broadcast_loop(code_name)
        )
        logger.info(f"Задача для {code_name} перезапущена")

    async def _send_message(
        self,
        chat_id: int,
        msg: Union[Message, List[Message]],
        grouped_ids: Optional[List[int]] = None
    ) -> bool:
        if self.pause_event.is_set():
            return False
        await self.GLOBAL_LIMITER.acquire()
        try:
            async def forward_messages(messages: List[Message]) -> None:
                await self.client.forward_messages(
                    entity=chat_id,
                    messages=[m.id for m in messages],
                    from_peer=messages[0].chat_id,
                )

            await _internal.fw_protect()

            if isinstance(msg, list) or grouped_ids:
                if grouped_ids:
                    messages = []
                    for msg_id in grouped_ids:
                        msg = await self._fetch_messages({
                            "chat_id": msg[0],
                            "message_id": msg_id,
                        })
                        if msg:
                            messages.append(msg)
                    if messages:
                        await forward_messages(messages)
                else:
                    await forward_messages(msg)
            else:
                await forward_messages([msg])
            return True
        except FloodWaitError as e:
            logger.error(f"Флуд-контроль: {e}")
            await self._handle_flood_wait(e, chat_id)
            return False
        except (ChatWriteForbiddenError, UserBannedInChannelError) as e:
            logger.error(f"Доступ запрещен: {chat_id}")
            await self._handle_permanent_error(chat_id)
            return False
        except Exception as e:
            logger.error(f"🛑 Ошибка в {chat_id}: {repr(e)}")
            await self._handle_permanent_error(chat_id)
            return False

    async def _send_messages_to_chats(
        self, code: Broadcast, messages: Iterable[Message], code_name: str
    ) -> Set[int]:
        """Улучшенная отправка с обработкой альбомов"""
        if self.pause_event.is_set():
            return set()

        all_messages_to_send = []
        
        # Fix: Iterate over message tuples instead of Message objects
        for msg_tuple in code.messages:  # Use original message tuples from Broadcast
            chat_id, main_msg_id, grouped_ids = msg_tuple

            # Rest of the processing remains the same
            all_msg_ids = [main_msg_id]
            if grouped_ids:
                all_msg_ids.extend(grouped_ids)
            
            album_messages = []
            for msg_id in all_msg_ids:
                msg = await self._fetch_messages({
                    "chat_id": chat_id,
                    "message_id": msg_id
                })
                if msg:
                    album_messages.append(msg)
            
            if album_messages:
                all_messages_to_send.append({
                    "chat_id": chat_id,
                    "messages": album_messages,
                    "grouped_ids": grouped_ids if grouped_ids else None
                })

        # Rest of the method remains unchanged
        valid_chats = [cid for cid in code.chats if await self._is_chat_valid(cid)]
        if not valid_chats:
            logger.error("💥 Нет доступных чатов для отправки!")
            code._active = False
            return set()

        failed_chats = set()

        for chat_id in valid_chats:
            success = True
            for album in all_messages_to_send:
                result = await self._send_message(
                    chat_id=chat_id,
                    msg=album["messages"],
                    grouped_ids=album["grouped_ids"]
                )
                
                if not result:
                    success = False
                    break
            
            if not success:
                failed_chats.add(chat_id)

        if failed_chats:
            code.chats -= failed_chats
            await self.save_config()

        return failed_chats

    async def _send_batch(
        self, chat_ids: List[int], messages: Iterable[Message]
    ) -> List[bool]:
        """Отправка батча с ограничением параллелизма"""
        sem = asyncio.Semaphore(10)

        async def send_one(chat_id: int) -> bool:
            async with sem:
                try:
                    return await self._send_message(chat_id, messages)
                except asyncio.CancelledError:
                    logger.warning(f"Задача для чата {chat_id} была отменена")
                    raise
                except Exception as e:
                    logger.error(f"Ошибка в чате {chat_id}: {str(e)}")
                    return False

        return await asyncio.gather(*[send_one(cid) for cid in chat_ids])

    async def handle_command(self, message: Message):
        """Обработчик команд для управления рассылкой"""
        args = message.text.split()[1:]
        if not args:
            await utils.answer(message, "❌ Укажите действие и код рассылки")
            return
        action = args[0].lower()
        code_name = args[1] if len(args) > 1 else None

        if action == "list":
            await self._handle_list_command(message)
            return
        elif action == "watcher":
            await self._handle_watcher_command(message, args)
            return
        elif action == "pause":
            self.pause_event.set()
            await utils.answer(message, "✅ Глобальная пауза активирована")
            return
        elif action == "resume":
            self.pause_event.clear()
            await self._restart_all_broadcasts()
            await utils.answer(message, "✅ Рассылки возобновлены")
            return
        if not code_name:
            await utils.answer(message, "❌ Укажите код рассылки")
            return
        code = self.codes.get(code_name)
        if action != "add" and not code:
            await utils.answer(message, f"❌ Код рассылки {code_name} не найден")
            return
        command_handlers = {
            "add": lambda: self._handle_add_command(message, code, code_name),
            "del": lambda: self._handle_delete_command(message, code_name),
            "rm": lambda: self._handle_remove_command(message, code),
            "addchat": lambda: self._handle_addchat_command(message, code, args),
            "rmchat": lambda: self._handle_rmchat_command(message, code, args),
            "int": lambda: self._handle_interval_command(message, code, args),
            "allmsgs": lambda: self._handle_allmsgs_command(message, code, args),
            "start": lambda: self._handle_start_command(message, code, code_name),
            "stop": lambda: self._handle_stop_command(message, code, code_name),
        }

        handler = command_handlers.get(action)
        if handler:
            await handler()
        else:
            await utils.answer(message, "❌ Неизвестное действие")

    async def save_config(self):
        try:
            config = {
                "codes": {},
                "version": 3,
                "timestamp": datetime.now().timestamp(),
            }

            for name, code in self.codes.items():
                messages = []
                for msg in code.messages:
                    serialized_group = list(msg[2]) if msg[2] else []
                    
                    messages.append({
                        "chat_id": msg[0],
                        "message_id": msg[1],
                        "grouped_ids": serialized_group,
                    })
                config["codes"][name] = {
                    "chats": list(code.chats),
                    "messages": messages,
                    "interval": list(code.interval),
                    "batch_mode": code.batch_mode,
                    "active": code._active,
                    "original_interval": list(code.original_interval),
                }
            self.db.set("broadcast", "config", config)
        except Exception as e:
            logger.error(f"Ошибка сохранения: {e}", exc_info=True)

    async def start_adaptive_interval_adjustment(self):
        """Фоновая задача для адаптации интервалов"""
        while self._active:
            try:
                await asyncio.sleep(1800)
                await self._check_and_adjust_intervals()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в адаптивной регулировке: {e}", exc_info=True)

    async def start_cache_cleanup(self):
        """Запускает фоновую очистку кэша"""
        self.cache_cleanup_task = asyncio.create_task(
            self._message_cache.start_auto_cleanup()
        )
