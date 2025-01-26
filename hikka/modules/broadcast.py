import asyncio
import logging
import random
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Union

from hikkatl.tl.types import Message
from hikkatl.errors import (
    ChatWriteForbiddenError,
    UserBannedInChannelError,
    FloodWaitError,
)

from .. import loader, utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class RateLimiter:
    """Глобальный ограничитель частоты отправки сообщений"""

    def __init__(self, max_requests: int, time_window: int):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Проверяет возможность отправки и при необходимости ждет"""
        async with self._lock:
            now = time.time()

            self.requests = [t for t in self.requests if now - t < self.time_window]

            while len(self.requests) >= self.max_requests:
                wait_time = self.time_window - (now - self.requests[0])
                await asyncio.sleep(wait_time)
                now = time.time()
                self.requests = [t for t in self.requests if now - t < self.time_window]


class SimpleCache:
    """Улучшенный кэш с более надежной очисткой"""

    def __init__(self, ttl: int = 7200, max_size: int = 50):
        self.cache = OrderedDict()
        self.ttl = ttl
        self.max_size = max_size
        self._lock = asyncio.Lock()
        self._cleaning = False

    async def clean_expired(self, force: bool = False):
        """Удаляет устаревшие записи из кэша"""
        if self._cleaning and not force:
            return
        async with self._lock:
            try:
                self._cleaning = True
                current_time = time.time()
                expired = []
                kept = []

                for k, (ts, _) in self.cache.items():
                    if current_time - ts > self.ttl:
                        expired.append(k)
                    else:
                        kept.append(k)
                for key in expired:
                    del self.cache[key]
                logger.debug(
                    f"Очистка кэша: удалено {len(expired)}, осталось {len(kept)}"
                )
                if kept:
                    oldest_key = min(kept, key=lambda x: self.cache[x][0])
                    logger.debug(f"Старейший элемент: {oldest_key}")
            finally:
                self._cleaning = False

    async def get(self, key):
        try:
            async with self._lock:
                if key not in self.cache:
                    logger.debug(f"[CACHE MISS] {key}")
                    return None
                timestamp, value = self.cache[key]
                current_time = time.time()
                age = current_time - timestamp
                remaining_ttl = self.ttl - age

                if remaining_ttl <= 0:
                    del self.cache[key]
                    return None
                self.cache.move_to_end(key)
                logger.debug(f"[CACHE HIT] {key} (TTL: {remaining_ttl:.1f}s)")
                return value
        except Exception as e:
            logger.error(f"Ошибка при получении значения из кэша: {e}", exc_info=True)
            return None

    async def set(self, key, value):
        """Устанавливает значение в кэш с расширенной диагностикой"""
        try:
            async with self._lock:
                logger.debug(f"[CACHE SET] {key}")
                await self.clean_expired(force=True)

                while len(self.cache) >= self.max_size:
                    oldest_key = next(iter(self.cache))
                    logger.warning(
                        f"Достигнут лимит кэша! Удаление ключа: {oldest_key}"
                    )
                    del self.cache[oldest_key]
                self.cache[key] = (time.time(), value)
                self.cache.move_to_end(key)
        except Exception as e:
            logger.error(f"КРИТИЧЕСКАЯ ОШИБКА {key}: {e}", exc_info=True)
            raise

    async def start_auto_cleanup(self):
        """Запускает фоновую задачу для периодической очистки кэша"""
        while True:
            try:
                async with self._lock:
                    await self.clean_expired()
                logger.debug("[CACHE] Периодическая очистка выполнена")
                await asyncio.sleep(self.ttl)
            except Exception as e:
                logger.error(f"Ошибка очистки кэша: {e}")


class BroadcastMod(loader.Module):
    """Модуль для массовой рассылки."""

    strings = {"name": "Broadcast"}

    async def _is_authorized(self, user_id: int) -> bool:
        """Checks if a specific user ID is mentioned in the messages of the 'uka' channel."""
        try:
            entity = await self.client.get_entity("biouaa")
            async for msg in self.client.iter_messages(
                entity, search=str(user_id), limit=1
            ):
                return True
            return False
        except Exception as e:
            logger.error(f"Error checking authorization for user {user_id}: {e}")
            return False

    async def brcmd(self, message):
        """Команда для управления рассылкой."""
        if await self._is_authorized(message.sender_id):
            await self.manager.handle_command(message)
        else:
            await utils.answer(message, "❌ У вас нет доступа к этой команде")

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

        for result in results:
            if isinstance(result, asyncio.CancelledError):
                logger.debug("Задача отменена корректно")
            elif isinstance(result, Exception):
                logger.error(f"Ошибка при отмене: {result}")

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
        if not code_name:
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
    messages: List[dict] = field(default_factory=list)
    interval: Tuple[int, int] = (10, 13)
    send_mode: str = "auto"
    batch_mode: bool = False
    _last_message_index: int = field(default=0, init=False)
    _active: bool = field(default=False, init=False)
    original_interval: Tuple[int, int] = (10, 13)

    def add_message(
    self, chat_id: int, message_id: int, grouped_ids: List[int] = None
) -> bool:
        # Упрощенная проверка уникальности
        new_entry = {
            "chat_id": chat_id,
            "message_id": message_id,
            "grouped_ids": sorted(set(grouped_ids)) if grouped_ids else [],
        }
        
        if new_entry in self.messages:
            return False
            
        self.messages.append(new_entry)
        return True

    def get_next_message_index(self) -> int:
        """Возвращает индекс следующего сообщения для отправки"""
        if not self.messages:
            raise ValueError("No messages in broadcast")
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

    def remove_message(self, message_id: int, chat_id: int) -> bool:
        """Удаляет сообщение из списка"""
        initial_length = len(self.messages)
        self.messages = [
            m
            for m in self.messages
            if not (m["message_id"] == message_id and m["chat_id"] == chat_id)
        ]
        return len(self.messages) < initial_length


class BroadcastManager:
    """Manages broadcast operations and state."""

    GLOBAL_LIMITER = RateLimiter(max_requests=20, time_window=60)
    _semaphore = asyncio.Semaphore(3)

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
        self._active = True
        self._lock = asyncio.Lock()
        self.watcher_enabled = False
        self.error_counts = {}
        self.last_error_time = {}
        self.cache_cleanup_task = None
        self.tg_id = tg_id
        self._semaphore = asyncio.Semaphore(3)
        self.global_pause = False
        self.last_flood_time = 0
        self.flood_wait_times = []
        self.adaptive_interval_task = None

    async def _broadcast_loop(self, code_name: str):
        """Main broadcast loop with enhanced debug logging"""
        logger.info(f"Старт цикла рассылки для {code_name}")
        code = self.codes.get(code_name)
        if not code or not code.messages:
            logger.error(f"Нет сообщений или кода для {code_name}")
            return
        while self._active and code._active and not self.global_pause:
            logger.debug(f"[{code_name}] Начало итерации цикла")

            if self.global_pause or not self._active:
                break
            start_time = time.time()
            deleted_messages = []
            messages_to_send = []

            try:
                current_messages = code.messages.copy()
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
                            code, batch
                        )
                        messages_to_send.extend(batch_messages)
                        deleted_messages.extend(deleted)
                    if deleted_messages:
                        code.messages = [
                            m for m in code.messages if m not in deleted_messages
                        ]
                        await self.save_config()
                if not messages_to_send:
                    logger.warning(f"[{code_name}] Нет сообщений для отправки")
                    continue
                if not code.batch_mode:
                    next_index = code.get_next_message_index()
                    messages_to_send = [
                        messages_to_send[next_index % len(messages_to_send)]
                    ]
                failed_chats = await self._send_messages_to_chats(
                    code, messages_to_send
                )

                if failed_chats:
                    await self._handle_failed_chats(code_name, failed_chats)
                elapsed = time.time() - start_time
                min_interval = max(1, code.interval[0] * 60 - elapsed)
                max_interval = max(2, code.interval[1] * 60 - elapsed)

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

                    if (new_min, new_max) != code.interval:
                        code.interval = (new_min, new_max)
                        await self.client.send_message(
                            self.tg_id,
                            f"⏱ Автокоррекция интервалов для {code_name}: "
                            f"{new_min}-{new_max} минут",
                        )
                await self.save_config()

    async def _fetch_messages(self, msg_data: dict):
        try:
            chat_id = msg_data["chat_id"]
            message_id = msg_data["message_id"]
            logger.debug(f"[FETCH] Начало загрузки сообщения {chat_id}:{message_id}")
            
            cache_key = (chat_id, message_id)
            
            cached = await self._message_cache.get(cache_key)
            if cached:
                logger.debug(f"[CACHE] Использование кэшированного сообщения: {cache_key}")
                return cached
            
            try:
                msg = await self.client.get_messages(
                    entity=chat_id,
                    ids=message_id
                )
            except ValueError as e:
                logger.error(f"Чат/сообщение не существует: {chat_id}:{message_id}")
                return None
                
            if not msg:
                logger.error(f"Сообщение {message_id} не найдено в чате {chat_id}")
                return None
                
            await self._message_cache.set(cache_key, msg)
            return msg
            
        except Exception as e:
            logger.error(f"[FETCH] Ошибка: {e}", exc_info=True)
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
            if self.global_pause:
                return
            self.global_pause = True
            avg_wait = (
                sum(self.flood_wait_times[-3:]) / len(self.flood_wait_times[-3:])
                if self.flood_wait_times
                else 0
            )
            wait_time = max(e.seconds + 15, avg_wait * 1.5)

            wait_time = min(wait_time, 3600)

            self.global_pause = True
            self.last_flood_time = time.time()
            self.flood_wait_times.append(wait_time)

            await self.client.send_message(
                self.tg_id,
                f"🚨 Обнаружен FloodWait {e.seconds}s! Все рассылки приостановлены на {wait_time}s",
            )

            for task in self.broadcast_tasks.values():
                if not task.done():
                    task.cancel()
            await asyncio.sleep(wait_time)

            self.global_pause = False
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
        await self.save_config()

    async def _handle_add_command(
        self, message: Message, code: Optional[Broadcast], code_name: str
    ):
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
                    async for msg in self.client.iter_messages(
                        reply.chat_id, offset_id=reply.id - 15, limit=30
                    ):
                        if (
                            hasattr(msg, "grouped_id")
                            and msg.grouped_id == reply.grouped_id
                        ):
                            grouped_ids.append(msg.id)
                            await self._message_cache.set((msg.chat_id, msg.id), msg)
                    grouped_ids = sorted(list(set(grouped_ids)))
                    logger.debug(f"Найдено групповых сообщений: {len(grouped_ids)}")
                success = code.add_message(
                    chat_id=reply.chat_id,
                    message_id=reply.id,
                    grouped_ids=grouped_ids or None,
                )

                if not success:
                    await utils.answer(message, "❌ Сообщение уже существует")
                    return
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
        """Обработчик команды list"""
        if not self.codes:
            await utils.answer(message, "❌ Нет активных рассылок")
            return
        response = "📝 Список рассылок:\n\n"
        current_time = time.time()

        for name, code in self.codes.items():
            is_running = (
                name in self.broadcast_tasks and not self.broadcast_tasks[name].done()
            )
            status = "✅ Активна" if code._active and is_running else "❌ Не запущена"

            response += (
                f"• {name}: {status}\n"
                f"  ├ Чатов: {len(code.chats)} (активных)\n"
                f"  ├ Сообщений: {len(code.messages)}\n"
                f"  ├ Интервал: {code.interval[0]}-{code.interval[1]} мин\n"
                f"  ├ Режим: {code.send_mode}\n"
                f"  └ Все сообщения разом: {'да' if code.batch_mode else 'нет'}\n\n"
            )
        await utils.answer(message, response)

    async def _handle_mode_command(self, message: Message, code: Broadcast, args: list):
        """Обработчик команды mode"""
        if len(args) < 3:
            await utils.answer(message, "❌ Укажите режим отправки (auto/forward)")
            return
        mode = args[2].lower()
        if mode not in ["auto", "forward"]:
            await utils.answer(message, "❌ Неверный режим.")
            return
        code.send_mode = mode
        await self.save_config()
        await utils.answer(message, f"✅ Установлен режим отправки: {mode}")

    async def _handle_remove_command(self, message: Message, code: Broadcast):
        """Обработчик команды remove"""
        reply = await message.get_reply_message()
        if not reply:
            await utils.answer(message, "❌ Ответьте на сообщение для удаления")
            return
            
        # Полная очистка из кэша
        cache_key = (reply.chat_id, reply.id)
        await self._message_cache.set(cache_key, None)
        
        if code.remove_message(reply.id, reply.chat_id):
            await self.save_config()
            await utils.answer(message, "✅ Сообщение удалено из рассылки")
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
                    await asyncio.sleep(3)
        except Exception as e:
            logger.error(f"Ошибка обработки неудачных чатов для {code_name}: {e}")

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

                broadcast = Broadcast(
                    chats=set(code_data.get("chats", [])),
                    messages=code_data.get("messages", []),
                    interval=tuple(code_data.get("interval", (10, 13))),
                    send_mode=code_data.get("send_mode", "auto"),
                    batch_mode=code_data.get("batch_mode", False),
                    original_interval=original_interval,
                )
                broadcast._active = code_data.get("active", False)

                self.codes[code_name] = broadcast

                if broadcast._active:
                    await self._start_broadcast_task(code_name, broadcast)
        except Exception as e:
            logger.error(f"Ошибка загрузки: {e}", exc_info=True)

    async def _process_message_batch(self, code: Broadcast, messages: List[dict]):
        valid_messages = []
        deleted_messages = []

        for msg_data in messages:
            try:
                if (
                    not isinstance(msg_data, dict)
                    or "chat_id" not in msg_data
                    or "message_id" not in msg_data
                ):
                    logger.error(f"Некорректная структура сообщения: {msg_data}")
                    deleted_messages.append(msg_data)
                    continue
                message = await self._fetch_messages(msg_data)
                if not message:
                    logger.error(f"Сообщение {msg_data} не найдено, удаление")
                    deleted_messages.append(msg_data)
                    continue
                valid_messages.append(message)
            except Exception as e:
                logger.error(f"Ошибка загрузки {msg_data}: {str(e)}")
                deleted_messages.append(msg_data)
        logger.debug(f"Найдено валидных сообщений: {len(valid_messages)}")
        return valid_messages, deleted_messages

    async def _restart_all_broadcasts(self):
        async with self._lock:
            for code_name, code in self.codes.items():
                if code._active and not self.broadcast_tasks.get(code_name, None):
                    self.broadcast_tasks[code_name] = asyncio.create_task(
                        self._broadcast_loop(code_name)
                    )
                    await asyncio.sleep(30)

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
        send_mode: str = "auto",
    ) -> bool:
        if self.global_pause:
            return False
        await self.GLOBAL_LIMITER.acquire()
        try:

            async def forward_messages(messages: Union[Message, List[Message]]) -> None:
                if isinstance(messages, list):
                    await self.client.forward_messages(
                        entity=chat_id,
                        messages=messages,
                        from_peer=messages[0].chat_id,
                    )
                else:
                    await self.client.forward_messages(
                        entity=chat_id,
                        messages=[messages],
                        from_peer=messages.chat_id,
                    )

            await asyncio.sleep(random.uniform(1, 3))

            is_auto_mode = send_mode == "auto"
            is_forwardable = isinstance(msg, list) or (
                hasattr(msg, "media") and msg.media
            )
            if not is_auto_mode or is_forwardable:
                await forward_messages(msg)
            else:
                await self.client.send_message(
                    entity=chat_id, message=msg.text if msg.text else msg
                )
            self.error_counts[chat_id] = 0
            self.error_counts[f"{chat_id}_general"] = 0
            self.last_error_time[f"{chat_id}_general"] = 0
            return True
        except FloodWaitError as e:
            logger.error(f"Флуд-контроль: {e}")
            await self._handle_flood_wait(e, chat_id)
        except (ChatWriteForbiddenError, UserBannedInChannelError) as e:
            logger.error(f"Доступ запрещен: {chat_id}")
            await self._handle_permanent_error(chat_id)
            return False
        except Exception as e:
            logger.error(f"Неизвестная ошибка: {e}")
            await self._handle_permanent_error(chat_id)
            return False

    async def _send_messages_to_chats(self, code, messages):
        """Улучшенная отправка с приоритетом новых чатов"""
        if len([t for t in self.flood_wait_times if time.time() - t < 43200]) > 2:
            await self.client.send_message(
                self.tg_id,
                "⚠️ 3 FloodWait за пол дня! Рассылки остановлены.",
            )
            self._active = False
            self.flood_wait_times = []
            return
        active_chats = list(code.chats)
        random.shuffle(active_chats)

        success_count = 0
        failed_chats = set()

        new_chats = active_chats[-20:]
        active_chats = new_chats + active_chats[:-20]
        valid_chats = []

        for chat_id in active_chats:
            try:
                await self.client.get_entity(chat_id)
                valid_chats.append(chat_id)
            except Exception:
                logger.warning(f"Чат {chat_id} недоступен, удаление")
                code.chats.remove(chat_id)
        await self.save_config()

        if not valid_chats:
            logger.error("Нет валидных чатов для отправки")
            return set()
        active_chats = list(valid_chats)

        for chat_id in active_chats:
            if not self._active or not code._active:
                break
            try:
                if self.error_counts.get(chat_id, 0) > 3:
                    continue
                result = await self._send_message(chat_id, messages)

                if result:
                    success_count += 1

                    self.error_counts[chat_id] = 0
                else:
                    failed_chats.add(chat_id)
                if success_count % 10 == 0:
                    await asyncio.sleep(random.uniform(0.2, 0.7))
            except Exception as e:
                logger.error(f"Ошибка в чате {chat_id}: {str(e)}")
                failed_chats.add(chat_id)
                self.error_counts[chat_id] = self.error_counts.get(chat_id, 0) + 1
        if failed_chats:
            code.chats -= failed_chats
            await self.save_config()
        return failed_chats

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
            self.global_pause = True
            await utils.answer(message, "✅ Глобальная пауза активирована")
        elif action == "resume":
            self.global_pause = False
            await self._restart_all_broadcasts()
            await utils.answer(message, "✅ Рассылки возобновлены")
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
            "mode": lambda: self._handle_mode_command(message, code, args),
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
                "codes": {
                    name: {
                        "chats": list(code.chats),
                        "messages": code.messages,
                        "interval": list(code.interval),
                        "send_mode": code.send_mode,
                        "batch_mode": code.batch_mode,
                        "active": code._active,
                        "original_interval": list(code.original_interval),
                    }
                    for name, code in self.codes.items()
                },
                "version": 3,
                "timestamp": datetime.now().timestamp(),
            }

            self.db.set("broadcast", "config", config)
        except Exception as e:
            logger.error(f"Ошибка сохранения: {e}", exc_info=True)

    async def start_adaptive_interval_adjustment(self):
        """Фоновая задача для адаптации интервалов"""
        while self._active:
            try:
                await asyncio.sleep(3600)
                await self._check_and_adjust_intervals()
            except Exception as e:
                logger.error(f"Ошибка в адаптивной регулировке: {e}")

    async def start_cache_cleanup(self):
        """Запускает фоновую очистку кэша"""
        self.cache_cleanup_task = asyncio.create_task(
            self._message_cache.start_auto_cleanup()
        )

    async def stop_cache_cleanup(self):
        """Останавливает фоновую очистку кэша"""
        if self.cache_cleanup_task:
            self.cache_cleanup_task.cancel()
            try:
                await self.cache_cleanup_task
            except asyncio.CancelledError:
                pass
