import asyncio
import logging
import random
import sys
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

            # Добавить таймаут для избежания вечной блокировки

            while len(self.requests) >= self.max_requests:
                wait_time = self.time_window - (now - self.requests[0])
                await asyncio.sleep(wait_time)
                now = time.time()  # Обновить время после сна
                self.requests = [t for t in self.requests if now - t < self.time_window]

    async def get_stats(self) -> dict:
        """Возвращает текущую статистику использования"""
        async with self._lock:
            now = time.time()
            active_requests = [t for t in self.requests if now - t < self.time_window]
            return round(len(active_requests) / self.max_requests * 100, 1)


class SimpleCache:
    """Улучшенный кэш с более надежной очисткой"""

    def __init__(self, ttl: int = 7200, max_size: int = 50):
        self.cache = OrderedDict()
        self.ttl = ttl
        self.max_size = max_size
        self._lock = asyncio.Lock()
        self._last_cleanup = time.time()
        self._cleaning = False
        logger.debug(f"Инициализирован кэш | TTL: {ttl}s | Макс. размер: {max_size}")

    def _estimate_memory_usage(self):
        """Примерная оценка использования памяти"""
        try:
            return (
                sum(
                    sys.getsizeof(k) + sys.getsizeof(v[0]) + sys.getsizeof(v[1])
                    for k, v in self.cache.items()
                )
                // 1024
            )
        except:
            return "N/A"

    def _cache_state_report(self):
        """Генерирует подробный отчет о состоянии кэша"""
        if not self.cache:
            return "Кэш пуст"
        current_time = time.time()
        return "\n".join(
            [
                f"Ключ: {k} | Возраст: {current_time - v[0]:.1f}s | Тип: {type(v[1]).__name__}"
                for k, v in self.cache.items()
            ]
        )

    async def clean_expired(self, force: bool = False):
        """Удаляет устаревшие записи из кэша"""
        try:
            if self._cleaning and not force:
                return
            self._cleaning = True
            current_time = time.time()
            expired_keys = [
                k for k, (ts, _) in self.cache.items()
                if current_time - ts > self.ttl
            ]
            for key in expired_keys:
                del self.cache[key]
            logger.debug(f"Удалено устаревших записей: {len(expired_keys)}")
        except Exception as e:
            logger.error(f"Ошибка очистки кэша: {e}", exc_info=True)
        finally:
            self._cleaning = False

    async def get(self, key):
        logger.debug(f"[CACHE GET] Попытка получения ключа {key}")
        try:
            async with self._lock:
                if key not in self.cache:
                    logger.debug(f"[CACHE] Промах кэша для ключа {key}")
                    return None
                timestamp, value = self.cache[key]
                current_time = time.time()
                age = current_time - timestamp
                remaining_ttl = self.ttl - age

                if remaining_ttl <= 0:
                    logger.debug(
                        f"[CACHE] Устаревшая запись {key} (возраст {age:.1f} сек)"
                    )
                    del self.cache[key]
                    return None
                logger.debug(
                    f"[CACHE] Попадание кэша для {key}. "
                    f"Остаток TTL: {remaining_ttl:.1f} сек"
                )
                self.cache.move_to_end(key)
                return value
        except Exception as e:
            logger.error(f"Ошибка при получении значения из кэша: {e}", exc_info=True)
            return None

    async def set(self, key, value):
        """Устанавливает значение в кэш с расширенной диагностикой"""
        logger.debug(f"[CACHE SET] Начало операции для ключа {key}")
        try:
            async with self._lock:
                logger.debug(f"Блокировка захвачена для ключа {key}")

                logger.debug(
                    f"Состояние кэша ДО операции: {self._cache_state_report()}"
                )

                await self.clean_expired(force=True)

                if key in self.cache:
                    logger.debug(f"Обновление существующего ключа: {key}")
                while len(self.cache) >= self.max_size:
                    oldest_key = next(iter(self.cache))
                    logger.warning(
                        f"Достигнут лимит кэша! Удаление ключа: {oldest_key}"
                    )
                    del self.cache[oldest_key]
                self.cache[key] = (time.time(), value)
                self.cache.move_to_end(key)

                logger.debug(
                    f"Успешно добавлен ключ: {key}\n"
                    f"Тип значения: {type(value)}\n"
                    f"Размер кэша: {len(self.cache)}\n"
                    f"Примерный размер памяти: {self._estimate_memory_usage()}"
                )

                logger.debug(
                    f"Промежуточное состояние кэша: {self._cache_state_report()}"
                )
        except Exception as e:
            logger.error(
                f"КРИТИЧЕСКАЯ ОШИБКА при установке ключа {key}: {e}", exc_info=True
            )
            raise
        finally:
            logger.debug(f"Блокировка отпущена для ключа {key}")
            logger.debug(f"Состояние кэша ПОСЛЕ операции: {self._cache_state_report()}")

    async def start_auto_cleanup(self):
        """Запускает фоновую задачу для периодической очистки кэша"""
        logger.debug(f"[CACHE] Запуск фоновой очистки с интервалом {self.ttl} сек")
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
            logger.error(
                f"Error checking authorization for user {user_id} in {msg}: {e}"
            )
            return False

    async def brcmd(self, message):
        """Команда для управления рассылкой."""
        if await self._is_authorized(message.sender_id):
            await self.manager.handle_command(message)
        else:
            await utils.answer(message, "❌ У вас нет доступа к этой команде")

    async def client_ready(self):
        """Initialization sequence"""
        self.manager = BroadcastManager(self._client, self.db, self.tg_id)
        try:
            await asyncio.wait_for(self.manager._load_config(), timeout=30)
            await self.manager.start_cache_cleanup()
            self._initialized = True
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            self._initialized = False

    async def on_unload(self):
        "На выходе"
        await self.manager.stop_cache_cleanup()
        for task in self.manager.broadcast_tasks.values():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        await self.manager._semaphore.acquire()
        self.manager._semaphore.release()

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

    def add_message(
        self, chat_id: int, message_id: int, grouped_ids: List[int] = None
    ) -> bool:
        grouped_ids = sorted(list(set(grouped_ids))) if grouped_ids else []

        new_message = {
            "chat_id": chat_id,
            "message_id": message_id,
            "grouped_ids": grouped_ids,
        }

        # Проверка на абсолютные дубликаты

        for existing in self.messages:
            if (
                existing["chat_id"] == new_message["chat_id"]
                and existing["message_id"] == new_message["message_id"]
                and sorted(existing["grouped_ids"]) == new_message["grouped_ids"]
            ):
                return False
        self.messages.append(new_message)
        return True

    @classmethod
    def from_dict(cls, data: dict) -> "Broadcast":
        instance = cls(
            chats=set(data.get("chats", [])),
            messages=[dict(msg) for msg in data.get("messages", [])],
            interval=tuple(data["interval"]) if "interval" in data else (10, 13),
            send_mode=data.get("send_mode", "auto"),
            batch_mode=data.get("batch_mode", False),
        )
        instance._active = data.get(
            "active", False
        )  # Восстанавливаем состояние активности
        return instance

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

    def to_dict(self) -> dict:
        """Сериализует объект в словарь"""
        result = {
            "chats": list(self.chats),
            "messages": self.messages,
            "interval": list(self.interval),
            "send_mode": self.send_mode,
            "batch_mode": self.batch_mode,
            "active": self._active,
        }

        # Явная проверка и логирование

        logger.debug(f"Сериализация словаря: {result}")

        return result


class BroadcastManager:
    """Manages broadcast operations and state."""

    BATCH_SIZE_SMALL = 5
    BATCH_SIZE_MEDIUM = 8
    BATCH_SIZE_LARGE = 10
    MAX_MESSAGES_PER_CODE = 50
    MAX_CONSECUTIVE_ERRORS = 7
    BATCH_THRESHOLD_SMALL = 20
    BATCH_THRESHOLD_MEDIUM = 50
    NOTIFY_DELAY = 1
    NOTIFY_GROUP_SIZE = 30
    PERMISSION_CHECK_INTERVAL = 1800
    GLOBAL_MINUTE_LIMITER = RateLimiter(30, 60)
    GLOBAL_HOUR_LIMITER = RateLimiter(500, 3600)
    MAX_PERMISSION_RETRIES = 3
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

    async def _broadcast_loop(self, code_name: str):
        """Main broadcast loop with enhanced debug logging"""
        async with self._semaphore:
            code = self.codes.get(code_name)
            if not code or not code.messages:
                return
            while self._active:

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
                        code, code_name, messages_to_send
                    )

                    if failed_chats:
                        await self._handle_failed_chats(code_name, failed_chats)
                    elapsed = time.time() - start_time
                    min_interval = max(
                        1, code.interval[0] * 60 - elapsed
                    )  # Минимум 1 секунда
                    max_interval = max(
                        2, code.interval[1] * 60 - elapsed
                    )  # Минимум 2 секунды

                    await asyncio.sleep(random.uniform(min_interval, max_interval))
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(
                        f"[{code_name}] Error in broadcast loop: {str(e)}",
                        exc_info=True,
                    )
                    await asyncio.sleep(30)

    async def _calculate_and_sleep(self, min_interval: int, max_interval: int):
        """Вычисляет время сна и засыпает."""
        sleep_time = random.uniform(min_interval * 60, max_interval * 60)
        await asyncio.sleep(sleep_time - 15)

    async def _fetch_messages(self, msg_data: dict):
        """Получает сообщения с улучшенной обработкой ошибок"""
        try:
            key = (msg_data["chat_id"], msg_data["message_id"])
            logger.debug(f"[CACHE CHECK] Проверка ключа {key}")

            # Проверка основного сообщения

            cached = await self._message_cache.get(key)
            if cached:
                logger.debug(f"[CACHE HIT] Найдено в кэше: {key}")
                return cached
            logger.debug(f"[CACHE MISS] Отсутствует в кэше: {key}")
            message = await self.client.get_messages(
                msg_data["chat_id"], ids=msg_data["message_id"]
            )

            if not message:
                logger.error(f"Сообщение {msg_data} не найдено")
                return None
            # Сохраняем основное сообщение

            await self._message_cache.set(key, message)
            logger.debug(f"[CACHE SET] Сохранено сообщение {key}")

            # Обработка групповых сообщений

            if msg_data.get("grouped_ids"):
                group_key = (
                    msg_data["chat_id"],
                    tuple(sorted(msg_data["grouped_ids"])),
                )

                # 1. Проверяем наличие всех сообщений группы

                has_all_messages = True
                for msg_id in msg_data["grouped_ids"]:
                    if not await self._message_cache.get(...):
                        has_all_messages = False
                        break  # Добавить прерывание цикла
                # 2. Если все сообщения есть - проверяем группу целиком

                cached_group = None
                if has_all_messages:
                    cached_group = await self._message_cache.get(group_key)
                    # Улучшенная проверка даты
                    if isinstance(cached_group, list) and len(cached_group) > 0:
                        try:
                            if not hasattr(cached_group[0], 'date'):
                                logger.error(f"Группа {group_key} повреждена: нет атрибута date")
                                await self._message_cache.set(group_key, None)
                                return None
                                
                            if cached_group[0].date < datetime.now() - timedelta(days=14):
                                logger.warning(f"Группа {group_key} устарела ({cached_group[0].date})")
                                # Инвалидируем всю группу и отдельные сообщения
                                for msg in cached_group:
                                    await self._message_cache.set(
                                        (msg.chat_id, msg.id), 
                                        None
                                    )
                                await self._message_cache.set(group_key, None)
                                return None
                        except Exception as e:
                            logger.error(f"Ошибка проверки даты группы {group_key}: {str(e)}")
                            return None
                    if cached_group:
                        logger.debug(f"[GROUP CACHE HIT] Найдена группа: {group_key}")
                        return cached_group
                # 3. Если группа не полная - загружаем заново

                logger.debug(f"[GROUP CACHE MISS] Загрузка группы: {group_key}")
                grouped_messages = []
                for msg_id in msg_data["grouped_ids"]:
                    # Используем уже загруженное основное сообщение

                    if msg_id == msg_data["message_id"]:
                        grouped_messages.append(message)
                        continue
                    msg = await self.client.get_messages(
                        msg_data["chat_id"], ids=msg_id
                    )
                    if msg:
                        grouped_messages.append(msg)
                        await self._message_cache.set(
                            (msg_data["chat_id"], msg_id), msg
                        )
                # Сохраняем полную группу только если все сообщения получены

                if len(grouped_messages) == len(msg_data["grouped_ids"]):
                    await self._message_cache.set(group_key, grouped_messages)
                    logger.debug(f"[GROUP CACHE SET] Сохранена группа: {group_key}")
                    return grouped_messages
                logger.warning(f"Не удалось загрузить полную группу для {group_key}")
                return None
            return message
        except Exception as e:
            logger.error(f"Ошибка _fetch_messages: {str(e)}", exc_info=True)
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
        logger.info(f"Уровень прав для чата {chat_id}: {permission_level}")
        return permission_level

    async def _handle_flood_wait(self, e: FloodWaitError, chat_id: int):
        wait_time = e.seconds + random.randint(5, 15)
        logger.info(f"Ожидание {wait_time} сек для чата {chat_id}")
        await asyncio.sleep(wait_time)
        self.error_counts.pop(f"{chat_id}_flood", None)

    async def _handle_permanent_error(self, chat_id: int):
        async with self._lock:
            for code in self.codes.values():
                code.chats.discard(chat_id)
        await self.save_config()

    async def _handle_temporary_error(self, chat_id: int):
        error_key = f"{chat_id}_temp"
        self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1
        if self.error_counts[error_key] > 3:
            await self._handle_permanent_error(chat_id)

    async def _handle_add_command(
        self, message: Message, code: Optional[Broadcast], code_name: str
    ):
        async with self._lock:
            reply = await message.get_reply_message()
            if not reply:
                await utils.answer(message, "❌ Ответьте на сообщение для добавления")
                return
            try:
                # 1. Создаем новую рассылку при необходимости

                is_new = code is None
                if is_new:
                    code = Broadcast()
                    self.codes[code_name] = code
                    logger.debug(f"Создан новый код рассылки: {code_name}")
                # 2. Обработка групповых сообщений

                grouped_ids = []
                if getattr(reply, "grouped_id", None):
                    async for msg in self.client.iter_messages(
                        reply.chat_id, offset_id=reply.id - 15, limit=30
                    ):
                        if getattr(msg, "grouped_id", None) == reply.grouped_id:
                            grouped_ids.append(msg.id)
                            # Кэшируем каждое сообщение отдельно

                            await self._message_cache.set((msg.chat_id, msg.id), msg)
                    grouped_ids = sorted(list(set(grouped_ids)))
                    logger.debug(f"Найдено групповых сообщений: {len(grouped_ids)}")
                # 3. Добавление в рассылку

                success = code.add_message(
                    chat_id=reply.chat_id,
                    message_id=reply.id,
                    grouped_ids=grouped_ids or None,
                )

                if not success:
                    await utils.answer(message, "❌ Сообщение уже существует")
                    return
                # 4. Принудительное сохранение с валидацией

                logger.debug(f"Перед сохранением. Сообщений: {len(code.messages)}")
                await self.save_config()

                # 5. Двойная проверка после сохранения

                if code_name in self.codes and len(self.codes[code_name].messages) > 0:
                    await utils.answer(
                        message,
                        f"✅ {'Создана рассылка' if is_new else 'Обновлена'} | "
                        f"Сообщений: {len(code.messages)}\n"
                        f"Групповых ID: {len(grouped_ids)}",
                    )
                else:
                    await utils.answer(message, "⚠️ Ошибка сохранения конфигурации!")
                    logger.debug("Конфигурация не сохранилась после добавления")
            except Exception as e:
                logger.critical(f"Critical error: {e}", exc_info=True)
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
            await utils.answer(
                message, "❌ Ответьте на сообщение, которое нужно удалить из рассылки"
            )
            return
        if code.remove_message(reply.id, reply.chat_id):
            await self.save_config()
            await utils.answer(message, "✅ Сообщение удалено из рассылки")
        else:
            await utils.answer(message, "❌ Это сообщение не найдено в рассылке")

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
                        str(chat_id)
                        for chat_id in tuple(failed_chats)[
                            i : i + self.NOTIFY_GROUP_SIZE
                        ]
                    )
                    for i in range(0, len(failed_chats), self.NOTIFY_GROUP_SIZE)
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
                    await asyncio.sleep(self.NOTIFY_DELAY)
        except Exception as e:
            logger.error(f"Ошибка обработки неудачных чатов для {code_name}: {e}")

    async def _start_broadcast_task(self, code_name: str, code: Broadcast):
        """Запускает или перезапускает задачу рассылки для кода."""
        if (
            code_name in self.broadcast_tasks
            and not self.broadcast_tasks[code_name].done()
        ):
            self.broadcast_tasks[code_name].cancel()
            try:
                await self.broadcast_tasks[
                    code_name
                ]  # Дождаться завершения отмененной задачи
            except asyncio.CancelledError:
                pass  # Ожидаемо, можно игнорировать
        self.broadcast_tasks[code_name] = asyncio.create_task(
            self._broadcast_loop(code_name)
        )
        logger.info(f"Запущена задача рассылки для кода: {code_name}")

    async def _load_config(self):
        try:
            config = self.db.get("broadcast", "config", {})
            logger.debug(f"ЗАГРУЖЕННАЯ КОНФИГУРАЦИЯ: {config}")

            if not config or "codes" not in config:
                logger.warning("Конфигурация пуста")
                return
            logger.info("Перезапуск активных рассылок из конфигурации...")
            for code_name, code_data in config.get("codes", {}).items():
                logger.debug(f"Восстановление кода {code_name}: {code_data}")

                broadcast = Broadcast(
                    chats=set(code_data.get("chats", [])),
                    messages=code_data.get("messages", []),
                    interval=tuple(code_data.get("interval", (10, 13))),
                    send_mode=code_data.get("send_mode", "auto"),
                    batch_mode=code_data.get("batch_mode", False),
                )
                broadcast._active = code_data.get("active", False)

                self.codes[code_name] = broadcast
                logger.debug(f"Восстановлен код {code_name}: {broadcast}")

                if broadcast._active:  # Проверяем active и запускаем задачу
                    await self._start_broadcast_task(code_name, broadcast)
            logger.info("Перезапуск активных рассылок завершен.")
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации: {e}", exc_info=True)

    async def _process_message_batch(self, code: Broadcast, messages: List[dict]):
        """Обработка пакета сообщений с принудительной перезагрузкой"""
        valid_messages = []
        deleted_messages = []

        for msg_data in messages:
            for attempt in range(3):
                try:
                    message = await self._fetch_messages(msg_data)
                    if not message:
                        raise ValueError("Сообщение не найдено")
                    # Проверка актуальности сообщения

                    if isinstance(
                        message, Message
                    ) and message.date < datetime.now() - timedelta(days=14):
                        logger.warning(f"Сообщение {msg_data} устарело")
                        deleted_messages.append(msg_data)
                        break
                    valid_messages.append(message)
                    break
                except FloodWaitError as e:
                    await self._handle_flood_wait(e, msg_data["chat_id"])
                except Exception as e:
                    logger.error(f"Ошибка загрузки {msg_data}: {str(e)}")
                    if attempt == 2:
                        deleted_messages.append(msg_data)
                    await asyncio.sleep(2**attempt)
        return valid_messages, deleted_messages

    async def _send_message(
        self,
        code_name: str,
        chat_id: int,
        msg: Union[Message, List[Message]],
        send_mode: str = "auto",
    ) -> bool:
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

            await self.GLOBAL_MINUTE_LIMITER.acquire()
            await self.GLOBAL_HOUR_LIMITER.acquire()

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
            logger.warning(f"Флуд-контроль: {e}")
            await self._handle_flood_wait(e, chat_id)
        except (ChatWriteForbiddenError, UserBannedInChannelError) as e:
            logger.error(f"Доступ запрещен: {chat_id}")
            await self._handle_permanent_error(chat_id)
        except Exception as e:
            logger.error(f"Неизвестная ошибка: {e}")
            await self._handle_temporary_error(chat_id)

    async def _send_messages_to_chats(self, code, code_name, messages):
        """Улучшенная отправка с приоритетом новых чатов"""
        active_chats = list(code.chats)
        random.shuffle(active_chats)

        success_count = 0
        failed_chats = set()

        # Приоритет для новых чатов (последние 20 добавленных)

        new_chats = active_chats[-20:]
        active_chats = new_chats + active_chats[:-20]

        for chat_id in active_chats:
            if not self._active or not code._active:
                break
            try:
                # Пропускаем чаты с частыми ошибками

                if self.error_counts.get(chat_id, 0) > 3:
                    continue
                # Отправка сообщения

                result = await self._send_message(code_name, chat_id, messages)

                if result:
                    success_count += 1
                    # Сброс счетчика ошибок при успехе

                    self.error_counts[chat_id] = 0
                else:
                    failed_chats.add(chat_id)
                # Динамическое регулирование скорости

                if success_count % 10 == 0:
                    await asyncio.sleep(random.uniform(0.2, 0.7))
            except Exception as e:
                logger.error(f"Ошибка в чате {chat_id}: {str(e)}")
                failed_chats.add(chat_id)
                self.error_counts[chat_id] = self.error_counts.get(chat_id, 0) + 1
        # Автоматическое удаление проблемных чатов

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
            # Логируем максимально подробно

            logger.debug(f"ПОЛНЫЙ СТАТУС КОДОВ: {self.codes}")
            logger.debug(f"КОЛИЧЕСТВО КОДОВ: {len(self.codes)}")

            for code_name, code in self.codes.items():
                logger.debug(
                    f"КОД {code_name}: chats={code.chats}, messages={code.messages}"
                )
            config = {
                "codes": {
                    name: {
                        "chats": list(code.chats),
                        "messages": code.messages,
                        "interval": list(code.interval),
                        "send_mode": code.send_mode,
                        "batch_mode": code.batch_mode,
                        "active": code._active,
                    }
                    for name, code in self.codes.items()
                },
                "version": 2,
                "timestamp": datetime.utcnow().timestamp(),
            }

            logger.debug(f"ФИНАЛЬНАЯ КОНФИГУРАЦИЯ: {config}")

            # Используем прямое сохранение без asyncio

            self.db.set("broadcast", "config", config)

            logger.debug("КОНФИГУРАЦИЯ СОХРАНЕНА")
        except Exception as e:
            logger.error(f"КРИТИЧЕСКАЯ ОШИБКА СОХРАНЕНИЯ: {e}", exc_info=True)

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
