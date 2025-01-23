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

            if len(self.requests) >= self.max_requests:
                wait_time = self.time_window - (now - self.requests[0])
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
            self.requests.append(now)

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
        logger.info(f"Инициализирован кэш с TTL {ttl} сек, макс. размер {max_size}")

    def _estimate_ttl_stats(self) -> str:
        """Возвращает статистику времени жизни записей"""
        if not self.cache:
            return "Кэш пуст"
        current_time = time.time()
        ages = [current_time - t for t, _ in self.cache.values()]
        avg_age = sum(ages) / len(ages)
        max_age = max(ages)
        return (
            f"Средний возраст: {avg_age:.1f} сек, "
            f"Максимальный возраст: {max_age:.1f} сек, "
            f"Записей скоро истечёт ({self.ttl * 0.9}-{self.ttl} сек): "
            f"{len([a for a in ages if a > self.ttl * 0.9])}"
        )

    async def _maybe_cleanup(self):
        """Проверяет необходимость очистки устаревших записей"""
        current_time = time.time()
        if current_time - self._last_cleanup > self.ttl:
            logger.info("Запуск периодической очистки кэша")
            await self.clean_expired()
            self._last_cleanup = current_time

    async def clean_expired(self):
        """Очищает устаревшие записи"""
        if self._cleaning:
            return
        logger.info("[CACHE CLEAN] Начало очистки устаревших записей")
        try:
            self._cleaning = True
            async with self._lock:
                current_time = time.time()
                initial_size = len(self.cache)
                expired_keys = []

                for k, (t, _) in self.cache.items():
                    if current_time - t > self.ttl:
                        expired_keys.append(k)
                        logger.debug(
                            f"Найден устаревший ключ: {k} (возраст {current_time - t:.1f} сек)"
                        )
                logger.info(f"[CACHE] Найдено устаревших записей: {len(expired_keys)}")

                for key in expired_keys:
                    try:
                        del self.cache[key]
                    except KeyError:
                        pass
                logger.info(
                    f"[CACHE] Очистка завершена. Удалено: {len(expired_keys)}. "
                    f"Текущий размер: {len(self.cache)} (было {initial_size})"
                )
                logger.debug(
                    f"Статистика TTL после очистки: {self._estimate_ttl_stats()}"
                )
        except Exception as e:
            logger.error(f"Ошибка при очистке кэша: {e}", exc_info=True)
        finally:
            self._cleaning = False

    async def get(self, key):
        """Получает значение из кэша"""
        logger.info(f"[CACHE GET] Попытка получения ключа {key}")
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
                    logger.info(
                        f"[CACHE] Устаревшая запись {key} (возраст {age:.1f} сек)"
                    )
                    del self.cache[key]
                    return None
                logger.info(
                    f"[CACHE] Попадание кэша для {key}. "
                    f"Остаток TTL: {remaining_ttl:.1f} сек"
                )
                self.cache.move_to_end(key)
                return value
        except Exception as e:
            logger.error(f"Ошибка при получении значения из кэша: {e}", exc_info=True)
            return None

    async def get_cache_contents(self) -> dict:
        """Возвращает содержимое кэша с метаданными"""
        async with self._lock:
            return {
                key: {
                    "timestamp": ts,
                    "type": type(payload).__name__,
                    "is_media": bool(getattr(payload, "media", None)),
                    "group_size": len(payload) if isinstance(payload, list) else 1,
                }
                for key, (ts, payload) in self.cache.items()
            }

    async def set(self, key, value):
        """Устанавливает значение в кэш"""
        logger.info(f"[CACHE SET] Начало операции для ключа {key}")
        logger.debug(f"Текущий размер кэша: {len(self.cache)}")
        try:
            async with self._lock:
                try:
                    await self._maybe_cleanup()
                except Exception as e:
                    logger.error(f"Ошибка при проверке очистки: {e}", exc_info=True)
                if key in self.cache:
                    logger.info(f"[CACHE] Обновление ключа {key}")
                    old_ts, _ = self.cache[key]
                    age = time.time() - old_ts
                    logger.debug(f"Возраст предыдущей записи: {age:.1f} сек")
                cache_size_before = len(self.cache)
                while len(self.cache) >= self.max_size:
                    oldest_key = next(iter(self.cache))
                    logger.debug(f"Удаление старого ключа: {oldest_key}")
                    del self.cache[oldest_key]
                self.cache[key] = (time.time(), value)
                logger.info(
                    f"[CACHE] Успешно сохранено. Новый размер: {len(self.cache)} "
                    f"(было {cache_size_before}). TTL: {self.ttl} сек"
                )
                cache_contents = await self.get_cache_contents()

                for key, meta in cache_contents.items():
                    what = f"""
                    Чат: {key[0]}
                    Сообщение ID: {key[1]}
                    Тип: {meta['type']}
                    Медиа: {'Да' if meta['is_media'] else 'Нет'}
                    Группа: {meta['group_size']} сообщений
                    Время сохранения: {datetime.fromtimestamp(meta['timestamp'])}
                    """
                logger.debug(
                    f"Примерное время жизни записи: {self._estimate_ttl_stats()}\n{what}"
                )
        except Exception as e:
            logger.error(f"Критическая ошибка в методе set: {e}", exc_info=True)
            raise

    async def start_auto_cleanup(self):
        """Запускает фоновую задачу для периодической очистки кэша"""
        logger.info(f"[CACHE] Запуск фоновой очистки с интервалом {self.ttl} сек")
        while True:
            try:
                await self.clean_expired()
                await asyncio.sleep(self.ttl)
                logger.debug("[CACHE] Периодическая очистка выполнена")
            except Exception as e:
                logger.error(f"Ошибка очистки кэша: {e}")


@loader.tds
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
        await self.manager.stop_cache_cleanup()
        for task in self.manager.broadcast_tasks.values():
            task.cancel()

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
        """Добавляет сообщение с проверкой дубликатов"""
        message_data = {
            "chat_id": chat_id,
            "message_id": message_id,
            "grouped_ids": grouped_ids or [],
        }

        for existing in self.messages:
            if existing["chat_id"] == chat_id and existing["message_id"] == message_id:
                return False
        self.messages.append(message_data)

        return True

    @classmethod
    def from_dict(cls, data: dict) -> "Broadcast":
        """Создает объект из словаря"""
        instance = cls(
            chats=set(data.get("chats", [])),
            messages=data.get("messages", []),
            interval=tuple(data.get("interval", (10, 13))),
            send_mode=data.get("send_mode", "auto"),
            batch_mode=data.get("batch_mode", False),
        )
        instance._active = data.get("active", False)
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
        return {
            "chats": list(self.chats),
            "messages": self.messages,
            "interval": list(self.interval),
            "send_mode": self.send_mode,
            "batch_mode": self.batch_mode,
            "active": self._active,
        }


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
    GLOBAL_MINUTE_LIMITER = RateLimiter(20, 60)
    GLOBAL_HOUR_LIMITER = RateLimiter(250, 3600)
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
            await self._calculate_and_sleep(code.interval[0], code.interval[1])
            while self._active:
                deleted_messages = []
                messages_to_send = []

                try:
                    current_messages = code.messages.copy()
                    if not current_messages:
                        await asyncio.sleep(300)
                        continue
                    try:
                        if not current_messages:
                            batches = []
                        batches = [
                            current_messages[i : i + self.BATCH_SIZE_LARGE]
                            for i in range(
                                0, len(current_messages), self.BATCH_SIZE_LARGE
                            )
                        ]

                        for batch in batches:
                            if not self._active or not code._active:
                                return
                            batch_messages, deleted = await self._process_message_batch(
                                code, batch
                            )
                            messages_to_send.extend(batch_messages)
                            deleted_messages.extend(deleted)
                        if not messages_to_send:
                            await asyncio.sleep(300)
                            continue
                    except Exception as batch_error:
                        logger.error(
                            f"[{code_name}] Batch processing error: {batch_error}",
                            exc_info=True,
                        )
                        await asyncio.sleep(300)
                        continue
                    if deleted_messages:
                        code.messages = [
                            m for m in code.messages if m not in deleted_messages
                        ]
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
                    await asyncio.sleep(60)
                    await self.save_config()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(
                        f"[{code_name}] Error in broadcast loop: {str(e)}",
                        exc_info=True,
                    )
                    await asyncio.sleep(300)

    async def _calculate_and_sleep(self, min_interval: int, max_interval: int):
        """Вычисляет время сна и засыпает."""
        sleep_time = random.uniform(min_interval * 60, max_interval * 60)
        await asyncio.sleep(max(60, sleep_time - 15))

    async def _fetch_messages(self, msg_data: dict):
        """Получает сообщения с улучшенной обработкой ошибок"""
        try:
            key = (msg_data["chat_id"], msg_data["message_id"])

            cached = await self._message_cache.get(key)
            if cached:
                return cached
            message = await self.client.get_messages(
                msg_data["chat_id"], ids=msg_data["message_id"]
            )

            if message:
                if msg_data.get("grouped_ids"):
                    logger.info(
                        f"Обработка группы сообщений: {msg_data['grouped_ids']}"
                    )
                    messages = []
                    for msg_id in msg_data["grouped_ids"]:
                        logger.info(f"Получение сгруппированного сообщения {msg_id}")
                        grouped_msg = await self.client.get_messages(
                            msg_data["chat_id"], ids=msg_id
                        )
                        if grouped_msg:
                            messages.append(grouped_msg)
                    if messages:
                        logger.info(f"Сохранение {len(messages)} сообщений в кэш")
                        await self._message_cache.set(key, messages)
                        logger.info("Возврат группы сообщений")
                        return messages[0] if len(messages) == 1 else messages
                else:
                    await self._message_cache.set(key, message)
                    return message
            return None
        except Exception as e:
            logger.error(f"Ошибка в _fetch_messages: {str(e)}", exc_info=True)
            raise

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

    async def _get_chat_permissions(self, chat_id: int) -> int:
        """
        Enhanced permission check that safely handles cases where user cannot access chat

        Returns:
            permission_level: int
                0 - No permissions
                1 - Text only
                2 - Full media permissions
        """
        logger.info(f"Проверка прав доступа для чата {chat_id}")
        try:
            entity = await self.client.get_entity(chat_id)
            logger.debug(f"Получен объект сущности для чата {chat_id}")
        except ValueError as e:
            logger.warning(f"Не удалось получить сущность для чата {chat_id}: {e}")
            return self.MediaPermissions.NONE
        if not hasattr(entity, "default_banned_rights"):
            logger.warning(f"У сущности {chat_id} отсутствуют права доступа")
            return self.MediaPermissions.NONE
        banned = entity.default_banned_rights

        permission_level = (
            self.MediaPermissions.NONE
            if banned.send_messages
            else (
                self.MediaPermissions.TEXT_ONLY
                if banned.send_media or banned.send_photos
                else self.MediaPermissions.FULL_MEDIA
            )
        )

        logger.info(f"Уровень прав для чата {chat_id}: {permission_level}")
        return permission_level

    async def _handle_add_command(
        self, message: Message, code: Optional[Broadcast], code_name: str
    ):
        """Обработчик команды add"""
        async with self._lock:
            reply = await message.get_reply_message()
            if not reply:
                await utils.answer(
                    message,
                    "❌ Ответьте на сообщение, которое нужно добавить в рассылку",
                )
                return
            is_new = code is None
            if is_new:
                code = Broadcast()
            if len(code.messages) >= self.MAX_MESSAGES_PER_CODE:
                await utils.answer(
                    message,
                    f"❌ Достигнут лимит сообщений ({self.MAX_MESSAGES_PER_CODE})",
                )
                return
            grouped_id = getattr(reply, "grouped_id", None)
            grouped_ids = []

            if grouped_id:
                album_messages = []
                async for album_msg in message.client.iter_messages(
                    reply.chat_id,
                    min_id=max(0, reply.id - 10),
                    max_id=reply.id + 10,
                    limit=30,
                ):
                    if getattr(album_msg, "grouped_id", None) == grouped_id:
                        album_messages.append(album_msg)
                album_messages.sort(key=lambda m: m.id)
                grouped_ids = list(dict.fromkeys(msg.id for msg in album_messages))
            if code.add_message(reply.chat_id, reply.id, grouped_ids):
                if is_new:
                    self.codes[code_name] = code
                await self.save_config()
                await utils.answer(
                    message,
                    f"✅ {'Рассылка создана и с' if is_new else 'С'}ообщение добавлено",
                )
            else:
                await utils.answer(message, "❌ Это сообщение уже есть в рассылке")

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
            await message.respondondond("❌ Этот чат уже добавлен в рассылку")
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
            await message.respondond("❌ Укажите on или off")
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
            await message.respondond(
                "❌ Укажите минимальный и максимальный интервал в минутах"
            )
            return
        try:
            min_val = int(args[2])
            max_val = int(args[3])
        except ValueError:
            await message.respondondondondondondond("❌ Интервалы должны быть числами")
            return
        code.interval = (min_val, max_val)
        if not code.is_valid_interval():
            await message.respondond("❌ Некорректный интервал (0 < min < max <= 1440)")
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
            await message.respondond(
                "❌ Ответьте на сообщение, которое нужно удалить из рассылки"
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
            await message.respondondond("❌ Этот чат не найден в рассылке")
            return
        code.chats.remove(chat_id)
        await self.save_config()
        await message.respondondond("✅ Чат удален из рассылки")

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

    async def _load_config(self):
        """Loads configuration from database with improved state handling"""
        try:
            config = self.db.get("broadcast", "config", {})
            if not config:
                return
            for code_name, code_data in config.get("codes", {}).items():
                broadcast = Broadcast.from_dict(code_data)
                self.codes[code_name] = broadcast
                broadcast._active = False
            active_broadcasts = config.get("active_broadcasts", [])
            for code_name in active_broadcasts:
                try:
                    if code_name not in self.codes:
                        continue
                    code = self.codes[code_name]

                    if not code.messages or not code.chats:
                        continue
                    code._active = True
                    self.broadcast_tasks[code_name] = asyncio.create_task(
                        self._broadcast_loop(code_name)
                    )
                except Exception as e:
                    continue
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")

    async def _process_message_batch(
        self, code: Optional[Broadcast], messages: List[dict]
    ) -> Tuple[List[Union[Message, List[Message]]], List[dict]]:
        """Обрабатывает пакет сообщений с улучшенной обработкой ошибок."""
        if not code:
            logger.warning("Код рассылки не предоставлен")
            return [], messages
        messages_to_send = []
        deleted_messages = []

        fetch_tasks = []
        for msg in messages:
            task = asyncio.create_task(self._fetch_messages(msg))
            fetch_tasks.append((msg, task))
        try:
            results = []
            for msg, task in fetch_tasks:
                try:
                    result = await task
                    results.append((msg, result))
                except Exception as e:
                    logger.error(
                        f"Ошибка при выполнении задачи для {msg}: {e}", exc_info=True
                    )
                    results.append((msg, e))
            for msg_data, result in results:
                try:
                    if isinstance(result, Exception):
                        deleted_messages.append(msg_data)
                        continue
                    if not result:
                        deleted_messages.append(msg_data)
                        continue
                    messages_to_send.append(result)
                except Exception as e:
                    logger.error(
                        f"Ошибка при обработке результата для {msg_data}: {e}",
                        exc_info=True,
                    )
                    deleted_messages.append(msg_data)
            return messages_to_send, deleted_messages
        except Exception as e:
            logger.error(
                f"Критическая ошибка в _process_message_batch: {e}", exc_info=True
            )
            return [], messages

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
            error_key = f"{chat_id}_flood"
            self.error_counts[error_key] = 0
            wait_time = e.seconds + random.randint(5, 15)
            logger.warning(f"FloodWait {e.seconds}s → Adjusted {wait_time}s")
            await asyncio.sleep(wait_time)
            raise
        except (ChatWriteForbiddenError, UserBannedInChannelError):
            raise
        except Exception as e:
            logger.error(
                f"[{code_name}][send_message] Error sending to {chat_id}: {str(e)}"
            )
            error_key = f"{chat_id}_general"
            self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1
            self.last_error_time[error_key] = time.time()

            if self.error_counts[error_key] >= self.MAX_CONSECUTIVE_ERRORS:
                wait_time = 60 * (
                    2 ** (self.error_counts[error_key] - self.MAX_CONSECUTIVE_ERRORS)
                )
                await asyncio.sleep(wait_time)
            raise

    async def _send_messages_to_chats(
        self,
        code: Optional[Broadcast],
        code_name: str,
        messages_to_send: List[Union[Message, List[Message]]],
    ) -> Set[int]:
        async with self._semaphore:
            if not code:
                return set()
            failed_chats: Set[int] = set()
            media_restricted_chats: Set[int] = set()
            success_count: int = 0

            async def send_to_chat(chat_id: int):
                nonlocal success_count
                await asyncio.sleep(random.uniform(0.5, 1.5))
                try:
                    error_key = f"{chat_id}_general"
                    perm_key = f"{chat_id}_permission"

                    last_check = self.last_error_time.get(perm_key, 0)
                    if time.time() - last_check > self.PERMISSION_CHECK_INTERVAL:
                        perm_level = await self._get_chat_permissions(chat_id)

                        if perm_level == self.MediaPermissions.NONE:
                            self.error_counts[perm_key] = (
                                self.error_counts.get(perm_key, 0) + 1
                            )
                            self.last_error_time[perm_key] = time.time()

                            if (
                                self.error_counts[perm_key]
                                >= self.MAX_PERMISSION_RETRIES
                            ):
                                failed_chats.add(chat_id)
                            return
                    for message in messages_to_send:
                        has_media = (
                            isinstance(message, list) and any(m.media for m in message)
                        ) or (not isinstance(message, list) and message.media)

                        if has_media and perm_level == self.MediaPermissions.TEXT_ONLY:
                            media_restricted_chats.add(chat_id)
                            continue
                        success = await self._send_message(
                            code_name, chat_id, message, code.send_mode
                        )
                        if not success:
                            raise Exception(f"Failed to send message to {chat_id}")
                    success_count += 1
                    self.error_counts[error_key] = 0
                    self.error_counts[perm_key] = 0
                except Exception as e:
                    logger.error(f"Error in _send_messages_to_chats: {e}")

            chats = list(code.chats)
            random.shuffle(chats)
            total_chats = len(chats)

            async def _calculate_batch_size(total_chats: int) -> int:
                minute_usage_percent = await self.GLOBAL_MINUTE_LIMITER.get_stats()
                hour_usage_percent = await self.GLOBAL_HOUR_LIMITER.get_stats()

                if minute_usage_percent > 80 or hour_usage_percent > 80:
                    return max(self.BATCH_SIZE_SMALL // 2, 1)
                if total_chats <= self.BATCH_THRESHOLD_SMALL:
                    return self.BATCH_SIZE_SMALL
                elif total_chats <= self.BATCH_THRESHOLD_MEDIUM:
                    return self.BATCH_SIZE_MEDIUM
                return self.BATCH_SIZE_LARGE

            batch_size = await _calculate_batch_size(total_chats)

            for i in range(0, total_chats, batch_size):
                if not self._active or not code._active:
                    break
                current_batch = chats[i : i + batch_size]
                tasks = [send_to_chat(chat_id) for chat_id in current_batch]
                await asyncio.gather(*tasks)
                await self._calculate_and_sleep(code.interval[0], code.interval[1])
            if media_restricted_chats:
                message = (
                    f"⚠️ Рассылка '{code_name}':\n"
                    f"Обнаружено {len(media_restricted_chats)} чатов, где запрещена отправка медиа.\n"
                    f"ID чатов с ограничением медиа:\n{', '.join(map(str, media_restricted_chats))}"
                )
                await self.client.send_message(self.tg_id, message)
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
        """Saves configuration to database with improved reliability and state handling"""
        try:
            codes_snapshot = self.codes.copy()
            tasks_snapshot = self.broadcast_tasks.copy()

            invalid_codes = set()
            for code_name, code in codes_snapshot.items():
                if not code or not isinstance(code, Broadcast):
                    invalid_codes.add(code_name)
                    continue
                if not code.messages and not code.chats and not code._active:
                    invalid_codes.add(code_name)
                    continue
            for code_name in invalid_codes:
                codes_snapshot.pop(code_name, None)
                task = tasks_snapshot.pop(code_name, None)
                if task:
                    try:
                        if not task.done():
                            task.cancel()
                        await asyncio.wait_for(task, timeout=3)
                    except Exception as e:
                        logger.error(f"Error cleaning up task for {code_name}: {e}")
            for code_name, code in codes_snapshot.items():
                task = tasks_snapshot.get(code_name)
                code._active = bool(task and not task.done() and not task.cancelled())
            finished_tasks = [
                code_name
                for code_name, task in tasks_snapshot.items()
                if task and (task.done() or task.cancelled())
            ]

            for code_name in finished_tasks:
                task = tasks_snapshot.pop(code_name)
                try:
                    await asyncio.wait_for(task, timeout=3)
                except Exception as e:
                    logger.error(f"Error cleaning finished task for {code_name}: {e}")
            config = {
                "version": 1,
                "last_save": int(time.time()),
                "codes": {
                    name: code.to_dict()
                    for name, code in codes_snapshot.items()
                    if isinstance(code, Broadcast)
                },
                "active_broadcasts": [
                    name
                    for name, code in codes_snapshot.items()
                    if code._active and name in tasks_snapshot
                ],
            }

            self.codes = codes_snapshot
            self.broadcast_tasks = tasks_snapshot

            self.db.set("broadcast", "config", config)
        except Exception as e:
            logger.error(f"Critical error saving configuration: {e}", exc_info=True)
            raise

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
