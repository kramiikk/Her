import asyncio
import logging
import random
import time
from .. import _internal
from collections import deque, OrderedDict, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional, Set, Tuple

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

    async def clean_expired(self, force: bool = False):
        async with self._lock:
            if not force and len(self.cache) < self.max_size // 2:
                return
            current_time = time.time()
            expired = [
                k
                for k, (expire_time, _) in self.cache.items()
                if current_time > expire_time
            ]
            for key in expired:
                del self.cache[key]

    async def get(self, key: tuple):
        """
        Get a value from cache using a tuple key
        """
        async with self._lock:
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
        """
        Set a value in cache using a tuple key
        """
        async with self._lock:
            if expire is not None and expire <= 0:
                return
            ttl = expire if expire is not None else self.ttl
            expire_time = time.time() + ttl
            if key in self.cache:
                del self.cache[key]
            self.cache[key] = (expire_time, value)
            while len(self.cache) > self.max_size:
                self.cache.popitem(last=False)

    async def start_auto_cleanup(self):
        """Запускает фоновую задачу для периодической очистки кэша"""
        while True:
            await self.clean_expired()
            logger.debug("Периодическая очистка выполнена")
            await asyncio.sleep(self.ttl)


class BroadcastMod(loader.Module):
    """Модуль для массовой рассылки."""

    strings = {"name": "Broadcast"}
    
    @loader.command()
    async def bcmd(self, message):
        """Команда для управления рассылкой."""
        await self.manager.handle_command(message)

    async def client_ready(self):
        """Initialization sequence"""
        self.manager = BroadcastManager(self._client, self.db, self._client.tg_id)
        self.manager._message_cache = SimpleCache(ttl=7200, max_size=50)
        try:
            await asyncio.wait_for(self.manager.load_config(), timeout=30)
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
    messages: Set[Tuple[int, int]] = field(default_factory=set)
    interval: Tuple[int, int] = (10, 13)
    _last_message_index: int = field(default=0, init=False)
    _active: bool = field(default=False, init=False)
    original_interval: Tuple[int, int] = (10, 13)
    start_time: float = field(default_factory=time.time)
    last_sent: float = 0
    total_sent: int = 0
    total_failed: int = 0
    initial_chats_count: int = 0
    speed: float = 0
    last_error: Optional[Tuple[str, float]] = None

    def add_message(self, chat_id: int, message_id: int) -> bool:
        """Добавляет одиночное сообщение"""
        key = (chat_id, message_id)
        if key in self.messages:
            return False
        self.messages.add(key)
        return True

    def is_valid_interval(self) -> bool:
        """Проверяет корректность интервала"""
        min_val, max_val = self.interval
        return 0 < min_val < max_val <= 1440

    def remove_message(self, chat_id: int, message_id: int) -> bool:
        """Удаляет сообщение из рассылки"""
        key = (chat_id, message_id)
        if key in self.messages:
            self.messages.remove(key)
            return True
        return False


class BroadcastManager:
    """Manages broadcast operations and state."""

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
        """Масштабируемый цикл рассылки для больших объёмов чатов"""
        code = self.codes.get(code_name)
        if not code or not code.messages or not code.chats:
            return
        MAX_PARALLEL = 7
        SAFETY_FACTOR = 0.9
        MIN_BATCH_SIZE = 10
        MAX_BATCH_SIZE = 30

        chat_ring = deque(code.chats)
        last_sent = defaultdict(float)
        message = None
        semaphore = asyncio.Semaphore(MAX_PARALLEL)

        while self._active and code._active and not self.pause_event.is_set():
            if not code._active:
                break
            try:
                if not message:
                    msg_key = random.choice(list(code.messages))
                    message = await self._fetch_messages(msg_key)
                    if not message:
                        code.messages.remove(msg_key)
                        await self.save_config()
                        continue
                interval_range = code.interval[1] - code.interval[0]
                target_batch = max(
                    MIN_BATCH_SIZE,
                    min(
                        MAX_BATCH_SIZE,
                        int(len(code.chats) * SAFETY_FACTOR / interval_range),
                    ),
                )

                now = time.time()
                batch = []
                for _ in range(len(chat_ring)):
                    chat_id = chat_ring.popleft()
                    if now - last_sent[chat_id] >= code.interval[0] * 60:
                        batch.append(chat_id)
                        if len(batch) >= target_batch:
                            break
                    chat_ring.append(chat_id)
                failed_chats = set()
                batch_start_time = time.time()
                sent_count = 0

                async def send_to_chat(chat_id):
                    nonlocal sent_count
                    async with semaphore:
                        try:
                            success = await self._send_message(chat_id, message)
                            if success:
                                last_sent[chat_id] = now
                                sent_count += 1
                            else:
                                failed_chats.add(chat_id)
                            await asyncio.sleep(random.uniform(0.3, 0.7))
                        except Exception as e:
                            logger.error(f"Ошибка в {chat_id}: {str(e)}")
                            failed_chats.add(chat_id)

                if batch:
                    await asyncio.gather(*[send_to_chat(cid) for cid in batch])
                    chat_ring.extend(batch)

                    time_diff = time.time() - batch_start_time
                    if time_diff > 0:
                        code.speed = sent_count / time_diff * 60
                    else:
                        code.speed = 0
                if failed_chats:
                    code.total_failed += len(failed_chats)
                    code.chats -= failed_chats
                    chat_ring = deque(
                        cid for cid in chat_ring if cid not in failed_chats
                    )
                    await self.save_config()
                    await self._handle_failed_chats(code_name, failed_chats)
                if len(batch) < target_batch:
                    message = None
                    sleep_time = random.uniform(
                        code.interval[0] * 60 * 0.9, code.interval[1] * 60 * 1.1
                    )
                else:
                    sleep_time = random.uniform(
                        code.interval[0] * 60 / (len(code.chats) / target_batch),
                        code.interval[1] * 60 / (len(code.chats) / target_batch),
                    )
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{code_name}] Ошибка: {str(e)}")
                await asyncio.sleep(30)

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
        Fetch a message from cache or Telegram
        """
        try:
            chat_id = msg_data["chat_id"]
            message_id = msg_data["message_id"]

            cache_key = (chat_id, message_id)

            cached = await self._message_cache.get(cache_key)
            if cached:
                return cached
            try:
                msg = await self.client.get_messages(entity=chat_id, ids=message_id)
                if msg:
                    await self._message_cache.set(cache_key, msg)
                    logger.debug(f"Сообщение {chat_id}:{message_id} сохранено в кэш")
                    return msg
                else:
                    logger.error(f"Сообщение {chat_id}:{message_id} не найдено")
                    return None
            except ValueError as e:
                logger.error(
                    f"Чат/сообщение не существует: {chat_id} {message_id}: {e}"
                )
                return None
        except Exception as e:
            logger.error(f"Ошибка: {e}", exc_info=True)
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

    async def _handle_add_command(
        self, message: Message, code: Optional[Broadcast], code_name: str
    ):
        async with self._lock:
            reply = await message.get_reply_message()
            if not reply:
                await utils.answer(message, "❌ Ответьте на сообщение для добавления")
                return
            try:
                if code is None:
                    code = Broadcast()
                    self.codes[code_name] = code
                success = code.add_message(chat_id=reply.chat_id, message_id=reply.id)

                if not success:
                    await utils.answer(message, "❌ Сообщение уже существует")
                    return
                cache_key = (reply.chat_id, reply.id)
                await self._message_cache.set(cache_key, reply)
                await self.save_config()

                await utils.answer(
                    message,
                    f"✅ {'Создана рассылка' if code is None else 'Обновлена'} | "
                    f"Сообщений: {len(code.messages)}",
                )
            except Exception as e:
                if code is None and code_name in self.codes:
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
        code.initial_chats_count = len(code.chats)
        await utils.answer(message, "✅ Чат добавлен в рассылку")

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
        """Показывает расширенную статистику рассылок"""
        if not self.codes:
            await utils.answer(message, "❌ Нет активных рассылок")
            return
        lines = ["📊 **Статистика рассылок**\n"]

        for code_name, code in self.codes.items():

            status_icon = "✅" if code._active else "⏸️"
            runtime = timedelta(seconds=int(time.time() - code.start_time))
            progress = (
                f"{len(code.chats)}/{code.initial_chats_count}"
                if code.initial_chats_count > 0
                else len(code.chats)
            )

            next_run = (
                datetime.fromtimestamp(code.last_sent + code.interval[0] * 60)
                if code.last_sent
                else "-"
            )
            last_activity = (
                datetime.fromtimestamp(code.last_sent).strftime("%H:%M:%S")
                if code.last_sent
                else "никогда"
            )

            block = [
                f"**{code_name}** {status_icon}",
                f"├ 🕒 Работает: `{runtime}`",
                f"├ 📨 Сообщений: `{len(code.messages)}`",
                f"├ 📡 Статус: `{progress}` чатов",
                f"├ ⏱ Интервал: `{code.interval[0]}-{code.interval[1]} мин`",
                f"├ 📈 Успешно: `{code.total_sent}`",
                f"├ ❌ Ошибки: `{code.total_failed}`",
                (
                    f"├ 🔄 Следующий цикл: `{next_run:%H:%M:%S}`"
                    if code.last_sent
                    else "├ 🔄 Следующий цикл: `-`"
                ),
                f"└ ⚡ Скорость: `{code.speed:.1f} msg/min`",
            ]

            if code.last_error:
                block.append(
                    f"└ 🔥 Последняя ошибка: `{code.last_error[0]}` ({datetime.fromtimestamp(code.last_error[1]):%H:%M:%S})"
                )
            lines.append("\n".join(block))
        total_stats = (
            f"\n**Общая статистика**\n"
            f"├ Всего рассылок: {len(self.codes)}\n"
            f"├ Активных: {sum(1 for c in self.codes.values() if c._active)}\n"
            f"└ Всего сообщений отправлено: {sum(c.total_sent for c in self.codes.values())}"
        )
        lines.append(total_stats)

        await utils.answer(message, "\n\n".join(lines))

    async def _handle_remove_command(self, message: Message, code: Broadcast):
        """Обработчик команды удаления сообщения из рассылки"""
        reply = await message.get_reply_message()
        if not reply:
            await utils.answer(
                message, "❌ Ответьте на сообщение, которое нужно удалить"
            )
            return
        try:

            chat_id = reply.chat_id
            message_id = reply.id

            removed = code.remove_message(chat_id, message_id)

            if removed:

                cache_key = (chat_id, message_id)
                await self._message_cache.set(cache_key, None)
                await self.save_config()

                remaining = len(code.messages)
                status = (
                    f"✅ Сообщение удалено из рассылки\n"
                    f"📊 Осталось сообщений: {remaining}\n"
                    f"📍 Источник: [чат {chat_id}, сообщение {message_id}]"
                )
            else:
                status = "❌ Сообщение не найдено в текущей рассылке"
            await utils.answer(message, status)
        except Exception as e:
            error_msg = (
                f"🚨 Критическая ошибка при удалении:\n"
                f"Тип: {type(e).__name__}\n"
                f"Описание: {str(e)}"
            )
            logger.error(error_msg, exc_info=True)
            await utils.answer(message, error_msg)

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

    async def _restart_all_broadcasts(self):
        async with self._lock:
            for code_name, code in self.codes.items():
                if code._active and code_name not in self.broadcast_tasks:
                    if self.broadcast_tasks.get(code_name):
                        self.broadcast_tasks[code_name].cancel()
                    self.broadcast_tasks[code_name] = asyncio.create_task(
                        self._broadcast_loop(code_name)
                    )

    async def _send_message(self, chat_id: int, msg: Message) -> bool:
        """Отправка одиночного сообщения"""
        if self.pause_event.is_set():
            return False
        await self.GLOBAL_LIMITER.acquire()

        try:
            await self.client.forward_messages(
                entity=chat_id, messages=msg.id, from_peer=msg.chat_id
            )
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
            "start": lambda: self._handle_start_command(message, code, code_name),
            "stop": lambda: self._handle_stop_command(message, code, code_name),
        }

        handler = command_handlers.get(action)
        if handler:
            await handler()
        else:
            await utils.answer(message, "❌ Неизвестное действие")

    async def load_config(self):
        """Загрузка конфигурации в актуальном формате"""
        try:
            config = self.db.get("broadcast", "config") or {}

            for code_name, code_data in config.get("codes", {}).items():
                messages = {
                    (int(msg["chat_id"]), int(msg["message_id"]))
                    for msg in code_data.get("messages", [])
                    if isinstance(msg, dict)
                    and "chat_id" in msg
                    and "message_id" in msg
                }

                broadcast = Broadcast(
                    chats=set(map(int, code_data.get("chats", []))),
                    messages=messages,
                    interval=tuple(map(int, code_data.get("interval", (10, 13)))),
                    original_interval=tuple(
                        map(
                            int,
                            code_data.get(
                                "original_interval", code_data.get("interval", (10, 13))
                            ),
                        )
                    ),
                    _active=code_data.get("active", False),
                    start_time=code_data.get("start_time", time.time()),
                    last_sent=code_data.get("last_sent", 0),
                    total_sent=code_data.get("total_sent", 0),
                    total_failed=code_data.get("total_failed", 0),
                    initial_chats_count=len(code_data.get("chats", [])),
                )

                if "last_error" in code_data and isinstance(
                    code_data["last_error"], list
                ):
                    broadcast.last_error = (
                        str(code_data["last_error"][0]),
                        float(code_data["last_error"][1]),
                    )
                self.codes[code_name] = broadcast
                logger.debug(f"Loaded: {code_name} ({len(messages)} messages)")
        except Exception as e:
            logger.critical(f"Config load failed: {str(e)}")
            self.codes = {}

    async def save_config(self):
        """Сохранение конфигурации в актуальном формате"""
        try:
            config = {"codes": {}, "version": 1}

            for name, code in self.codes.items():
                messages = [
                    {"chat_id": cid, "message_id": mid} for cid, mid in code.messages
                ]

                code_data = {
                    "chats": list(code.chats),
                    "messages": messages,
                    "interval": list(code.interval),
                    "original_interval": list(code.original_interval),
                    "active": code._active,
                    "start_time": code.start_time,
                    "last_sent": code.last_sent,
                    "total_sent": code.total_sent,
                    "total_failed": code.total_failed,
                    "initial_chats_count": code.initial_chats_count,
                }

                if code.last_error:
                    code_data["last_error"] = [
                        code.last_error[0],
                        float(code.last_error[1]),
                    ]
                config["codes"][name] = code_data
            self.db.set("broadcast", "config", config)
            logger.debug("Конфигурация успешно сохранена")
        except Exception as e:
            logger.critical(f"Ошибка сохранения конфигурации: {str(e)}", exc_info=True)

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
