import asyncio
import logging
import random
import time
from .. import _internal
from collections import deque, OrderedDict, defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
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

            for code_name, code in self.manager.codes.items():
                if code._active:
                    self.manager.broadcast_tasks[code_name] = asyncio.create_task(
                        self.manager._broadcast_loop(code_name)
                    )
                    logger.info(f"Автозапуск рассылки {code_name}")
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
    speed: float = 0
    last_error: Optional[Tuple[str, float]] = None

    def is_valid_interval(self) -> bool:
        """Проверяет корректность интервала"""
        min_val, max_val = self.interval
        return 0 < min_val < max_val <= 1440


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
        chat_ring = deque(code.chats)
        last_sent = defaultdict(float)
        message = None
        chat_semaphore = asyncio.Semaphore(1)

        while self._active and code._active and not self.pause_event.is_set():
            try:
                if not message:
                    if not code.messages:
                        logger.error(f"[{code_name}] Нет доступных сообщений")
                        break
                    msg_tuple = random.choice(tuple(code.messages))
                    message = await self._fetch_messages(msg_tuple)
                    if not message:
                        code.messages.remove(msg_tuple)
                        await self.save_config()
                        continue
                now = time.time()
                next_chat = None
                min_interval = code.interval[0] * 60

                for _ in range(len(chat_ring)):
                    chat_id = chat_ring.popleft()
                    time_since_last = now - last_sent[chat_id]

                    if time_since_last >= min_interval:
                        next_chat = chat_id
                        break
                    else:
                        chat_ring.append(chat_id)
                if next_chat is None:

                    min_wait = min(
                        min_interval - (now - last_sent[chat_id])
                        for chat_id in chat_ring
                    )
                    await asyncio.sleep(min_wait + random.uniform(0.5, 1.5))
                    continue
                async with chat_semaphore:
                    success = await self._send_message(next_chat, message)
                    current_time = time.time()

                    if success:
                        last_sent[next_chat] = current_time
                        code.last_sent = current_time
                        code.total_sent += 1
                        chat_ring.append(next_chat)
                    else:
                        code.total_failed += 1
                        if next_chat in code.chats:
                            code.chats.remove(next_chat)
                            await self.save_config()
                    delay = random.uniform(code.interval[0] * 60, code.interval[1] * 60)
                    await asyncio.sleep(delay)
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

    async def _fetch_messages(self, msg_tuple: Tuple[int, int]) -> Optional[Message]:
        """
        Fetch a message from cache or Telegram

        """
        try:
            chat_id, message_id = msg_tuple

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

    async def _generate_stats_report(self) -> str:
        """Генерация отчета: .br l"""
        if not self.codes:
            return "📭 Нет активных рассылок"
        report = ["📊 **Статистика рассылок**"]
        for code_name, code in self.codes.items():
            status = "✅" if code._active else "⏸"
            runtime = str(timedelta(seconds=int(time.time() - code.start_time)))[:-3]

            report.append(
                f"\n▸ **{code_name}** {status} {runtime}\n"
                f"├ Сообщений: {len(code.messages)}\n"
                f"├ Чатов: {len(code.chats)}\n"
                f"├ Интервал: {code.interval[0]}-{code.interval[1]} мин\n"
                f"└ Отправлено: ✅{code.total_sent} ❌{code.total_failed}"
            )
        return "".join(report)

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
                logger.warning(f"🚫 Ошибка в чате {chat_id}. Удален из рассылок.")
        await self.save_config()

    async def _handle_add(self, message, code, code_name, args) -> str:
        """Добавление сообщения в рассылку: .br a [code]"""
        reply = await message.get_reply_message()
        if not reply:
            return "🚫 Ответьте на сообщение"
        if not code:
            code = Broadcast()
            self.codes[code_name] = code
        key = (reply.chat_id, reply.id)
        if key in code.messages:
            return "ℹ️ Сообщение уже добавлено"
        code.messages.add(key)
        await self._message_cache.set(key, reply)
        await self.save_config()

        return f"✅ {code_name} | Сообщений: {len(code.messages)}"

    async def _handle_add_chat(self, message, code, code_name, args) -> str:
        """Добавление чата: .br ac [code] [@chat]"""
        target = args[2] if len(args) > 2 else message.chat_id
        chat_id = await self._parse_chat_identifier(target)

        if not chat_id:
            return "🚫 Неверный формат чата"
        if chat_id in code.chats:
            return "ℹ️ Чат уже добавлен"
        if len(code.chats) >= 500:
            return "🚫 Лимит 500 чатов"
        code.chats.add(chat_id)
        await self.save_config()
        return f"✅ +1 чат | Всего: {len(code.chats)}"

    async def _handle_delete(self, message, code, code_name, args) -> str:
        """Удаление рассылки: .br d [code]"""
        if code_name in self.broadcast_tasks:
            self.broadcast_tasks[code_name].cancel()
        del self.codes[code_name]
        await self.save_config()
        return f"🗑 {code_name} удалена"

    async def _handle_interval(self, message, code, code_name, args) -> str:
        """Установка интервала: .br i [code] [min] [max]"""
        if len(args) < 4:
            return "🚫 Укажите мин/макс интервалы"
        try:
            min_val = int(args[2])
            max_val = int(args[3])
        except ValueError:
            return "🚫 Некорректные значения"
        if not (0 < min_val < max_val <= 1440):
            return "🚫 Интервал 1-1440 мин (min < max)"
        code.interval = (min_val, max_val)
        code.original_interval = code.interval
        await self.save_config()

        return f"⏱ {code_name}: {min_val}-{max_val} мин"

    async def _handle_remove(self, message, code, code_name, args) -> str:
        """Удаление сообщения: .br r [code]"""
        reply = await message.get_reply_message()
        if not reply:
            return "🚫 Ответьте на сообщение"
        key = (reply.chat_id, reply.id)
        if key not in code.messages:
            return "🚫 Сообщение не найдено"
        code.messages.remove(key)
        await self._message_cache.set(key, None)
        await self.save_config()
        return f"✅ Удалено | Осталось: {len(code.messages)}"

    async def _handle_remove_chat(self, message, code, code_name, args) -> str:
        """Удаление чата: .br rc [code] [@chat]"""
        target = args[2] if len(args) > 2 else message.chat_id
        chat_id = await self._parse_chat_identifier(target)

        if not chat_id:
            return "🚫 Неверный формат чата"
        if chat_id not in code.chats:
            return "ℹ️ Чат не найден"
        code.chats.remove(chat_id)
        await self.save_config()
        return f"✅ -1 чат | Осталось: {len(code.chats)}"

    async def _handle_start(self, message, code, code_name, args) -> str:
        """Запуск рассылки: .br s [code]"""
        if not code.messages:
            return "🚫 Нет сообщений для отправки"
        if not code.chats:
            return "🚫 Нет чатов для рассылки"
        if code._active:
            return "ℹ️ Рассылка уже активна"
        code._active = True
        code.start_time = time.time()
        self.broadcast_tasks[code_name] = asyncio.create_task(
            self._broadcast_loop(code_name)
        )
        return f"🚀 {code_name} запущена | Чатов: {len(code.chats)}"

    async def _handle_stop(self, message, code, code_name, args) -> str:
        """Остановка рассылки: .br x [code]"""
        if not code._active:
            return "ℹ️ Рассылка не активна"
        code._active = False
        if code_name in self.broadcast_tasks:
            self.broadcast_tasks[code_name].cancel()
        return f"🛑 {code_name} остановлена"

    async def _parse_chat_identifier(self, identifier) -> Optional[int]:
        """Парсинг идентификатора чата"""
        try:
            if isinstance(identifier, int) or str(identifier).lstrip("-").isdigit():
                return int(identifier)
            entity = await self.client.get_entity(identifier)
            return entity.id
        except Exception:
            return None

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
        """Отправка одиночного сообщения с улучшенной обработкой ошибок"""
        if self.pause_event.is_set():
            return False
        try:
            await self.GLOBAL_LIMITER.acquire()
            max_retries = 3
            retry_delay = 5

            for attempt in range(max_retries):
                try:
                    await self.client.forward_messages(
                        entity=chat_id, messages=msg.id, from_peer=msg.chat_id
                    )
                    return True
                except FloodWaitError as e:
                    logger.error(f"Флуд-контроль: {e}")
                    await self._handle_flood_wait(e, chat_id)
                    return False
                except (ChatWriteForbiddenError, UserBannedInChannelError):
                    logger.error(f"Доступ запрещен: {chat_id}")
                    await self._handle_permanent_error(chat_id)
                    return False
                except Exception as e:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay * (attempt + 1))
                        continue
                    logger.error(f"🛑 Ошибка в {chat_id}: {repr(e)}")
                    await self._handle_permanent_error(chat_id)
                    return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке в {chat_id}: {repr(e)}")
            return False

    def _toggle_watcher(self, args) -> str:
        """Переключение авто-добавления: .br w [on/off]"""
        if len(args) < 2:
            return f"🔍 Автодобавление: {'ON' if self.watcher_enabled else 'OFF'}"
        self.watcher_enabled = args[1].lower() == "on"
        return f"✅ Автодобавление: {'ВКЛ' if self.watcher_enabled else 'ВЫКЛ'}"

    async def handle_command(self, message: Message):
        """Обработчик команд управления рассылкой"""
        args = message.text.split()[1:]
        if not args:
            return await utils.answer(message, "🚫 Недостаточно аргументов")
        action = args[0].lower()
        code_name = args[1] if len(args) > 1 else None

        if action == "l":
            return await utils.answer(message, await self._generate_stats_report())
        if action == "w":
            return await utils.answer(message, self._toggle_watcher(args))
        if not code_name:
            return await utils.answer(message, "🚫 Укажите код рассылки")
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
            return await utils.answer(message, "🚫 Неизвестная команда")
        if action != "a" and not code:
            return await utils.answer(message, f"🚫 Рассылка {code_name} не найдена")
        try:
            result = await handler_map[action](message, code, code_name, args)
            await utils.answer(message, result)
        except Exception as e:
            logger.error(f"Command error: {e}")
            await utils.answer(message, f"🚨 Ошибка: {str(e)}")

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
                    start_time=code_data.get("start_time", time.time()),
                    last_sent=code_data.get("last_sent", 0),
                    total_sent=code_data.get("total_sent", 0),
                    total_failed=code_data.get("total_failed", 0),
                )
                broadcast._active = code_data.get("active", False)

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
