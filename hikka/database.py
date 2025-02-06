# 🌟 Hikka, Friendly Telegram

# Maintainers  | Dan Gazizullin, codrago 
# Years Active | 2018 - 2024 
# Repository   | https://github.com/hikariatama/Hikka


import asyncio
import collections
import json
import logging
import os
import time

try:
    import redis
except ImportError as e:
    if "RAILWAY" in os.environ:
        raise e
import typing

from collections import deque
from typing import Deque
from hikkatl.tl.types import User

from . import main, utils
from .pointers import (
    BaseSerializingMiddlewareDict,
    BaseSerializingMiddlewareList,
    NamedTupleMiddlewareDict,
    NamedTupleMiddlewareList,
    PointerDict,
    PointerList,
)
from .tl_cache import CustomTelegramClient
from .types import JSONSerializable

__all__ = [
    "Database",
    "PointerList",
    "PointerDict",
    "NamedTupleMiddlewareDict",
    "NamedTupleMiddlewareList",
    "BaseSerializingMiddlewareDict",
    "BaseSerializingMiddlewareList",
]

logger = logging.getLogger(__name__)


class Database(dict):
    def __init__(self, client: CustomTelegramClient):
        super().__init__()
        self._save_lock = asyncio.Lock()
        self._client: CustomTelegramClient = client
        self._next_revision_call: int = 0
        self._me: User = None
        self._redis: redis.Redis = None
        self._saving_task: asyncio.Future = None
        self._revisions: Deque[dict] = deque(maxlen=15)

    def __repr__(self):
        return object.__repr__(self)

    def _redis_save_sync(self):
        with self._redis.pipeline() as pipe:
            pipe.set(
                str(self._client.tg_id),
                json.dumps(self, ensure_ascii=True),
            )
            pipe.execute()

    async def remote_force_save(self) -> bool:
        """Force save database to remote endpoint without waiting"""
        if not self._redis:
            return False
        try:
            await utils.run_sync(self._redis_save_sync)
            return True
        except Exception as e:
            logger.error(f"Force save failed: {e}")
            return False

    async def _redis_save(self) -> bool:
        """Save database to redis"""
        async with self._save_lock:
            if not self._redis:
                return False
            await asyncio.sleep(1)
            await utils.run_sync(self._redis_save_sync)
            self._saving_task = None
            return True

    async def redis_init(self) -> bool:
        """Init redis database"""
        if REDIS_URI := (
            os.environ.get("REDIS_URL") or main.get_config_key("redis_uri")
        ):
            self._redis = redis.Redis.from_url(REDIS_URI)
            return True
        return False

    async def init(self):
        """Asynchronous initialization unit"""
        await self.redis_init()
        if not self._redis:
            self._db_file = main.BASE_PATH / f"config-{self._client.tg_id}.json"
            self.read()

    def read(self):
        if self._redis:
            try:
                if self._redis.ping():
                    data = self._redis.get(str(self._client.tg_id))
                    if data:
                        self.update(**json.loads(data.decode()))
                    else:
                        logger.info("Redis empty, new DB created")
                else:
                    logger.error("Redis connection lost")
            except Exception:
                logger.exception("Redis error")
            return
        try:
            if self._db_file.exists():
                self.update(**json.loads(self._db_file.read_text()))
            else:
                logger.info("Local DB not found, creating new one")
        except json.JSONDecodeError:
            logger.error("DB corrupted, resetting...")
            self._db_file.unlink()
        except Exception as e:
            logger.error(f"Read failed: {e}")

    def process_db_autofix(self, db: dict) -> bool:
        if not utils.is_serializable(db):
            return False
        for key, value in db.copy().items():
            if not isinstance(key, (str, int)):
                logger.warning(
                    "DbAutoFix: Dropped key %s, because it is not string or int",
                    key,
                )
                continue
            if not isinstance(value, dict):
                # If value is not a dict (module values), drop it,
                # otherwise it may cause problems

                del db[key]
                logger.warning(
                    "DbAutoFix: Dropped key %s, because it is non-dict, but %s",
                    key,
                    type(value),
                )
                continue
            for subkey in list(value.keys()):
                if not isinstance(subkey, (str, int)):
                    del db[key][subkey]
                    logger.warning(
                        (
                            "DbAutoFix: Dropped subkey %s of db key %s, because it is"
                            " not string or int"
                        ),
                        subkey,
                        key,
                    )
                    continue
        return True

    def save(self) -> bool:
        """Save database"""
        if not self.process_db_autofix(self):
            try:
                rev = self._revisions.pop()
                self.clear()
                self.update(**rev)
            except IndexError:
                logger.critical("No valid revisions available")
                return False
            logger.warning("Restored database from last valid revision")
            return self.save()
        if self._next_revision_call < time.time():
            self._revisions.append(dict(self))
            self._next_revision_call = time.time() + 3
        if self._redis:
            if not self._saving_task:
                self._saving_task = asyncio.ensure_future(self._redis_save())
            return True
        try:
            self._db_file.write_text(json.dumps(self, indent=4))
        except Exception:
            logger.exception("Database save failed!")
            return False
        return True

    def get(
        self,
        owner: str,
        key: str,
        default: typing.Optional[JSONSerializable] = None,
    ) -> JSONSerializable:
        """Get database key"""
        try:
            return self[owner][key]
        except KeyError:
            return default

    def set(self, owner: str, key: str, value: JSONSerializable) -> bool:
        """Set database key"""
        if not utils.is_serializable(owner):
            raise RuntimeError(
                "Attempted to write object to "
                f"{owner=} ({type(owner)=}) of database. It is not "
                "JSON-serializable key which will cause errors"
            )
        if not utils.is_serializable(key):
            raise RuntimeError(
                "Attempted to write object to "
                f"{key=} ({type(key)=}) of database. It is not "
                "JSON-serializable key which will cause errors"
            )
        if not utils.is_serializable(value):
            raise RuntimeError(
                "Attempted to write object of "
                f"{key=} ({type(value)=}) to database. It is not "
                "JSON-serializable value which will cause errors"
            )
        super().setdefault(owner, {})[key] = value
        return self.save()

    def pointer(
        self,
        owner: str,
        key: str,
        default: typing.Optional[JSONSerializable] = None,
        item_type: typing.Optional[typing.Any] = None,
    ) -> typing.Union[JSONSerializable, PointerList, PointerDict]:
        """Get a pointer to database key"""
        value = self.get(owner, key, default)
        mapping = {
            list: PointerList,
            dict: PointerDict,
            collections.abc.Hashable: lambda v: v,
        }

        pointer_constructor = next(
            (pointer for type_, pointer in mapping.items() if isinstance(value, type_)),
            None,
        )

        if (current_value := self.get(owner, key, None)) and type(
            current_value
        ) is not type(default):
            raise ValueError(
                f"Can't switch the type of pointer in database (current: {type(current_value)}, requested: {type(default)})"
            )
        if pointer_constructor is None:
            raise ValueError(
                f"Pointer for type {type(value).__name__} is not implemented"
            )
        if item_type is not None:
            if isinstance(value, list):
                for item in self.get(owner, key, default):
                    if not isinstance(item, dict):
                        raise ValueError(
                            "Item type can only be specified for dedicated keys and"
                            " can't be mixed with other ones"
                        )
                return NamedTupleMiddlewareList(
                    pointer_constructor(self, owner, key, default),
                    item_type,
                )
            if isinstance(value, dict):
                for item in self.get(owner, key, default).values():
                    if not isinstance(item, dict):
                        raise ValueError(
                            "Item type can only be specified for dedicated keys and"
                            " can't be mixed with other ones"
                        )
                return NamedTupleMiddlewareDict(
                    pointer_constructor(self, owner, key, default),
                    item_type,
                )
        return pointer_constructor(self, owner, key, default)
