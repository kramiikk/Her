import ast
import asyncio
import contextlib
import copy
import inspect
import time
import typing
from dataclasses import dataclass, field
from importlib.abc import SourceLoader

from hikkatl.hints import EntityLike
from hikkatl.tl.types import (
    ChannelFull,
    UserFull,
)

from .pointers import PointerDict, PointerList

__all__ = [
    "JSONSerializable",
    "ListLike",
    "StringLoader",
    "Module",
    "get_watchers",
    "get_callback_handlers",
    "PointerDict",
    "PointerList",
]


JSONSerializable = typing.Union[str, int, float, bool, list, dict, None]
ListLike = typing.Union[list, set, tuple]


class StringLoader(SourceLoader):
    """Load a python module/file from a string"""

    def __init__(self, data: str, origin: str):
        self.data = data.encode("utf-8") if isinstance(data, str) else data
        self.origin = origin

    def get_source(self, _=None) -> str:
        return self.data.decode("utf-8")

    def get_code(self, fullname: str) -> bytes:
        return (
            compile(source, self.origin, "exec", dont_inherit=True)
            if (source := self.get_data(fullname))
            else None
        )

    def get_filename(self, *args, **kwargs) -> str:
        return self.origin

    def get_data(self, *args, **kwargs) -> bytes:
        return self.data


class Module:
    strings = {"name": "Unknown"}

    """There is no help for this module"""

    def config_complete(self):
        """Called when module.config is populated"""

    async def client_ready(self):
        """Called after client is ready (after config_loaded)"""

    def internal_init(self):
        """Called after the class is initialized in order to pass the client and db. Do not call it yourself"""
        self.db = self.allmodules.db
        self.client = self.allmodules.client
        self.tg_id = self.client.tg_id

    async def on_unload(self):
        """Called after unloading / reloading module"""

    @property
    def callback_handlers(self) -> typing.Dict[str, typing.Callable[..., typing.Awaitable[typing.Any]]]:
        """List of callback handlers that module supports"""
        return get_callback_handlers(self)

    @property
    def her_callback_handlers(self) -> typing.Dict[str, typing.Callable[..., typing.Awaitable[typing.Any]]]:
        """List of callback handlers that module supports"""
        return get_callback_handlers(self)

    @property
    def watchers(self) -> typing.Dict[str, typing.Callable[..., typing.Awaitable[typing.Any]]]:
        """List of watchers that module supports"""
        return get_watchers(self)

    @property
    def her_watchers(self) -> typing.Dict[str, typing.Callable[..., typing.Awaitable[typing.Any]]]:
        """List of watchers that module supports"""
        return get_watchers(self)

    @callback_handlers.setter
    def callback_handlers(self, _):
        pass

    @her_callback_handlers.setter
    def her_callback_handlers(self, _):
        pass

    @watchers.setter
    def watchers(self, _):
        pass

    @her_watchers.setter
    def her_watchers(self, _):
        pass

    def get(
        self,
        key: str,
        default: typing.Optional[JSONSerializable] = None,
    ) -> JSONSerializable:
        return self.db.get(self.__class__.__name__, key, default)

    def set(self, key: str, value: JSONSerializable) -> bool:
        self.db.set(self.__class__.__name__, key, value)

    def pointer(
        self,
        key: str,
        default: typing.Optional[JSONSerializable] = None,
        item_type: typing.Optional[typing.Any] = None,
    ) -> typing.Union[JSONSerializable, PointerList, PointerDict]:
        return self.db.pointer(self.__class__.__name__, key, default, item_type)


class LoadError(Exception):
    """Tells user, why your module can't be loaded, if raised in `client_ready`"""

    def __init__(self, error_message: str):
        self._error = error_message

    def __str__(self) -> str:
        return self._error


class SelfUnload(Exception):
    """Silently unloads module, if raised in `client_ready`"""

    def __init__(self, error_message: str = ""):
        super().__init__()
        self._error = error_message

    def __str__(self) -> str:
        return self._error


class SelfSuspend(Exception):
    """
    Silently suspends module, if raised in `client_ready`
    Watcher will not be registered if raised
    Module won't be unloaded from db and will be unfreezed after restart, unless
    the exception is raised again
    """

    def __init__(self, error_message: str = ""):
        super().__init__()
        self._error = error_message

    def __str__(self) -> str:
        return self._error


class ModuleConfig(dict):
    """Stores config for modules and apparently libraries"""

    def __init__(self, *entries: typing.Union[str, "ConfigValue"]):
        if all(isinstance(entry, ConfigValue) for entry in entries):

            self._config = {config.option: config for config in entries}
        else:
            keys = []
            values = []
            defaults = []
            docstrings = []
            for i, entry in enumerate(entries):
                if i % 3 == 0:
                    keys += [entry]
                elif i % 3 == 1:
                    values += [entry]
                    defaults += [entry]
                else:
                    docstrings += [entry]
            self._config = {
                key: ConfigValue(option=key, default=default, doc=doc)
                for key, default, doc in zip(keys, defaults, docstrings)
            }
        super().__init__(
            {option: config.value for option, config in self._config.items()}
        )

    def getdef(self, key: str) -> str:
        """Get the default value by key"""
        return self._config[key].default

    def __setitem__(self, key: str, value: typing.Any):
        self._config[key].value = value
        super().__setitem__(key, value)

    def set_no_raise(self, key: str, value: typing.Any):
        self._config[key].set_no_raise(value)
        super().__setitem__(key, value)

    def __getitem__(self, key: str) -> typing.Any:
        try:
            return self._config[key].value
        except KeyError:
            return None

    def reload(self):
        for key in self._config:
            super().__setitem__(key, self._config[key].value)

    def change_validator(
        self,
        key: str,
        validator: typing.Callable[[JSONSerializable], JSONSerializable],
    ):
        self._config[key].validator = validator


class _Placeholder:
    """Placeholder to determine if the default value is going to be set"""


async def wrap(func: typing.Callable[[], typing.Awaitable]) -> typing.Any:
    with contextlib.suppress(Exception):
        return await func()


def syncwrap(func: typing.Callable[[], typing.Any]) -> typing.Any:
    with contextlib.suppress(Exception):
        return func()


@dataclass(repr=True)
class ConfigValue:
    option: str
    default: typing.Any = None
    doc: typing.Union[typing.Callable[[], str], str] = "No description"
    value: typing.Any = field(default_factory=_Placeholder)
    validator: typing.Optional[
        typing.Callable[[JSONSerializable], JSONSerializable]
    ] = None
    on_change: typing.Optional[
        typing.Union[typing.Callable[[], typing.Awaitable], typing.Callable]
    ] = None

    def __post_init__(self):
        if isinstance(self.value, _Placeholder):
            self.value = self.default

    def set_no_raise(self, value: typing.Any) -> bool:
        """
        Sets the config value w/o ValidationError being raised
        Should not be used uninternally
        """
        return self.__setattr__("value", value, ignore_validation=True)

    def __setattr__(
        self,
        key: str,
        value: typing.Any,
        *,
        ignore_validation: bool = False,
    ):
        if key == "value":
            try:
                value = ast.literal_eval(value)
            except Exception:
                pass

            if isinstance(value, (set, tuple)):
                value = list(value)
            if isinstance(value, list):
                value = [
                    item.strip() if isinstance(item, str) else item for item in value
                ]
            if self.validator is not None:
                if value is not None:
                    from . import validators

                    try:
                        value = self.validator.validate(value)
                    except validators.ValidationError as e:
                        if not ignore_validation:
                            raise e
                        value = self.default
                else:
                    defaults = {
                        "String": "",
                        "Integer": 0,
                        "Boolean": False,
                        "Series": [],
                        "Float": 0.0,
                    }

                    if self.validator.internal_id in defaults:
                        value = defaults[self.validator.internal_id]
            self._save_marker = True
        object.__setattr__(self, key, value)

        if key == "value" and not ignore_validation and callable(self.on_change):
            if inspect.iscoroutinefunction(self.on_change):
                asyncio.ensure_future(wrap(self.on_change))
            else:
                syncwrap(self.on_change)


def _get_members(
    mod: Module,
    ending: str,
    attribute: typing.Optional[str] = None,
    strict: bool = False,
) -> dict:
    """Get method of module, which end with ending"""
    return {
        (
            method_name.rsplit(ending, maxsplit=1)[0]
            if (method_name == ending if strict else method_name.endswith(ending))
            else method_name
        ).lower(): getattr(mod, method_name)
        for method_name in dir(mod)
        if not isinstance(getattr(type(mod), method_name, None), property)
        and callable(getattr(mod, method_name))
        and (
            (method_name == ending if strict else method_name.endswith(ending))
            or attribute
            and getattr(getattr(mod, method_name), attribute, False)
        )
    }


class CacheRecordEntity:
    def __init__(
        self,
        hashable_entity: "Hashable",
        resolved_entity: EntityLike,
        exp: int,
    ):
        self.entity = copy.deepcopy(resolved_entity)
        self._hashable_entity = copy.deepcopy(hashable_entity)
        self._exp = round(time.time() + exp)
        self.ts = time.time()

    @property
    def expired(self) -> bool:
        return self._exp < time.time()

    def __eq__(self, record: "CacheRecordEntity") -> bool:
        return hash(record) == hash(self)

    def __hash__(self) -> int:
        return hash(self._hashable_entity)

    def __str__(self) -> str:
        return f"CacheRecordEntity of {self.entity}"

    def __repr__(self) -> str:
        return (
            f"CacheRecordEntity(entity={type(self.entity).__name__}(...),"
            f" exp={self._exp})"
        )


class CacheRecordPerms:
    def __init__(
        self,
        hashable_entity: "Hashable",
        hashable_user: "Hashable",
        resolved_perms: EntityLike,
        exp: int,
    ):
        self.perms = copy.deepcopy(resolved_perms)
        self._hashable_entity = copy.deepcopy(hashable_entity)
        self._hashable_user = copy.deepcopy(hashable_user)
        self._exp = round(time.time() + exp)
        self.ts = time.time()

    @property
    def expired(self) -> bool:
        return self._exp < time.time()

    def __eq__(self, record: "CacheRecordPerms") -> bool:
        return hash(record) == hash(self)

    def __hash__(self) -> int:
        return hash((self._hashable_entity, self._hashable_user))

    def __str__(self) -> str:
        return f"CacheRecordPerms of {self.perms}"

    def __repr__(self) -> str:
        return (
            f"CacheRecordPerms(perms={type(self.perms).__name__}(...), exp={self._exp})"
        )


class CacheRecordFullChannel:
    def __init__(self, channel_id: int, full_channel: ChannelFull, exp: int):
        self.channel_id = channel_id
        self.full_channel = full_channel
        self._exp = round(time.time() + exp)
        self.ts = time.time()

    @property
    def expired(self) -> bool:
        return self._exp < time.time()

    def __eq__(self, record: "CacheRecordFullChannel") -> bool:
        return hash(record) == hash(self)

    def __hash__(self) -> int:
        return hash((self._hashable_entity, self._hashable_user))

    def __str__(self) -> str:
        return f"CacheRecordFullChannel of {self.channel_id}"

    def __repr__(self) -> str:
        return (
            f"CacheRecordFullChannel(channel_id={self.channel_id}(...),"
            f" exp={self._exp})"
        )


class CacheRecordFullUser:
    def __init__(self, user_id: int, full_user: UserFull, exp: int):
        self.user_id = user_id
        self.full_user = full_user
        self._exp = round(time.time() + exp)
        self.ts = time.time()

    @property
    def expired(self) -> bool:
        return self._exp < time.time()

    def __eq__(self, record: "CacheRecordFullUser") -> bool:
        return hash(record) == hash(self)

    def __hash__(self) -> int:
        return hash((self._hashable_entity, self._hashable_user))

    def __str__(self) -> str:
        return f"CacheRecordFullUser of {self.user_id}"

    def __repr__(self) -> str:
        return f"CacheRecordFullUser(channel_id={self.user_id}(...), exp={self._exp})"


def get_callback_handlers(mod: Module) -> dict:
    """Introspect the module to get its callback handlers"""
    return _get_members(mod, "_callback_handler", "is_callback_handler")


def get_watchers(mod: Module) -> dict:
    """Introspect the module to get its watchers"""
    return _get_members(
        mod,
        "watcher",
        "is_watcher",
        strict=True,
    )