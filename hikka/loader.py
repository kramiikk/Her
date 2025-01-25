"""Registers modules"""

# Friendly Telegram (telegram userbot)
# Copyright (C) 2018-2019 The Authors
# ©️ Dan Gazizullin, codrago 2021-2024
# This file is a part of Her
# 🌐 https://github.com/hikariatama/Hikka

import asyncio
import builtins
import contextlib
import importlib
import importlib.machinery
import importlib.util
import inspect
import logging
import os
import re
import sys
import typing
from pathlib import Path
from types import FunctionType
from uuid import uuid4

from hikkatl.tl.tlobject import TLObject

from . import security, utils, validators
from .database import Database
from .types import (
    Command,
    ConfigValue,
    CoreOverwriteError,
    CoreUnloadError,
    JSONSerializable,
    Library,
    LibraryConfig,
    LoadError,
    Module,
    ModuleConfig,
    SelfSuspend,
    SelfUnload,
    StopLoop,
    StringLoader,
    get_callback_handlers,
    get_commands,
)

__all__ = [
    "Modules",
    "InfiniteLoop",
    "Command",
    "CoreOverwriteError",
    "CoreUnloadError",
    "JSONSerializable",
    "Library",
    "LibraryConfig",
    "LoadError",
    "Module",
    "SelfSuspend",
    "SelfUnload",
    "StopLoop",
    "StringLoader",
    "get_commands",
    "get_callback_handlers",
    "validators",
    "Database",
    "ConfigValue",
    "ModuleConfig",
    "owner",
    "group_owner",
    "group_admin_add_admins",
    "group_admin_change_info",
    "group_admin_ban_users",
    "group_admin_delete_messages",
    "group_admin_pin_messages",
    "group_admin_invite_users",
    "group_admin",
    "group_member",
    "pm",
    "unrestricted",
    "loop",
]

logger = logging.getLogger(__name__)

owner = security.owner

# deprecated
sudo = security.sudo
support = security.support
# /deprecated

group_owner = security.group_owner
group_admin_add_admins = security.group_admin_add_admins
group_admin_change_info = security.group_admin_change_info
group_admin_ban_users = security.group_admin_ban_users
group_admin_delete_messages = security.group_admin_delete_messages
group_admin_pin_messages = security.group_admin_pin_messages
group_admin_invite_users = security.group_admin_invite_users
group_admin = security.group_admin
group_member = security.group_member
pm = security.pm
unrestricted = security.unrestricted


async def stop_placeholder() -> bool:
    return True


class Placeholder:
    """Placeholder"""


VALID_PIP_PACKAGES = re.compile(
    r"^\s*# ?requires:(?: ?)((?:{url} )*(?:{url}))\s*$".format(
        url=r"[-[\]_.~:/?#@!$&'()*+,;%<=>a-zA-Z0-9]+"
    ),
    re.MULTILINE,
)

USER_INSTALL = "PIP_TARGET" not in os.environ and "VIRTUAL_ENV" not in os.environ

native_import = builtins.__import__


def patched_import(name: str, *args, **kwargs):
    if name.startswith("telethon"):
        return native_import("hikkatl" + name[8:], *args, **kwargs)

    return native_import(name, *args, **kwargs)


builtins.__import__ = patched_import


class InfiniteLoop:
    _task = None
    status = False
    module_instance = None  # Will be passed later

    def __init__(
        self,
        func: FunctionType,
        interval: int,
        autostart: bool,
        wait_before: bool,
        stop_clause: typing.Union[str, None],
    ):
        self.func = func
        self.interval = interval
        self._wait_before = wait_before
        self._stop_clause = stop_clause
        self.autostart = autostart

    def _stop(self, *args, **kwargs):
        self._wait_for_stop.set()

    def stop(self, *args, **kwargs):
        if self._task:
            self._wait_for_stop = asyncio.Event()
            self.status = False
            self._task.add_done_callback(self._stop)
            self._task.cancel()
            return asyncio.ensure_future(self._wait_for_stop.wait())

        return asyncio.ensure_future(stop_placeholder())

    def start(self, *args, **kwargs):
        if not self._task:
            self._task = asyncio.ensure_future(self.actual_loop(*args, **kwargs))

    async def actual_loop(self, *args, **kwargs):
        # Wait for loader to set attribute
        while not self.module_instance:
            await asyncio.sleep(0.01)

        if isinstance(self._stop_clause, str) and self._stop_clause:
            self.module_instance.set(self._stop_clause, True)

        self.status = True

        while self.status:
            if self._wait_before:
                await asyncio.sleep(self.interval)

            if (
                isinstance(self._stop_clause, str)
                and self._stop_clause
                and not self.module_instance.get(self._stop_clause, False)
            ):
                break

            try:
                await self.func(self.module_instance, *args, **kwargs)
            except StopLoop:
                break
            except Exception:
                logger.exception("Error running loop!")

            if not self._wait_before:
                await asyncio.sleep(self.interval)

        self._wait_for_stop.set()

        self.status = False

    def __del__(self):
        self.stop()


def loop(
    interval: int = 5,
    autostart: typing.Optional[bool] = False,
    wait_before: typing.Optional[bool] = False,
    stop_clause: typing.Optional[str] = None,
) -> FunctionType:
    """Create new infinite loop from class method"""

    def wrapped(func):
        return InfiniteLoop(func, interval, autostart, wait_before, stop_clause)

    return wrapped


MODULES_NAME = "modules"
ru_keys = 'ёйцукенгшщзхъфывапролджэячсмитьбю.Ё"№;%:?ЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭ/ЯЧСМИТЬБЮ,'
en_keys = "`qwertyuiop[]asdfghjkl;'zxcvbnm,./~@#$%^&QWERTYUIOP{}ASDFGHJKL:\"|ZXCVBNM<>?"

BASE_DIR = (
    "/data"
    if "DOCKER" in os.environ
    else os.path.normpath(os.path.join(utils.get_base_dir(), ".."))
)

LOADED_MODULES_DIR = os.path.join(BASE_DIR, "loaded_modules")
LOADED_MODULES_PATH = Path(LOADED_MODULES_DIR)
LOADED_MODULES_PATH.mkdir(parents=True, exist_ok=True)


def ratelimit(func: Command) -> Command:
    """Decorator that causes ratelimiting for this command to be enforced more strictly"""
    func.ratelimit = True
    return func


def tag(*tags, **kwarg_tags):
    """Tag function (esp. watchers) with some tags"""

    def inner(func: Command) -> Command:
        for _tag in tags:
            setattr(func, _tag, True)

        for _tag, value in kwarg_tags.items():
            setattr(func, _tag, value)

        return func

    return inner


def _mark_method(mark: str, *args, **kwargs) -> typing.Callable[..., Command]:
    """
    Mark method as a method of a class
    """

    def decorator(func: Command) -> Command:
        setattr(func, mark, True)
        for arg in args:
            setattr(func, arg, True)

        for kwarg, value in kwargs.items():
            setattr(func, kwarg, value)

        return func

    return decorator


def command(*args, **kwargs):
    """
    Decorator that marks function as userbot command
    """
    return _mark_method("is_command", *args, **kwargs)


def debug_method(*args, **kwargs):
    """
    Decorator that marks function as IDM (Internal Debug Method)
    :param name: Name of the method
    """
    return _mark_method("is_debug_method", *args, **kwargs)


def watcher(*args, **kwargs):
    """
    Decorator that marks function as watcher
    """
    return _mark_method("is_watcher", *args, **kwargs)


def callback_handler(*args, **kwargs):
    """
    Decorator that marks function as callback handler
    """
    return _mark_method("is_callback_handler", *args, **kwargs)


def raw_handler(*updates: TLObject):
    """
    Decorator that marks function as raw telethon events handler
    Use it to prevent zombie-event-handlers, left by unloaded modules
    :param updates: Update(-s) to handle
    ⚠️ Do not try to simulate behavior of this decorator by yourself!
    ⚠️ This feature won't work, if you dynamically declare method with decorator!
    """

    def inner(func: Command) -> Command:
        func.is_raw_handler = True
        func.updates = updates
        func.id = uuid4().hex
        return func

    return inner


class Modules:
    """Stores all registered modules"""

    def __init__(
        self,
        client: "CustomTelegramClient",  # type: ignore  # noqa: F821
        db: Database,
        allclients: list,
    ):
        self._initial_registration = True
        self.commands = {}
        self.callback_handlers = {}
        self.modules = []  # skipcq: PTC-W0052
        self.libraries = []
        self.watchers = []
        self._log_handlers = []
        self._core_commands = []
        self.__approve = []
        self.allclients = allclients
        self.client = client
        self._db = db
        self.db = db
        self.secure_boot = False
        asyncio.ensure_future(self._junk_collector())

    async def _junk_collector(self):
        """
        Periodically reloads commands, callback handlers and watchers from loaded
        modules to prevent zombie handlers
        """
        while True:
            await asyncio.sleep(60)
            commands = {}
            callback_handlers = {}
            watchers = []
            for module in self.modules:
                commands.update(module.hikka_commands)
                callback_handlers.update(module.hikka_callback_handlers)
                watchers.extend(module.hikka_watchers.values())

            self.commands = commands
            self.callback_handlers = callback_handlers

            self.watchers = watchers

    async def register_all(
        self,
        mods: typing.Optional[typing.List[str]] = None,
        no_external: bool = False,
    ) -> typing.List[Module]:
        """Load all modules in the module directory"""
        external_mods = []

        if not mods:
            mods = [
                os.path.join(utils.get_base_dir(), MODULES_NAME, mod)
                for mod in filter(
                    lambda x: (x.endswith(".py") and not x.startswith("_")),
                    os.listdir(os.path.join(utils.get_base_dir(), MODULES_NAME)),
                )
            ]

            self.secure_boot = self._db.get(__name__, "secure_boot", False)

            external_mods = (
                []
                if self.secure_boot
                else [
                    (LOADED_MODULES_PATH / mod).resolve()
                    for mod in filter(
                        lambda x: (
                            x.endswith(f"{self.client.tg_id}.py")
                            and not x.startswith("_")
                        ),
                        os.listdir(LOADED_MODULES_DIR),
                    )
                ]
            )

        loaded = []
        loaded += await self._register_modules(mods)

        if not no_external:
            loaded += await self._register_modules(external_mods, "<file>")

        return loaded

    async def _register_modules(
        self,
        modules: list,
        origin: str = "<core>",
    ) -> typing.List[Module]:
        loaded = []

        for mod in modules:
            try:
                mod_shortname = os.path.basename(mod).rsplit(".py", maxsplit=1)[0]
                module_name = f"{__package__}.{MODULES_NAME}.{mod_shortname}"
                user_friendly_origin = (
                    "<core {}>" if origin == "<core>" else "<file {}>"
                ).format(module_name)

                spec = importlib.machinery.ModuleSpec(
                    module_name,
                    StringLoader(Path(mod).read_text(), user_friendly_origin),
                    origin=user_friendly_origin,
                )

                loaded += [await self.register_module(spec, module_name, origin)]
            except Exception as e:
                logger.exception("Failed to load module %s due to %s:", mod, e)

        return loaded

    async def register_module(
        self,
        spec: importlib.machinery.ModuleSpec,
        module_name: str,
        origin: str = "<core>",
        save_fs: bool = False,
    ) -> Module:
        """Register single module from importlib spec"""
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        ret = None

        ret = next(
            (
                value()
                for value in vars(module).values()
                if inspect.isclass(value) and issubclass(value, Module)
            ),
            None,
        )

        if hasattr(module, "__version__"):
            ret.__version__ = module.__version__

        if ret is None:
            ret = module.register(module_name)
            if not isinstance(ret, Module):
                raise TypeError(f"Instance is not a Module, it is {type(ret)}")

        await self.complete_registration(ret)
        ret.__origin__ = origin

        cls_name = ret.__class__.__name__

        if save_fs:
            path = os.path.join(
                LOADED_MODULES_DIR,
                f"{cls_name}_{self.client.tg_id}.py",
            )

            if origin == "<string>":
                Path(path).write_text(spec.loader.data.decode())

        return ret

    def register_raw_handlers(self, instance: Module):
        """Register event handlers for a module"""
        for name, handler in utils.iter_attrs(instance):
            if getattr(handler, "is_raw_handler", False):
                self.client.dispatcher.raw_handlers.append(handler)

    def register_commands(self, instance: Module):
        """Register commands from instance"""
        if instance.__origin__.startswith("<core"):
            self._core_commands += list(
                map(lambda x: x.lower(), list(instance.hikka_commands))
            )

        for _command, cmd in instance.hikka_commands.items():
            if (
                _command.lower() in self._core_commands
                and not instance.__origin__.startswith("<core")
            ):
                with contextlib.suppress(Exception):
                    self.modules.remove(instance)

                raise CoreOverwriteError(command=_command)

            self.commands.update({_command.lower(): cmd})

    def register_watchers(self, instance: Module):
        """Register watcher from instance"""
        for _watcher in self.watchers:
            if _watcher.__self__.__class__.__name__ == instance.__class__.__name__:
                self.watchers.remove(_watcher)

        for _watcher in instance.hikka_watchers.values():
            self.watchers += [_watcher]

    def lookup(
        self,
        modname: str,
    ) -> typing.Union[bool, Module, Library]:
        return next(
            (lib for lib in self.libraries if lib.name.lower() == modname.lower()),
            False,
        ) or next(
            (
                mod
                for mod in self.modules
                if mod.__class__.__name__.lower() == modname.lower()
                or mod.name.lower() == modname.lower()
            ),
            False,
        )

    @property
    def get_approved_channel(self):
        return self.__approve.pop(0) if self.__approve else None

    def get_prefix(self) -> str:
        """Get command prefix"""
        from . import main

        key = main.__name__
        default = "."

        return self._db.get(key, "command_prefix", default)

    async def complete_registration(self, instance: Module):
        """Complete registration of instance"""
        instance.allmodules = self
        instance.internal_init()

        for module in self.modules:
            if module.__class__.__name__ == instance.__class__.__name__:
                if module.__origin__.startswith(
                    "<core"
                ):
                    raise CoreOverwriteError(
                        module=(
                            module.__class__.__name__[:-3]
                            if module.__class__.__name__.endswith("Mod")
                            else module.__class__.__name__
                        )
                    )

                await module.on_unload()

                self.modules.remove(module)
                for _, method in utils.iter_attrs(module):
                    if isinstance(method, InfiniteLoop):
                        method.stop()

        self.modules += [instance]

    def dispatch(self, _command: str) -> typing.Tuple[str, typing.Optional[str]]:
        """Dispatch command to appropriate module"""
        cmd_lower = _command.lower()
        return (
            (cmd_lower, self.commands[cmd_lower])
            if cmd_lower in self.commands
            else (_command, None)
        )

    def send_config(self, skip_hook: bool = False):
        """Configure modules"""
        for mod in self.modules:
            self.send_config_one(mod, skip_hook)

    def send_config_one(self, mod: Module, skip_hook: bool = False):
        """Send config to single instance"""
        if hasattr(mod, "config"):
            modcfg = self._db.get(
                mod.__class__.__name__,
                "__config__",
                {},
            )
            try:
                for conf in mod.config:
                    with contextlib.suppress(validators.ValidationError):
                        mod.config.set_no_raise(
                            conf,
                            (
                                modcfg[conf]
                                if conf in modcfg
                                else os.environ.get(f"{mod.__class__.__name__}.{conf}")
                                or mod.config.getdef(conf)
                            ),
                        )
            except AttributeError:
                logger.warning(
                    "Got invalid config instance. Expected `ModuleConfig`, got %s, %s",
                    type(mod.config),
                    mod.config,
                )

        if not hasattr(mod, "name"):
            mod.name = getattr(mod, 'name', mod.__class__.__name__)

        if skip_hook:
            return

        try:
            mod.config_complete()
        except Exception as e:
            logger.exception("Failed to send mod config complete signal due to %s", e)
            raise

    async def send_ready_one_wrapper(self, *args, **kwargs):
        """Wrapper for send_ready_one"""
        try:
            await self.send_ready_one(*args, **kwargs)
        except Exception as e:
            logger.exception("Failed to send mod init complete signal due to %s", e)

    async def send_ready(self):
        """Send all data to all modules"""
        await asyncio.gather(
            *[self.send_ready_one_wrapper(mod) for mod in self.modules]
        )

    async def send_ready_one(
        self,
        mod: Module,
        no_self_unload: bool = False,
        from_dlmod: bool = False,
    ):
        if from_dlmod:
            try:
                if len(inspect.signature(mod.on_dlmod).parameters) == 2:
                    await mod.on_dlmod(self.client, self._db)
                else:
                    await mod.on_dlmod()
            except Exception:
                logger.info("Can't process `on_dlmod` hook", exc_info=True)

        try:
            if len(inspect.signature(mod.client_ready).parameters) == 2:
                await mod.client_ready(self.client, self._db)
            else:
                await mod.client_ready()
        except SelfUnload as e:
            if no_self_unload:
                raise e

            self.modules.remove(mod)
        except SelfSuspend as e:
            if no_self_unload:
                raise e
            return
        except Exception as e:
            logger.exception(
                (
                    "Failed to send mod init complete signal for %s due to %s,"
                    " attempting unload"
                ),
                mod,
                e,
            )
            self.modules.remove(mod)
            raise

        for _, method in utils.iter_attrs(mod):
            if isinstance(method, InfiniteLoop):
                setattr(method, "module_instance", mod)

                if method.autostart:
                    method.start()

        self.unregister_commands(mod)
        self.unregister_raw_handlers(mod)

        self.register_commands(mod)
        self.register_watchers(mod)
        self.register_raw_handlers(mod)

    def get_classname(self, name: str) -> str:
        return next(
            (
                module.__class__.__module__
                for module in reversed(self.modules)
                if name in (module.name, module.__class__.__module__)
            ),
            name,
        )

    def unregister_loops(self, instance: Module):
        for name, method in utils.iter_attrs(instance):
            if isinstance(method, InfiniteLoop):
                method.stop()

    def unregister_commands(self, instance: Module):
        for name, cmd in self.commands.copy().items():
            if cmd.__self__.__class__.__name__ == instance.__class__.__name__:
                del self.commands[name]

    def unregister_raw_handlers(self, instance: Module):
        """Unregister event handlers for a module"""
        for handler in self.client.dispatcher.raw_handlers:
            if handler.__self__.__class__.__name__ == instance.__class__.__name__:
                self.client.dispatcher.raw_handlers.remove(handler)
