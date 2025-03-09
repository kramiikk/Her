import asyncio
import contextlib
import importlib
import importlib.machinery
import importlib.util
import inspect
import logging
import os
import sys
import typing
from pathlib import Path
from types import FunctionType
from uuid import uuid4

from hikkatl.tl.tlobject import TLObject

from . import utils, validators
from .database import Database
from .types import (
    Command,
    ConfigValue,
    JSONSerializable,
    LoadError,
    Module,
    ModuleConfig,
    SelfSuspend,
    SelfUnload,
    StringLoader,
    get_callback_handlers,
    get_commands,
)

__all__ = [
    "Modules",
    "InfiniteLoop",
    "Command",
    "JSONSerializable",
    "LoadError",
    "Module",
    "SelfSuspend",
    "SelfUnload",
    "StringLoader",
    "get_commands",
    "get_callback_handlers",
    "validators",
    "Database",
    "ConfigValue",
    "ModuleConfig",
    "loop",
]

logger = logging.getLogger(__name__)


async def stop_placeholder() -> bool:
    return True


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
        while not self.module_instance:
            await asyncio.sleep(1)
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
        session,
        db: Database,
    ):
        self._initial_registration = True
        self.commands = {}
        self.callback_handlers = {}
        self.modules = []  # skipcq: PTC-W0052
        self.watchers = []
        self.raw_handlers = []
        self.client = client
        self.session = session
        self.db = db
        asyncio.ensure_future(self._junk_collector())

    async def _junk_collector(self):
        """
        Periodically reloads commands, callback handlers and watchers from loaded
        modules to prevent zombie handlers
        """
        last_modules = set()
        while True:
            await asyncio.sleep(5)
            if {id(m) for m in self.modules} != last_modules:
                last_modules = {id(m) for m in self.modules}
                commands = {}
                callback_handlers = {}
                watchers = []
                for module in self.modules:
                    commands.update(module.her_commands)
                    callback_handlers.update(module.her_callback_handlers)
                    watchers.extend(module.her_watchers.values())
                self.commands = commands
                self.callback_handlers = callback_handlers

                self.watchers = watchers

    async def register_all(self) -> typing.List[Module]:
        """Load all modules in the module directory"""
        mods = [
            os.path.join(utils.get_base_dir(), "modules", mod)
            for mod in filter(
                lambda x: (x.endswith(".py") and not x.startswith("_")),
                os.listdir(os.path.join(utils.get_base_dir(), "modules")),
            )
        ]
        loaded = []
        loaded += await self._register_modules(mods)

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
                module_name = f"{__package__}.{"modules"}.{mod_shortname}"
                user_friendly_origin = ("<core {}>").format(module_name)

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

        if ret is None:
            ret = module.register(module_name)
            if not isinstance(ret, Module):
                raise TypeError(f"Instance is not a Module, it is {type(ret)}")
        await self.complete_registration(ret)
        ret.__origin__ = origin

        return ret

    def register_raw_handlers(self, instance: Module):
        instance.raw_handlers = []
        for name, handler in utils.iter_attrs(instance):
            if getattr(handler, "is_raw_handler", False):
                self.client.dispatcher.raw_handlers.append(handler)
                instance.raw_handlers.append(handler)

    def register_commands(self, instance: Module):
        """Register commands from instance"""
        for _command, cmd in instance.her_commands.items():
            self.commands.update({_command.lower(): cmd})

    def register_watchers(self, instance: Module):
        """Register watcher from instance"""
        for _watcher in self.watchers:
            if _watcher.__self__.__class__.__name__ == instance.__class__.__name__:
                self.watchers.remove(_watcher)
        for _watcher in instance.her_watchers.values():
            self.watchers += [_watcher]

    async def complete_registration(self, instance: Module):
        """Complete registration of instance"""
        instance.allmodules = self
        instance.internal_init()

        for module in self.modules:
            if module.__class__.__name__ == instance.__class__.__name__:
                if hasattr(module, "close"):
                    await module.close()
                await module.on_unload()
                for handler in module.raw_handlers:
                    with contextlib.suppress(ValueError):
                        self.client.dispatcher.raw_handlers.remove(handler)
                self.modules.remove(module)
                for _, method in utils.iter_attrs(module):
                    if isinstance(method, InfiniteLoop):
                        await method.stop()
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
            modcfg = self.db.get(
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
            mod.name = getattr(mod, "name", mod.__class__.__name__)
        if skip_hook:
            return
        try:
            mod.config_complete()
        except Exception as e:
            logger.exception("Failed to send mod config complete signal due to %s", e)
            raise

    async def send_ready_one_wrapper(self, mod):
        """Wrapper for send_ready_one"""
        try:
            await self.send_ready_one(mod)
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
    ):
        try:
            if len(inspect.signature(mod.client_ready).parameters) == 2:
                await mod.client_ready(self.client, self.db)
            else:
                await mod.client_ready()
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
        self.register_commands(mod)
        self.register_watchers(mod)
        self.register_raw_handlers(mod)
