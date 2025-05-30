import inspect
import logging
import traceback
import typing
from logging.handlers import RotatingFileHandler


class HerException:
    def __init__(
        self,
        message: str,
        full_stack: str,
        sysinfo: typing.Optional[
            typing.Tuple[object, Exception, traceback.TracebackException]
        ] = None,
    ):
        self.message = message
        self.full_stack = full_stack
        self.sysinfo = sysinfo


class TelegramLogsHandler(logging.Handler):
    """Keeps 2 buffers"""

    def __init__(self, targets: list, capacity: int):
        super().__init__(0)
        self.buffer = []
        self.handledbuffer = []
        self.targets = targets
        self.capacity = capacity
        self.lvl = logging.NOTSET

    def setLevel(self, level: int):
        self.lvl = level

    def dump(self):
        """Return a list of logging entries"""
        return self.handledbuffer + self.buffer

    def dumps(
        self,
        lvl: int = 0,
        client_id: typing.Optional[int] = None,
    ) -> typing.List[str]:
        """Return all entries of minimum level as list of strings"""
        return [
            self.targets[0].format(record)
            for record in (self.buffer + self.handledbuffer)
            if record.levelno >= lvl
            and (not record.her_caller or client_id == record.her_caller)
        ]

    def emit(self, record: logging.LogRecord):
        try:
            caller = next(
                (
                    frame_info.frame.f_locals["_her_client_id_logging_tag"]
                    for frame_info in inspect.stack()
                    if isinstance(
                        getattr(getattr(frame_info, "frame", None), "f_locals", {}).get(
                            "_her_client_id_logging_tag"
                        ),
                        int,
                    )
                ),
                False,
            )

            if not isinstance(caller, int):
                caller = None
        except Exception:
            caller = None
        record.her_caller = caller

        if len(self.buffer) + len(self.handledbuffer) >= self.capacity:
            if self.handledbuffer:
                del self.handledbuffer[0]
            else:
                del self.buffer[0]
        self.buffer.append(record)

        if record.levelno >= self.lvl >= 0:
            self.acquire()
            try:
                for precord in self.buffer:
                    for target in self.targets:
                        if record.levelno >= target.level:
                            target.handle(precord)
                self.handledbuffer = (
                    self.handledbuffer[-(self.capacity - len(self.buffer)) :]
                    + self.buffer
                )
                self.buffer = []
            finally:
                self.release()


_main_formatter = logging.Formatter(
    fmt="%(levelname)s %(asctime)s %(name)s: %(message)s",
    datefmt="%d %H:%M:%S",
    style="%",
)

rotating_handler = RotatingFileHandler(
    filename="her.log",
    mode="a",
    maxBytes=1 * 1024 * 1024,
    backupCount=1,
    encoding="utf-8",
    delay=0,
)

rotating_handler.setFormatter(_main_formatter)


def init():
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(_main_formatter)
    logging.getLogger().handlers = []
    logging.getLogger().addHandler(
        TelegramLogsHandler((handler, rotating_handler), 7000)
    )
    logging.getLogger("hikkatl").setLevel(logging.WARNING)
    logging.getLogger().setLevel(logging.NOTSET)
    logging.captureWarnings(True)
