import logging
from enum import Enum
from functools import total_ordering

from rich.console import Console
from rich.logging import RichHandler
from rich.style import Style


@total_ordering
class MessageLevel(Enum):
    CRITICAL = (50, "bold bright_magenta")
    ERROR = (40, "bright_red")
    WARNING = (30, "bright_yellow")
    INFO = (20, "bright_green")
    DEBUG = (10, "bright_cyan")

    def __lt__(self, other) -> bool:
        if isinstance(other, MessageLevel):
            return self.val < other.val
        else:
            raise TypeError(
                "Operator '<' not supported between instances of "
                f"{type(self)} and {type(other)}"
            )

    @property
    def val(self) -> int:
        return self.value[0]

    @property
    def style(self) -> str:
        return self.value[1]


class ConsoleLogger:
    def __init__(
        self,
        log_file_path: str,
        *,
        print_level: MessageLevel = MessageLevel.WARNING,
        log_level: MessageLevel = MessageLevel.DEBUG,
    ) -> None:
        log_console = Console(
            file=open(log_file_path, "w", encoding="utf-8"), width=120
        )

        FORMAT = "%(message)s"
        logging.basicConfig(
            level="NOTSET",
            format=FORMAT,
            datefmt="[%X]",
            handlers=[
                RichHandler(console=log_console, rich_tracebacks=True),
            ],
        )

        self.console = Console()
        self.logger = logging.getLogger("rich")
        self.print_level = print_level
        self.log_level = log_level

    def print(self, msg: str = "", style: str | Style | None = None) -> None:
        self.console.print(msg, style=style)

    def log(self, msg: str, message_level: MessageLevel):
        if message_level == MessageLevel.CRITICAL:
            return self.logger.critical(msg)
        elif message_level == MessageLevel.ERROR:
            return self.logger.error(msg)
        elif message_level == MessageLevel.WARNING:
            return self.logger.warning(msg)
        elif message_level == MessageLevel.INFO:
            return self.logger.info(msg)
        elif message_level == MessageLevel.DEBUG:
            return self.logger.debug(msg)
        else:
            raise NotImplementedError(
                f"No support for logging MessageLevel '{message_level}'"
            )

    def _print_log(self, msg: str, message_level: MessageLevel):
        if self.print_level <= message_level:
            self.print(msg, style=message_level.style)
        if self.log_level <= message_level:
            self.log(msg, message_level)

    def critical(self, msg: str) -> None:
        self._print_log(msg, MessageLevel.CRITICAL)

    def error(self, msg: str) -> None:
        self._print_log(msg, MessageLevel.ERROR)

    def warning(self, msg: str) -> None:
        self._print_log(msg, MessageLevel.WARNING)

    def info(self, msg: str) -> None:
        self._print_log(msg, MessageLevel.INFO)

    def debug(self, msg: str) -> None:
        self._print_log(msg, MessageLevel.DEBUG)
