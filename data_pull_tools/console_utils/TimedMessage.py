from abc import ABC, abstractmethod
from time import sleep
from typing import Generic, TypeVar

from rich import print as rprint
from rich.text import Text

DEFAULT_PRINT_RATE = 0.025
DEFAULT_PAUSE_AFTER = 0.5
DEFAULT_END = "\n"
MessageType = TypeVar("MessageType")


class GenericTimedMessage(ABC, Generic[MessageType]):
    def __init__(
        self,
        message: MessageType,
        print_rate: float | None = None,
        pause_after: float | None = None,
        end: str | None = None,
    ) -> None:
        self.message = message
        self._print_rate = print_rate
        self._pause_after = pause_after
        self._end = end

    @property
    def print_rate(self) -> float:
        return DEFAULT_PRINT_RATE if self._print_rate is None else self._print_rate

    @property
    def pause_after(self) -> float:
        return DEFAULT_PAUSE_AFTER if self._pause_after is None else self._pause_after

    @property
    def end(self) -> str:
        return DEFAULT_END if self._end is None else self._end

    @abstractmethod
    def _print_immediate(self) -> None:
        pass

    @abstractmethod
    def _print_slow(self) -> bool:
        pass

    def print(self, skip_wait: bool = False) -> bool:
        if skip_wait or self.print_rate == 0.0:
            self._print_immediate()
            return skip_wait

        try:
            if self._print_slow():
                return True
            if self.pause_after != 0.0:
                sleep(self.pause_after)
        except KeyboardInterrupt:
            return True
        return False

    def __repr__(self) -> str:
        attributes = [self.message, self._print_rate, self._pause_after, self._end]
        last_non_none = None

        for index in range(len(attributes) - 1, -1, -1):
            if attributes[index] is not None:
                last_non_none = index + 1
                break

        if last_non_none is None:
            return f"{self.__class__.__name__}()"

        attributes = ", ".join([repr(x) for x in attributes[0:last_non_none]])

        return f"{self.__class__.__name__}({attributes})"

    @abstractmethod
    def __str__(self) -> str:
        pass


class TimedString(GenericTimedMessage[str]):
    def _print_immediate(self) -> None:
        print(self.message, end=self.end)

    def _print_slow(self) -> bool:
        msg_iter = iter(self.message + self.end)

        try:
            for item in msg_iter:
                print(item, end="", flush=True)
                sleep(self.print_rate)
            return False
        except KeyboardInterrupt:
            print("".join(msg_iter), end="")
            return True

    def __str__(self) -> str:
        return self.message


class TimedText(GenericTimedMessage[Text]):
    def _print_immediate(self) -> None:
        rprint(self.message, end=self.end)

    def _print_slow(self) -> bool:
        msg_iter = iter(self.message + self.end)

        try:
            for item in msg_iter:
                rprint(item, end="")
                sleep(self.print_rate)
            return False
        except KeyboardInterrupt:
            rprint(Text("").join(msg_iter), end="")  # type: ignore
            return True

    def __str__(self) -> str:
        return str(self.message)
