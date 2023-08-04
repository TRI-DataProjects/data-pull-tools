import sys
from collections.abc import Iterable
from typing import Self, SupportsIndex, overload

from data_pull_tools.console_utils.TimedMessageUtils import (
    Initable,
    TimedMessageLike,
    make_message_like,
)


def _make_message_list(items: Iterable[Initable]) -> list[TimedMessageLike]:
    return [make_message_like(item) for item in items]


class TimedMessageList:
    messages: list[TimedMessageLike]

    def __init__(
        self,
        messages: Iterable[Initable],
    ) -> None:
        self.messages = _make_message_list(messages)

    def __repr__(self) -> str:
        if len(self.messages) == 0:
            return f"{self.__class__.__name__}([])"
        if len(self.messages) == 1:
            return f"{self.__class__.__name__}([{repr(self.messages[0])}])"
        sep = ",\n\t"
        return (
            f"{self.__class__.__name__}([\n"
            f"\t{sep.join([repr(x) for x in self.messages])}\n"
            "])"
        )

    def __str__(self) -> str:
        return f"[{', '.join([repr(str(x)) for x in self.messages])}]"

    def __add__(self, other: Self) -> Self:
        return TimedMessageList(self.messages + other.messages)

    @overload
    def __getitem__(self, key: SupportsIndex) -> Self:
        pass

    @overload
    def __getitem__(self, key: slice) -> Self:
        pass

    def __getitem__(self, key: SupportsIndex | slice) -> Self:
        messages = self.messages[key]
        if isinstance(messages, list):
            return TimedMessageList(messages)
        return TimedMessageList([messages])

    @overload
    def __setitem__(self, key: SupportsIndex, value: Initable) -> None:
        pass

    @overload
    def __setitem__(self, key: slice, value: Iterable[Initable]) -> None:
        pass

    def __setitem__(self, key: SupportsIndex | slice, value) -> None:
        if isinstance(key, slice):
            self.messages[key] = _make_message_list(value)
        else:
            self.messages[key] = make_message_like(value)

    def __delitem__(self, key: SupportsIndex | slice) -> None:
        del self.messages[key]

    def append(self, value: Initable) -> None:
        self.insert(len(self.messages), make_message_like(value))

    def extend(self, iterable: Iterable[Initable]) -> None:
        self.messages.extend(_make_message_list(iterable))

    def insert(self, index: SupportsIndex, value: Initable) -> None:
        self.messages.insert(index, make_message_like(value))

    def remove(self, value: TimedMessageLike) -> None:
        self.messages.remove(value)

    def pop(self, index: SupportsIndex = -1) -> TimedMessageLike:
        return self.messages.pop(index)

    def clear(self) -> None:
        self.messages.clear()

    def index(
        self,
        value: TimedMessageLike,
        start: SupportsIndex = 0,
        stop: SupportsIndex = sys.maxsize,
    ) -> int:
        return self.messages.index(value, start, stop)

    def count(self, value: TimedMessageLike) -> int:
        return self.messages.count(value)

    def copy(self) -> Self:
        return TimedMessageList(self.messages)

    def print_messages(self) -> None:
        skipping = False
        for message in self.messages:
            skipping = message.print(skipping)


if __name__ == "__main__":
    from rich.text import Text

    from data_pull_tools.console_utils.TimedMessageUtils import slow_input

    while True:
        TimedMessageList(
            [
                Text.assemble(
                    "If the ",
                    ("quick", "italic"),
                    " ",
                    ("brown fox", "red underline"),
                ),
                Text.assemble(
                    ("jumps", "italic"),
                    " over the ",
                    ("lazy dog", "cyan underline"),
                    ",",
                ),
                Text.assemble(
                    "why did the ",
                    ("chicken", "yellow underline"),
                ),
                (
                    Text.assemble(
                        ("cross", "italic"),
                        " the ",
                        ("road", "green underline"),
                        "? ",
                    ),
                    None,
                    None,
                    "",
                ),
            ]
        ).print_messages()
        response = input()

        x = TimedMessageList(
            [
                f"'{response}'?",
                ("Curious", None, 0, ""),
                (" . . .", 0.5, 2, ""),
                ("\nNerd", 0),
            ]
        )

        print(x)
        x.print_messages()

        if not slow_input("Play again? ", 0.125).lower().startswith("y"):
            x[1:-1].print_messages()
            break
