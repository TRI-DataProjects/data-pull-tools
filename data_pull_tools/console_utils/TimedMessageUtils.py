from rich.text import Text

from data_pull_tools.console_utils.TimedMessage import TimedString, TimedText

TimedMessageLike = TimedText | TimedString
TextType = Text | str
Constructable = (
    TextType
    | tuple[TextType]
    | tuple[TextType, float | None]
    | tuple[TextType, float | None, float | None]
    | tuple[TextType, float | None, float | None, str | None]
)
Initable = TimedMessageLike | Constructable


def make_message_like(item: Initable) -> TimedMessageLike:
    if isinstance(item, (TimedText, TimedString)):
        return item
    elif isinstance(item, str):
        return TimedString(item)
    elif isinstance(item, Text):
        return TimedText(item)
    elif isinstance(item, tuple):
        if isinstance(item[0], str):
            return TimedString(*item)  # type: ignore
        elif isinstance(item[0], Text):
            return TimedText(*item)  # type: ignore

    raise TypeError


def slow_input(
    message: TextType,
    print_rate: float | None = None,
    pause_after: float | None = None,
    end: str | None = None,
) -> str:
    end = "" if end is None else end
    make_message_like(
        (
            message,
            print_rate,
            pause_after,
            end,
        )
    ).print()
    return input()
