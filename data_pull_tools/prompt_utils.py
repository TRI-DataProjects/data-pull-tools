import os
from collections.abc import Callable
from pathlib import Path
from typing import Any, TextIO

from rich.console import Console
from rich.prompt import DefaultType, InvalidResponse, PromptBase, PromptType
from rich.text import Text, TextType


class ExecutableOption:
    def __init__(
        self,
        choice_key: str,
        choice_disp: str,
        action: Callable | None,
        exits: bool = False,
    ) -> None:
        self.choice_key = choice_key
        self.choice_disp = choice_disp
        self.action = action
        self.exits = exits

    ...


class MultilinePrompt(PromptBase[PromptType]):
    post_prompt: TextType = "Enter your selection"
    row_prefix: TextType = ""

    def make_prompt(self, default: DefaultType) -> Text:
        """Make prompt text.

        Args:
            default (DefaultType): Default value.

        Returns:
            Text: Text to display in prompt.
        """
        prompt = self.prompt.copy()
        prompt.end = ""

        if self.show_choices and self.choices:
            row_prefix = Text("\n").append(self.row_prefix)
            _choices = list(
                map(
                    lambda choice: Text(choice, "prompt.choices"),
                    self.choices,
                )
            )
            choices = row_prefix.append(row_prefix.join(_choices))
            prompt.append(choices)
            prompt.append("\n")

        prompt.append(self.post_prompt)

        if (
            default != ...
            and self.show_default
            and isinstance(default, (str, self.response_type))
        ):
            prompt.append(" ")
            _default = self.render_default(default)
            prompt.append(_default)

        prompt.append(self.prompt_suffix)

        return prompt


class SynonymPrompt(MultilinePrompt, PromptBase[str]):
    def __init__(
        self,
        *,
        prompt: TextType = "",
        choices: dict[str, str],
        console: Console | None = None,
        password: bool = False,
        show_default: bool = True,
        show_choices: bool = True,
    ) -> None:
        disp_choices = [x + " - " + y for x, y in choices.items()]
        super().__init__(
            prompt,
            console=console,
            password=password,
            choices=disp_choices,
            show_default=show_default,
            show_choices=show_choices,
        )
        self.choice_keys = list(choices.keys())

    @classmethod
    def ask(
        cls,
        *,
        prompt: TextType = "",
        choices: dict[str, str],
        console: Console | None = None,
        password: bool = False,
        show_default: bool = True,
        show_choices: bool = True,
        default: Any = ...,
        stream: TextIO | None = None,
    ) -> Any:
        _prompt = cls(
            prompt=prompt,
            console=console,
            password=password,
            choices=choices,
            show_default=show_default,
            show_choices=show_choices,
        )
        return _prompt(default=default, stream=stream)

    def check_choice(self, value: str) -> bool:
        assert self.choice_keys is not None
        return value.strip().lower() in map(str.lower, self.choice_keys)


class ExecutablePrompt(MultilinePrompt, PromptBase[None]):
    def __init__(
        self,
        *,
        prompt: TextType = "",
        choices: list[ExecutableOption],
        console: Console | None = None,
        password: bool = False,
        show_default: bool = True,
        show_choices: bool = True,
    ) -> None:
        disp_choices = [x.choice_key.upper() + " - " + x.choice_disp for x in choices]
        super().__init__(
            prompt,
            console=console,
            password=password,
            choices=disp_choices,
            show_default=show_default,
            show_choices=show_choices,
        )
        self.exits_dict = {x.choice_key.upper(): x.exits for x in choices}
        self.action_dict = {x.choice_key.upper(): x.action for x in choices}

    @classmethod
    def ask(
        cls,
        *,
        prompt: TextType = "",
        choices: list[ExecutableOption],
        console: Console | None = None,
        password: bool = False,
        show_default: bool = True,
        show_choices: bool = True,
        default: Any = ...,
        stream: TextIO | None = None,
    ) -> Any:
        _prompt = cls(
            prompt=prompt,
            console=console,
            password=password,
            choices=choices,
            show_default=show_default,
            show_choices=show_choices,
        )
        return _prompt(default=default, stream=stream)

    def check_choice(self, value: str) -> bool:
        assert self.action_dict.keys() is not None
        return value in self.action_dict.keys()

    def process_response(self, value: str) -> bool:
        value = value.strip().upper()

        if self.choices is not None and not self.check_choice(value):
            raise InvalidResponse(self.illegal_choice_message)

        action = self.action_dict[value]
        if action is not None:
            action()

        return self.exits_dict[value]

    def __call__(self, *, default: Any = ..., stream: TextIO | None = None) -> None:
        """Run the prompt loop.

        Args:
            default (Any, optional): Optional default value.

        Returns:
            PromptType: Processed value.
        """
        exits = False
        while not exits:
            self.pre_prompt()
            prompt = self.make_prompt(default)
            value = self.get_input(self.console, prompt, self.password, stream=stream)
            if value == "" and default != ...:
                value = default
            try:
                exits = self.process_response(value)
            except InvalidResponse as error:
                self.on_validate_error(value, error)
                continue


##################
# Path Prompters #
##################


class PathPrompt(PromptBase[Path]):
    response_type = Path
    validate_error_message = "[prompt.invalid]Please enter a valid path"

    def __init__(
        self,
        prompt: TextType = "",
        root: Path | None = None,
        *,
        console: Console | None = None,
        password: bool = False,
        choices: list[str] | None = None,
        show_default: bool = True,
        show_choices: bool = True,
    ) -> None:
        if root is None:
            root = Path()

        super().__init__(
            prompt,
            console=console,
            password=password,
            choices=choices,
            show_default=show_default,
            show_choices=show_choices,
        )
        self.root = root

    @classmethod
    def ask(
        cls,
        prompt: TextType = "",
        root: Path | None = None,
        *,
        console: Console | None = None,
        password: bool = False,
        choices: list[str] | None = None,
        show_default: bool = True,
        show_choices: bool = True,
        default: Path = ...,
        stream: TextIO | None = None,
    ) -> Path:
        if root is None:
            root = Path()

        _prompt = cls(
            prompt,
            root,
            console=console,
            password=password,
            choices=choices,
            show_default=show_default,
            show_choices=show_choices,
        )
        return _prompt(default=default, stream=stream)

    def process_response(self, value: str) -> Path:
        return_value = self.root / (value.strip())

        if not os.path.exists(return_value):
            raise InvalidResponse(self.validate_error_message)

        if self.choices is not None and not self.check_choice(value):
            raise InvalidResponse(self.illegal_choice_message)

        return return_value


class FilePrompt(PathPrompt):
    validate_error_message = "[prompt.invalid]Please enter a valid file path"

    def process_response(self, value: str) -> Path:
        return_value = super().process_response(value)

        if not os.path.isfile(return_value):
            raise InvalidResponse(self.validate_error_message)

        return return_value


class DirPrompt(PathPrompt):
    validate_error_message = "[prompt.invalid]Please enter a valid directory path"

    def process_response(self, value: str) -> Path:
        return_value = super().process_response(value)

        if not os.path.isdir(return_value):
            raise InvalidResponse(self.validate_error_message)

        return return_value


class MultilineDirPrompt(DirPrompt, MultilinePrompt):
    row_prefix: TextType = ""


class MultilineSubdirPrompt(MultilineDirPrompt):
    def __init__(
        self,
        prompt: TextType = "",
        root: Path | None = None,
        *,
        console: Console | None = None,
        password: bool = False,
        show_default: bool = True,
        show_choices: bool = True,
    ) -> None:
        if root is None:
            root = Path()

        dir_names = list()
        for entry in os.scandir(root):
            if entry.is_dir():
                dir_names.append(entry.name)

        if len(dir_names) == 0:
            raise FileNotFoundError(f"No subfolders exist for: {root}")

        dir_names.sort()

        super().__init__(
            prompt,
            console=console,
            password=password,
            choices=dir_names,
            show_default=show_default,
            show_choices=show_choices,
        )
        self.root = root

    @classmethod
    def ask(
        cls,
        prompt: TextType = "",
        root: Path | None = None,
        *,
        console: Console | None = None,
        password: bool = False,
        show_default: bool = True,
        show_choices: bool = True,
        default: Any = ...,
        stream: TextIO | None = None,
    ) -> Any:
        if root is None:
            root = Path()

        _prompt = cls(
            prompt,
            root,
            console=console,
            password=password,
            show_default=show_default,
            show_choices=show_choices,
        )
        return _prompt(default=default, stream=stream)


class MultilineFilePrompt(FilePrompt, MultilinePrompt):
    row_prefix: TextType = ""


class MultilineSubfilePrompt(MultilineFilePrompt):
    def __init__(
        self,
        prompt: TextType = "",
        root: Path | None = None,
        *,
        console: Console | None = None,
        password: bool = False,
        show_default: bool = True,
        show_choices: bool = True,
    ) -> None:
        if root is None:
            root = Path()
        file_names = list()
        for entry in os.scandir(root):
            if entry.is_file():
                file_names.append(entry.name)

        if len(file_names) == 0:
            raise FileNotFoundError(f"No files exist in dir: {root}")

        super().__init__(
            prompt,
            console=console,
            password=password,
            choices=file_names,
            show_default=show_default,
            show_choices=show_choices,
        )
        self.root = root

    @classmethod
    def ask(
        cls,
        prompt: TextType = "",
        root: Path | None = None,
        *,
        console: Console | None = None,
        password: bool = False,
        show_default: bool = True,
        show_choices: bool = True,
        default: Any = ...,
        stream: TextIO | None = None,
    ) -> Any:
        if root is None:
            root = Path()
        _prompt = cls(
            prompt,
            root,
            console=console,
            password=password,
            show_default=show_default,
            show_choices=show_choices,
        )
        return _prompt(default=default, stream=stream)
