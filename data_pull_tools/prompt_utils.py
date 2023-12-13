from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

from rich.prompt import InvalidResponse, PromptBase, PromptType
from rich.text import Text, TextType

if TYPE_CHECKING:
    from collections.abc import Callable

    from rich.console import Console


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


class MultilinePrompt(PromptBase[PromptType]):
    post_prompt: TextType = "Enter your selection"
    row_prefix: TextType = ""

    def make_prompt(self, default: PromptType) -> Text:
        prompt = self.prompt.copy()
        prompt.end = ""

        if self.show_choices and self.choices:
            row_prefix = Text("\n").append(self.row_prefix)
            _choices = [Text(choice, "prompt.choices") for choice in self.choices]
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


class SynonymPrompt(MultilinePrompt[str]):
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
        cls: type[SynonymPrompt],
        *,
        prompt: TextType = "",
        choices: dict[str, str],
        console: Console | None = None,
        password: bool = False,
        show_default: bool = True,
        show_choices: bool = True,
        default: str = ...,
        stream: TextIO | None = None,
    ) -> str:
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
        if self.choice_keys is None:
            msg = "Expected a list of valid choices but none were provided."
            raise ValueError(msg)
        return value.strip().lower() in map(str.lower, self.choice_keys)


class ExecutablePrompt(MultilinePrompt[None]):
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
        cls: type[ExecutablePrompt],
        *,
        prompt: TextType = "",
        choices: list[ExecutableOption],
        console: Console | None = None,
        password: bool = False,
        show_default: bool = True,
        show_choices: bool = True,
        default: None = ...,
        stream: TextIO | None = None,
    ) -> None:
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
        if self.action_dict.keys() is None:
            msg = "Expected a list of valid actions but none were provided."
            raise ValueError(msg)
        return value in self.action_dict

    def process_response(self, value: str) -> bool:
        value = value.strip().upper()

        if self.choices is not None and not self.check_choice(value):
            raise InvalidResponse(self.illegal_choice_message)

        action = self.action_dict[value]
        if action is not None:
            action()

        return self.exits_dict[value]

    def __call__(self, *, default: None = ..., stream: TextIO | None = None) -> None:
        """Run the prompt loop.

        Args:
            default (Any, optional): Optional default value.

        Returns
        -------
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
        cls: type[PathPrompt],
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

        if not return_value.exists():
            raise InvalidResponse(self.validate_error_message)

        if self.choices is not None and not self.check_choice(value):
            raise InvalidResponse(self.illegal_choice_message)

        return return_value


class FilePrompt(PathPrompt):
    """A prompt for a file path."""

    validate_error_message = "[prompt.invalid]Please enter a valid file path"

    def process_response(self, value: str) -> Path:
        return_value = super().process_response(value)

        if not return_value.is_file():
            raise InvalidResponse(self.validate_error_message)

        return return_value


class DirPrompt(PathPrompt):
    """A prompt for a directory path."""

    validate_error_message = "[prompt.invalid]Please enter a valid directory path"

    def process_response(self, value: str) -> Path:
        return_value = super().process_response(value)

        if not return_value.is_dir():
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

        dir_names = [entry.name for entry in os.scandir(root) if entry.is_dir()]

        if len(dir_names) == 0:
            msg = f"No subfolders exist for: {root}"
            raise FileNotFoundError(msg)

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
        cls: type[MultilineSubdirPrompt],
        prompt: TextType = "",
        root: Path | None = None,
        *,
        console: Console | None = None,
        password: bool = False,
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

        file_names = [entry.name for entry in os.scandir(root) if entry.is_file()]

        if len(file_names) == 0:
            msg = f"No files exist in dir: {root}"
            raise FileNotFoundError(msg)

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
        cls: type[MultilineSubfilePrompt],
        prompt: TextType = "",
        root: Path | None = None,
        *,
        console: Console | None = None,
        password: bool = False,
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
            show_default=show_default,
            show_choices=show_choices,
        )
        return _prompt(default=default, stream=stream)
