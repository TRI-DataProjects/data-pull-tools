from pathlib import Path
from subprocess import PIPE, CompletedProcess, run
from typing import Iterable


class NotAGitProjectError(Exception):
    pass


def is_git_proj(path: Path | str) -> bool:
    if isinstance(path, str):
        path = Path(path)
    if not path.is_dir():
        raise NotADirectoryError(f"Path provided is not a directory: '{path}'")
    git_path = path / ".git"
    return git_path.exists()


def proj_has_changes(path: Path | str) -> bool:
    if not is_git_proj(path):
        raise NotAGitProjectError(f"Path provided is not a git project: '{path}'")
    result = run_git(["status", "-s"], path)
    return result.stdout != ""


def run_git(
    args: Iterable,
    c_dir: Path | str | None = None,
    *,
    capture_output: bool = True,
    check: bool = True,
) -> CompletedProcess[str]:
    """Run a git command with some set of `args`

    Args:
        args (list): Arguments to be passed to `git` process call.
        c_dir (Path | None, optional): Directory to execute `git` from via `-C` flag. Defaults to None.
        capture_output (bool, optional): Capture stdout and stderr streams into returned `CompletedProcess` object. Defaults to True.
        check (bool, optional): Should this function check the process exit code and raise a `CalledProcessError` when it is non-zero. Defaults to True.

    Returns:
        CompletedProcess[str]: The result of executing the git process and args with `subprocess.run()`.
    """
    g_args = ["git"]

    if c_dir is not None:
        if isinstance(c_dir, str):
            c_dir = Path(c_dir)
        if not c_dir.is_dir():
            raise NotADirectoryError(f"Path provided is not a directory: '{c_dir}'")
        g_args += ["-C", c_dir]

    g_args += args

    return run(
        g_args,
        encoding="utf-8",
        capture_output=capture_output,
        check=check,
    )


if __name__ == "__main__":
    import os

    root = Path(__file__).parent.absolute()

    try:
        # Print state of current project
        has_changes = proj_has_changes(root)
        msg = "has changes!" if has_changes else "is up to date!"
        print(f"Project '{root.name}' {msg}")
    except Exception as e:
        print(e)

    try:
        # Expect __file__ not to be a dir to print error example
        has_changes = proj_has_changes(Path(__file__))
    except Exception as e:
        print(e)

    home_directory = os.path.expanduser("~")

    try:
        # Expect home_directory not to be a git dir to print error example
        has_changes = proj_has_changes(Path(home_directory))
    except Exception as e:
        print(e)

    try:
        # Example usage of run_git
        result = run_git(["status", "-s"], root)
        print(result.stdout.rstrip())
    except Exception as e:
        print(e)
