from os import PathLike
from pathlib import Path
from subprocess import _CMD, CompletedProcess, run

from _typeshed import StrOrBytesPath


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
    args: _CMD,
    cwd: StrOrBytesPath | None = None,
    *,
    capture_output: bool = True,
    check: bool = True,
) -> CompletedProcess[str]:
    """Run a git command with some set of `args`

    Args:
        args (list): Arguments to be passed to `git` process call.
        cwd (StrOrBytesPath | None, optional): Directory to execute `git` from. When None, use default cwd. Defaults to None.
        capture_output (bool, optional): Capture stdout and stderr streams into returned `CompletedProcess` object. Defaults to True.
        check (bool, optional): Should this function check the process exit code and raise a `CalledProcessError` when it is non-zero. Defaults to True.

    Returns:
        CompletedProcess[str]: The result of executing the git process and args with `subprocess.run()`.
    """
    g_args: list[StrOrBytesPath] = ["git"]
    if isinstance(args, (str, bytes, PathLike)):
        g_args.append(args)
    else:
        g_args.extend(args)

    return run(
        args=g_args,
        cwd=cwd,
        encoding="utf-8",
        capture_output=capture_output,
        check=check,
    )


def run_git_safe_submodule(
    args: _CMD,
    cwd: StrOrBytesPath | None = None,
    *,
    capture_output: bool = True,
    check: bool = True,
) -> CompletedProcess[str]:
    # "protocol.file.allow=always" lets the submodule command clone from a local directory. It's
    # necessary as of Git 2.38.1, where the default was changed to "user" in response to
    # CVE-2022-39253. It isn't a concern here where all repos involved are trusted. For more
    # information, see:
    # https://github.blog/2022-10-18-git-security-vulnerabilities-announced/#cve-2022-39253
    # https://bugs.launchpad.net/ubuntu/+source/git/+bug/1993586
    # https://git-scm.com/docs/git-config#Documentation/git-config.txt-protocolallow
    g_args: list[StrOrBytesPath] = ["-c", "protocol.file.allow=always"]
    if isinstance(args, (str, bytes, PathLike)):
        g_args.append(args)
    else:
        g_args.extend(args)

    return run_git(
        args=g_args,
        cwd=cwd,
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
