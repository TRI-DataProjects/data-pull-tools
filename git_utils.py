from pathlib import Path
from subprocess import PIPE, run


class NotAGitProjectError(Exception):
    pass


def is_git_proj(path: Path) -> bool:
    if not path.is_dir():
        raise NotADirectoryError(f"Path provided is not a directory: '{path}'")
    git_path = path / ".git"
    return git_path.exists()


def proj_has_changes(path: Path) -> bool:
    if not is_git_proj(path):
        raise NotAGitProjectError(f"Path provided is not a git project: '{path}'")
    result = run(
        ["git", "-C", path, "status", "-s"],
        stdout=PIPE,
        encoding="utf-8",
    )
    return result.stdout != ""


if __name__ == "__main__":
    import os

    root = Path(__file__).parent.absolute()

    try:
        has_changes = proj_has_changes(root)
        msg = "has changes!" if has_changes else "is up to date!"
        print(f"Project '{root.name}' {msg}")
    except Exception as e:
        print(e)

    try:
        has_changes = proj_has_changes(Path(__file__))
    except Exception as e:
        print(e)

    home_directory = os.path.expanduser("~")

    try:
        has_changes = proj_has_changes(Path(home_directory))
    except Exception as e:
        print(e)
