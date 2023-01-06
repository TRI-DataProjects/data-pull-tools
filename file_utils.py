import os
import shutil
from pathlib import Path

if os.name == "nt":
    from ctypes import WinError, windll

    from win32con import FILE_ATTRIBUTE_HIDDEN


def hide_file(path: Path) -> Path:
    path = path.resolve()
    if not path.name.startswith("."):
        new_path = path.parent / ("." + path.name)
        os.rename(path, new_path)

    if os.name == "nt":
        # Set file attributes on win machines
        if not windll.kernel32.SetFileAttributesW(
            str(path.absolute()),
            FILE_ATTRIBUTE_HIDDEN,
        ):
            raise WinError()

    return path


def clear_dir(path: Path) -> None:
    if path.exists():
        with os.scandir(path) as entries:
            for entry in entries:
                try:
                    if entry.is_file() or entry.is_symlink():
                        os.remove(entry)
                    elif entry.is_dir():
                        shutil.rmtree(entry)
                except Exception as e:
                    print(f"Failed to delete {entry}. Reason: {e}")
