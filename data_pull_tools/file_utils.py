"""A module for various miscellaneous file operations."""
from __future__ import annotations

import errno
import logging
import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

if os.name == "nt":
    from ctypes import WinError, windll

    from win32con import FILE_ATTRIBUTE_HIDDEN

if TYPE_CHECKING:
    from os import PathLike

    Pathish = str | PathLike[str] | Path

module_logger = logging.getLogger(__name__)


def make_file_not_found_error(file_not_found: Path) -> FileNotFoundError:
    """Return a FileNotFoundError instance with standard errno and message."""
    return FileNotFoundError(
        errno.ENOENT,
        os.strerror(errno.ENOENT),
        str(file_not_found),
    )


def hide_file(path: Pathish) -> Path:
    """Hides the file/folder specified by `path`, modifying the name if necessary for
    the system.

    Parameters
    ----------
    path : Path
        The file/folder to hide.

    Returns
    -------
    Path
        The final name of the hidden file/folder.

    Raises
    ------
    WinError
        If the file/folder could not be hidden on a Windows system.
    """
    path = Path(path)

    if not path.name.startswith("."):
        new_path = path.parent / ("." + path.name)
        path.rename(new_path)

    # Set file attributes on win machines
    if os.name == "nt" and not windll.kernel32.SetFileAttributesW(  # type: ignore[reportUnboundVariable]
        str(path.absolute()),
        FILE_ATTRIBUTE_HIDDEN,  # type: ignore[reportUnboundVariable]
    ):
        raise WinError()  # type: ignore[reportUnboundVariable]

    return path


def _clear_dir(path: Path) -> None:
    for entry in path.iterdir():
        if entry.is_dir():
            shutil.rmtree(entry)
        else:
            entry.unlink()


def _try_clear_dir(path: Path) -> list[Path]:
    lack_permissions = []
    for entry in path.iterdir():
        try:
            if entry.is_dir():
                shutil.rmtree(entry)
            else:
                entry.unlink()
        except PermissionError as e:
            module_logger.warning("Failed to delete %s.", entry, exc_info=e)
            lack_permissions.append(entry)
    return lack_permissions


def clear_dir(
    path: Pathish,
    must_exist: bool = True,
    must_clear: bool = True,
) -> list[Path]:
    """Permanently remove all contents from a directory.

    Parameters
    ----------
    path: Pathish
        The directory to clear.

    Raises
    ------
    TypeError
        When `path` is not an existing directory.
    """
    path = Path(path)

    if not path.exists():
        if must_exist:
            raise make_file_not_found_error(path)
        return []

    if not path.is_dir():
        msg = "'path' must be a directory."
        raise TypeError(msg)

    if must_clear:
        _clear_dir(path)
        return []

    return _try_clear_dir(path)
