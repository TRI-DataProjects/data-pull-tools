"""A module for various miscellaneous file operations."""
import logging
import os
import shutil
from pathlib import Path

if os.name == "nt":
    from ctypes import WinError, windll

    from win32con import FILE_ATTRIBUTE_HIDDEN

module_logger = logging.getLogger(__name__)


def hide_file(path: Path) -> Path:
    """
    Hides the file/folder specified by `path`, modifying the name if necessary for the
    system.

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
    path = path.resolve()
    if not path.name.startswith("."):
        new_path = path.parent / ("." + path.name)
        path.rename(new_path)

    # Set file attributes on win machines
    if os.name == "nt" and not windll.kernel32.SetFileAttributesW(
        str(path.absolute()),
        FILE_ATTRIBUTE_HIDDEN,
    ):
        raise WinError()

    return path


def clear_dir(path: Path) -> None:
    """
    Remove all contents from a directory.

    Parameters
    ----------
    path : Path
        The directory to clear.

    Raises
    ------
    TypeError
        When `path` is not an existing directory.
    """
    if not (path.exists() and path.is_dir()):
        msg = "'path' must be an existing directory."
        raise TypeError(msg)

    for entry in path.iterdir():
        try:
            if entry.is_file() or entry.is_symlink():
                entry.unlink()
            elif entry.is_dir():
                shutil.rmtree(entry)
        except Exception as e:  # noqa: PERF203, BLE001
            module_logger.warning("Failed to delete %s.", entry, exc_info=e)
