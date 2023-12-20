from __future__ import annotations

from enum import Enum, member
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from platformdirs import user_cache_dir

from data_pull_tools.file_utils import hide_file

if TYPE_CHECKING:
    from . import Pathish


def sys_cache_dir(appname: str = "pycaching") -> Path:
    return Path(user_cache_dir(appname, appauthor=False))


DEFAULT_CACHE_DIR = sys_cache_dir()


class ResolveStrategy(Protocol):
    def __call__(
        self,
        root_dir: Path,
        cache_dir: Pathish | None,
    ) -> Path:
        ...


def _resolve_to_system(
    root_dir: Path,  # noqa: ARG001
    cache_dir: Pathish | None,
) -> Path:
    cache_dir = cache_dir or ""
    if isinstance(cache_dir, str):
        cache_dir = DEFAULT_CACHE_DIR / cache_dir
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _resolve_to_root(
    root_dir: Path,
    cache_dir: Pathish | None,
) -> Path:
    cache_dir = cache_dir or ".cache"
    if isinstance(cache_dir, str):
        cache_dir = root_dir / cache_dir
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    return hide_file(cache_dir)


class ResolveStrategyType(Enum):
    """Enumeration of several default cache resolve strategies."""

    def __call__(
        self,
        root_dir: Path,
        cache_dir: Pathish | None,
    ) -> Path:
        """Call the associated resolve function."""
        return self.value(root_dir, cache_dir)  # type: ignore reportGeneralTypeIssues

    RESOLVE_TO_SYSTEM = member(_resolve_to_system)
    RESOLVE_TO_ROOT = member(_resolve_to_root)


DEFAULT_RESOLVE_STRATEGY = ResolveStrategyType.RESOLVE_TO_ROOT
