from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING

import tomlkit
from tomlkit import TOMLDocument

if TYPE_CHECKING:
    from collections.abc import Generator
    from typing import Any, TypeAlias

StrPath: TypeAlias = str | PathLike[str]


@contextmanager
def manage_toml_file(
    path: StrPath | Path,
) -> Generator[TOMLDocument, Any, None]:
    path = Path(path)
    with path.open(mode="r+") as f:
        doc = tomlkit.load(f)
        yield doc
        f.seek(0)
        tomlkit.dump(doc, f)
        f.truncate()


def load_toml(
    path: StrPath | Path,
) -> TOMLDocument:
    path = Path(path)
    with path.open(mode="rb") as f:
        return tomlkit.load(f)


@dataclass
class CachedTOML:
    toml: TOMLDocument
    loaded: float


class CachedTOMLReader:
    _cache: dict[Path, CachedTOML]

    def __init__(self) -> None:
        self._cache = {}

    def load(self, path: StrPath) -> dict[str, Any]:
        if not isinstance(path, Path):
            path = Path(path)

        if not path.exists():
            msg = f"Provided path '{path}' does not exist."
            raise ValueError(msg)

        last_modified = path.stat().st_mtime

        # Reuse the cache if we can
        if path in self._cache and self._cache[path].loaded > last_modified:
            return self._cache[path].toml

        # Populate the cache when we need to
        toml_data = load_toml(path)
        self._cache[path] = CachedTOML(
            toml_data,
            datetime.now(tz=timezone.utc).timestamp(),
        )
        return toml_data
