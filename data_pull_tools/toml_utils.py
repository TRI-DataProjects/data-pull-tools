import os
import tomllib
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, TypeAlias

import tomlkit

StrPath: TypeAlias = str | os.PathLike[str]


@contextmanager
def ManageTOMLFile(path: StrPath | Path):
    with open(path, mode="r+") as f:
        doc = tomlkit.load(f)
        yield doc
        f.seek(0)
        tomlkit.dump(doc, f)
        f.truncate()


def LoadTOML(path: StrPath) -> dict[str, Any]:
    if not isinstance(path, Path):
        path = Path(path)
    with path.open(mode="rb") as f:
        return tomllib.load(f)


@dataclass
class CachedTOML:
    toml: dict[str, Any]
    loaded: float


class CachedTOMLReader:
    _cache: dict[Path, CachedTOML] = dict()

    def load(self, path: StrPath) -> dict[str, Any]:
        if not isinstance(path, Path):
            path = Path(path)

        if not path.exists():
            raise ValueError(f"Provided path '{path}' does not exist.")

        last_modified = os.path.getmtime(path)

        # Reuse the cache if we can
        if path in self._cache and self._cache[path].loaded > last_modified:
            return self._cache[path].toml

        # Populate the cache when we need to
        toml_data = LoadTOML(path)
        self._cache[path] = CachedTOML(toml_data, datetime.now().timestamp())
        return toml_data
