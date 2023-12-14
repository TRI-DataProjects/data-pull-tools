"""
Utilities for reading and modifying TOML files.

Includes functions to load TOML files, get or create tables, and update values.
"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypeVar

import tomlkit
from tomlkit import TOMLDocument, table
from tomlkit.items import Table

if TYPE_CHECKING:
    from collections.abc import Generator
    from typing import Any, TypeAlias

    from tomlkit.container import Container

StrPath: TypeAlias = str | PathLike[str]


K = TypeVar("K")
V = TypeVar("V")
RecursiveMap = Mapping[K, V | "RecursiveMap[K, V]"]
CollisionPolicy = Literal["raise", "replace"]
DEFAULT_COLLISION_POLICY: CollisionPolicy = "replace"


@contextmanager
def manage_toml_file(
    path: StrPath | Path,
) -> Generator[TOMLDocument, Any, None]:
    """Manages a toml file in a context.
    Loads the file, yields the contents, and then saves when the context exits.

    Parameters
    ----------
    path : StrPath | Path
        The path to the toml file.

    Yields
    ------
    TOMLDocument
        The loaded toml file.
    """
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
    """Loads a toml file into memory.

    Parameters
    ----------
    path : StrPath | Path
        The path to the toml file.

    Returns
    -------
    TOMLDocument
        The loaded toml document.
    """
    path = Path(path)
    with path.open(mode="rb") as f:
        return tomlkit.load(f)


class NonTableKeyCollisionError(Exception):
    ...


def _toml_get_or_table(
    toml: Container | Table,
    key: str,
    *,
    collisions: CollisionPolicy = "replace",
) -> Table:
    """Gets a table from the given key, creating if missing.

    Parameters
    ----------
    toml : Container | Table
        The parent toml object.
    key : str
        The key to get or create the table at.
    collisions : CollisionPolicy, default="replace"
        How to handle collisions, by default "replace".

    Returns
    -------
    Table
        The table at the given key.

    Raises
    ------
    NonTableKeyCollisionError
        If `collisions` is "raise" and the key already exists but is not a table.
    """
    if key not in toml:
        value = table()
        toml.add(key, value)
        return value

    value = toml[key]
    if not isinstance(value, Table):
        # Handle collisions
        if collisions == "raise":
            msg = "Key '%s' already exists and is not a table."
            raise NonTableKeyCollisionError(msg, key)
        if collisions == "replace":
            value = table()
            toml[key] = value
    return value


def update_toml_values(
    toml: Container | Table,
    data: RecursiveMap[str, Any],
    *,
    collisions: CollisionPolicy = "replace",
) -> Container | Table:
    """Updates the given toml values with the provided data.
    Creates intermediary tables as needed.

    Parameters
    ----------
    toml : Container | Table
        The toml object to update.
    data : RecursiveMap[str, Any]
        The data to update the toml with.
    collisions : CollisionPolicy, default="replace"
        How to handle key collisions, by default "replace"

    Returns
    -------
    Container | Table
        The updated toml object.
    """
    getter = partial(_toml_get_or_table, toml=toml, collisions=collisions)
    updater = partial(update_toml_values, collisions=collisions)

    for key, value in data.items():
        if isinstance(value, Mapping):
            updater(getter(key=key), value)
            continue
        toml[key] = value
    return toml


def update_toml_file(
    toml_path: StrPath | Path,
    data: RecursiveMap[str, Any],
    *,
    collisions: CollisionPolicy = "replace",
) -> None:
    """Opens a toml file, updates its contents with the provided data, and saves it.
    Creates intermediary tables as needed.

    Parameters
    ----------
    toml_path : StrPath | Path
        The path to the toml file to update.
    data : RecursiveMap[str, Any]
        The data to update the toml with.
    collisions : CollisionPolicy, default="replace"
        How to handle data key collisions, by default "replace"
    """
    with manage_toml_file(toml_path) as toml_file:
        update_toml_values(toml_file, data, collisions=collisions)


def update_toml_value(
    toml: Container | Table,
    key_chain: list[str],
    value: Any,  # noqa: ANN401
    *,
    collisions: CollisionPolicy = "replace",
) -> Container | Table:
    current = toml
    for key in key_chain[:-1]:
        current = _toml_get_or_table(current, key, collisions=collisions)
    current[key_chain[-1]] = value
    return toml


def update_toml_file_value(
    toml_path: StrPath | Path,
    key_chain: list[str],
    value: Any,  # noqa: ANN401
    *,
    collisions: CollisionPolicy = "replace",
) -> None:
    with manage_toml_file(toml_path) as toml_file:
        update_toml_value(toml_file, key_chain, value, collisions=collisions)


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
