from __future__ import annotations

from typing import TypeVar

from collections.abc import Iterable, Mapping

K = TypeVar("K")
V = TypeVar("V")
RecursiveMap = Mapping[K, V | "RecursiveMap[K, V]"]


def traverse_map(
    dict_in: RecursiveMap[K, V],
    key_chain: Iterable[K],
    default: V | None = None,
) -> RecursiveMap[K, V] | V | None:
    current = dict_in
    for key in key_chain:
        if not isinstance(current, Mapping) or key not in current:
            return default
        current = current[key]

    return current
