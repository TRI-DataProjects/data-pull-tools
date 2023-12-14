from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TypeVar

K = TypeVar("K")
V = TypeVar("V")
RecursiveMap = Mapping[K, V | "RecursiveMap[K, V]"]


def traverse_mapping(
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
