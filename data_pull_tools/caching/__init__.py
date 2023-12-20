from typing import TYPE_CHECKING

from .cache_manager import CacheManager
from .cache_strategy import (
    DEFAULT_CACHE_STRATEGY,
    CacheStrategy,
    CacheStrategyType,
)
from .cacher import (
    DEFAULT_CACHER,
    Cacher,
    CSVCacher,
    ParquetCacher,
)
from .excel_collector import ExcelCollector
from .excel_reader import ExcelReader
from .resolve_strategy import (
    DEFAULT_CACHE_DIR,
    DEFAULT_RESOLVE_STRATEGY,
    ResolveStrategy,
    ResolveStrategyType,
    sys_cache_dir,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from os import PathLike
    from pathlib import Path

    from pandas import DataFrame

    Pathish = str | PathLike[str] | Path
    Processor = Callable[[DataFrame], DataFrame]
    FrameReader = Callable[[], DataFrame]

__all__ = [
    # cache_manager
    "CacheManager",
    # cache_strategy
    "ResolveStrategy",
    "DEFAULT_CACHE_STRATEGY",
    "CacheStrategy",
    # cacher
    "CacheStrategyType",
    "DEFAULT_CACHER",
    "Cacher",
    "CSVCacher",
    "ParquetCacher",
    # excel_collector
    "ExcelCollector",
    # excel_reader
    "ExcelReader",
    # resolve_strategy
    "DEFAULT_CACHE_DIR",
    "DEFAULT_RESOLVE_STRATEGY",
    "ResolveStrategy",
    "ResolveStrategyType",
    "sys_cache_dir",
]
