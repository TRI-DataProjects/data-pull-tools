from __future__ import annotations

import logging
import multiprocessing as mp
from functools import partial
from typing import TYPE_CHECKING

import pandas as pd

from .cache_manager import CacheManager
from .cache_strategy import CacheStrategy, CacheStrategyType
from .cacher import DEFAULT_CACHER
from .excel_reader import ExcelReader

if TYPE_CHECKING:
    from pathlib import Path

    from . import Cacher, DataFrame, Pathish, ResolveStrategy


module_logger = logging.getLogger(__name__)


def _drop_empty(df: DataFrame) -> DataFrame:
    return df.convert_dtypes().dropna(how="all", axis=1).dropna(how="all", axis=0)


class ExcelCollector(CacheManager):
    reader: ExcelReader
    _output_name: str
    sheet_name: int | str | list[int | str] | None
    glob_pattern: str
    _cacher: Cacher
    _out_st_mtime: float | None

    def __init__(
        self,
        root_dir: Path,
        cache_dir: Pathish | None = None,
        output_name: str | None = None,
        sheet_name: int | str | list[int | str] | None = 0,
        glob_pattern: str = "*.xlsx",
        *,
        cache_resolver: ResolveStrategy | None = None,
        output_cacher: Cacher | None = None,
    ) -> None:
        super().__init__(
            root_dir=root_dir,
            cache_dir=cache_dir,
            cache_resolver=cache_resolver,
        )
        self.reader = ExcelReader(
            root_dir=root_dir,
            cache_dir=cache_dir,
            cache_resolver=cache_resolver,
        )
        self._cacher = output_cacher or DEFAULT_CACHER()
        self.output_name = output_name
        self.sheet_name = sheet_name
        self.glob_pattern = glob_pattern

    @property
    def root_dir(self) -> Path:  # noqa: D102
        return super().root_dir

    @root_dir.setter
    def root_dir(self, root_dir: Pathish | None) -> None:
        super().root_dir = root_dir
        self.reader.root_dir = root_dir

    @property
    def cache_dir(self) -> Path:  # noqa: D102
        return super().cache_dir

    @cache_dir.setter
    def cache_dir(self, cache_dir: Pathish | None) -> None:
        super().cache_dir = cache_dir
        self.reader.cache_dir = cache_dir
        self._update_output_path()

    @property
    def cache_resolver(self) -> ResolveStrategy:  # noqa: D102
        return super().cache_resolver

    @cache_resolver.setter
    def cache_resolver(self, cache_resolve: ResolveStrategy) -> None:
        super().cache_resolver = cache_resolve
        self.reader.cache_resolver = cache_resolve

    @property
    def cacher(self) -> Cacher:
        return self._cacher

    @property
    def output_name(self) -> str:
        return self._output_name

    @output_name.setter
    def output_name(self, output_name: str | None) -> None:
        self._output_name = output_name or self.root_dir.stem
        self._update_output_path()

    def _update_output_path(self) -> None:
        self._output_path = self.cache_dir / f"{self.output_name}{self.cacher.suffix}"
        self._update_st_mtime()

    @property
    def output_path(self) -> Path:
        return self._output_path

    def _update_st_mtime(self) -> float | None:
        if self.output_path.exists():
            self._out_st_mtime = self.output_path.stat().st_mtime
        else:
            self._out_st_mtime = None
        return self._out_st_mtime

    @property
    def out_st_mtime(self) -> float:
        return self._out_st_mtime or self._update_st_mtime() or 0.0

    @property
    def _should_collect(self) -> bool:
        if not self.output_path.exists():
            return True

        for entry in self.root_dir.glob(self.glob_pattern):
            if (
                entry.is_file()
                and entry != self.output_path
                and entry.stat().st_mtime > self.out_st_mtime
            ):
                return True

        return False

    def _perform_collect(
        self,
        reader: ExcelReader | None,
        cacher: Cacher,
        strategy: CacheStrategy,
    ) -> DataFrame:
        module_logger.info("Reading input file(s)")
        reader = reader or self.reader

        read_func = partial(
            reader.read_excel,
            sheet_name=self.sheet_name,
            cacher=cacher,
            strategy=strategy,
        )
        entries = [
            entry
            for entry in self.root_dir.glob(self.glob_pattern)
            if entry.is_file() and entry != self.output_path
        ]

        with mp.Pool() as pool:
            raw_frames = pool.map(read_func, entries)

        def _valid_frame(df: DataFrame) -> bool:
            return (not df.empty) and (df.notna().any().any())

        frames = []
        for frame in raw_frames:
            if isinstance(frame, dict):
                frames.extend(
                    [_drop_empty(df) for df in frame.values() if _valid_frame(df)],
                )
            elif _valid_frame(frame):
                frames.append(_drop_empty(frame))

        if len(frames) == 0:
            module_logger.info("No data collected.")
            return pd.DataFrame()

        module_logger.info("Saving result")

        collected = pd.concat(frames, ignore_index=True, copy=False).convert_dtypes()
        collected = self.cacher.write_cache(self.output_path, collected)
        self._update_st_mtime()
        return collected

    def collect(
        self,
        reader: ExcelReader | None = None,
        cacher: Cacher | None = None,
        strategy: CacheStrategy = CacheStrategyType.CHECK_CACHE,
    ) -> DataFrame:
        module_logger.info("Collecting Excel files.")
        if self._should_collect:
            cacher = cacher or DEFAULT_CACHER()
            return self._perform_collect(reader, cacher, strategy)

        return self.cacher.read_cache(self.output_path)
