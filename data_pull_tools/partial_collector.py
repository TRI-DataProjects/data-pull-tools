from __future__ import annotations

import logging
import multiprocessing as mp
from functools import partial
from typing import TYPE_CHECKING

import pandas as pd

from data_pull_tools.cached_excel_reader import DEFAULT_CACHER, CachedExcelReader

if TYPE_CHECKING:
    from pathlib import Path

    from pandas import DataFrame

    from data_pull_tools.cached_excel_reader import CacheLocation, Cacher

module_logger = logging.getLogger(__name__)


class PartialCollector:
    def __init__(
        self,
        input_dir: Path,
        output_file: Path | str,
        glob_pattern: str = "*.xlsx",
        *,
        collection_cacher: Cacher = DEFAULT_CACHER,
        cache_dir: Path | str | None = None,
        cache_location: CacheLocation | None = None,
    ) -> None:
        if isinstance(output_file, str):
            output_file = input_dir / (
                output_file
                if output_file.endswith(".xlsx")
                else (output_file + ".xlsx")
            )

        self.input_dir = input_dir
        self.reader = CachedExcelReader(
            self.input_dir,
            cache_dir,
            cache_location=cache_location,
        )
        self.collection_cacher = collection_cacher
        self.output_file = output_file
        if self.output_file.exists():
            self.out_st_mtime = self.output_file.stat().st_mtime
        self.glob_pattern = glob_pattern

    @property
    def _should_collect(self) -> bool:
        if not self.output_file.exists():
            return True

        for entry in self.input_dir.glob(self.glob_pattern):
            if (
                entry.is_file()
                and entry != self.output_file
                and entry.stat().st_mtime > self.out_st_mtime
            ):
                return True

        return False

    def _perform_collect(self) -> None:
        with mp.Pool() as pool:
            reader = partial(self.reader.read_excel, cacher=self.collection_cacher)
            entries = [
                entry
                for entry in self.input_dir.glob(self.glob_pattern)
                if entry.is_file() and entry != self.output_file
            ]
            frames = pool.map(reader, entries)

        collected = pd.concat(frames, ignore_index=True).convert_dtypes()
        collected.to_excel(self.output_file, index=False)
        self.out_st_mtime = self.output_file.stat().st_mtime

    def collect(
        self,
    ) -> DataFrame:
        if self._should_collect:
            self._perform_collect()
        return self.reader.read_excel(self.output_file)
