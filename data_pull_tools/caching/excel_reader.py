"""Module for reading and caching Excel files in alternative formats."""
from __future__ import annotations

import logging
from functools import partial
from typing import TYPE_CHECKING, overload

import pandas as pd

from data_pull_tools.excel_utils import get_sheet_names

from .cache_manager import CacheManager
from .cache_strategy import DEFAULT_CACHE_STRATEGY, CacheStrategy
from .cacher import DEFAULT_CACHER, Cacher

if TYPE_CHECKING:
    from pathlib import Path
    from typing import IO

    from . import DataFrame, Pathish

IntStr = int | str

module_logger = logging.getLogger(__name__)


class ExcelReader(CacheManager):
    """Reads and caches Excel sheet data in alternative formats
    to improve access times.
    """

    def _read_excel_sheet(  # noqa: PLR0913
        self,
        input_file: Path,
        sheet_name: IntStr,
        cacher: Cacher,
        strategy: CacheStrategy,
        open_handle: IO[bytes] | None = None,
    ) -> DataFrame:
        cache_file = self.output_path(f"{input_file.stem}-{sheet_name}", cacher)

        reader = partial(
            pd.read_excel,
            open_handle or input_file,
            sheet_name=sheet_name,
        )
        return strategy(
            input_file=input_file,
            cache_file=cache_file,
            cacher=cacher,
            reader=reader,
        )

    def _read_excel_sheets(
        self,
        input_file: Path,
        sheet_name: list[IntStr] | None,
        cacher: Cacher,
        strategy: CacheStrategy,
    ) -> dict[IntStr, DataFrame]:
        with input_file.open("rb") as file_handle:
            names = (
                get_sheet_names(input_file, file_handle)
                if sheet_name is None
                else sheet_name
            )
            return {
                name: self._read_excel_sheet(
                    input_file=input_file,
                    sheet_name=name,
                    cacher=cacher,
                    strategy=strategy,
                    open_handle=file_handle,
                )
                for name in names
            }

    @overload
    def read_excel(
        self,
        input_file: Pathish,
        sheet_name: IntStr = 0,
        *,
        cacher: Cacher | None = None,
        strategy: CacheStrategy = DEFAULT_CACHE_STRATEGY,
    ) -> DataFrame:
        ...

    @overload
    def read_excel(
        self,
        input_file: Pathish,
        sheet_name: list[IntStr] | None,
        *,
        cacher: Cacher | None = None,
        strategy: CacheStrategy = DEFAULT_CACHE_STRATEGY,
    ) -> dict[IntStr, DataFrame]:
        ...

    @overload
    def read_excel(
        self,
        input_file: Pathish,
        sheet_name: IntStr | list[IntStr] | None = 0,
        *,
        cacher: Cacher | None = None,
        strategy: CacheStrategy = DEFAULT_CACHE_STRATEGY,
    ) -> DataFrame | dict[IntStr, DataFrame]:
        ...

    def read_excel(
        self,
        input_file: Pathish,
        sheet_name: IntStr | list[IntStr] | None = 0,
        *,
        cacher: Cacher | None = None,
        strategy: CacheStrategy = DEFAULT_CACHE_STRATEGY,
    ) -> DataFrame | dict[IntStr, DataFrame]:
        """Read an Excel file and cache the result.

        Parameters
        ----------
        input_file : Pathish
            The input file path.
        sheet_name : IntStr, default 0
            Strings are used for sheet names. Integers are used in zero-indexed
            sheet positions (chart sheets do not count as a sheet position).
        cacher: Cacher, default DEFAULT_CACHER
            The cacher object for cache operations.
        strategy: CacheStrategy, default DEFAULT_STRATEGY
            A callable that determines how the input and cache are handled.
            See `cache_strategy` for more information and an enumeration of default
            strategies.

        Returns
        -------
        DataFrame | dict[IntStr, DataFrame]
            The data read from the Excel file, after any preprocessing
            or postprocessing performed by the `cacher`.

        Examples
        --------
        >>> reader = CachedExcelReader()
        >>> reader.read_excel("input.xlsx", 0)
        >>> reader.read_excel("input.xlsx", "Sheet1")

        These create two different cache files, even if they point to the same sheet.

        >>> reader = CachedExcelReader()
        >>> reader.read_excel("input.xlsx", None)

        This creates a cache file for each sheet in the Excel file and returns a
        dictionary of dataframes keyed by sheet name.
        """
        input_file = self._input_path(input_file)
        cacher = cacher or DEFAULT_CACHER()

        if sheet_name is not None and isinstance(sheet_name, IntStr):
            return self._read_excel_sheet(
                input_file=input_file,
                sheet_name=sheet_name,
                cacher=cacher,
                strategy=strategy,
            )

        return self._read_excel_sheets(
            input_file=input_file,
            sheet_name=sheet_name,
            cacher=cacher,
            strategy=strategy,
        )
