"""Module for reading and caching Excel files in alternative formats."""
from __future__ import annotations

import errno
import logging
import os
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Literal, overload

import pandas as pd
from platformdirs import user_cache_dir

from data_pull_tools.file_utils import hide_file

from .cache_strategy import CacheStrategy, CacheStrategyProtocol
from .cacher import DEFAULT_CACHER, Cacher

if TYPE_CHECKING:
    from pandas import DataFrame, ExcelFile

CacheLocation = Literal["system", "root_dir"]
module_logger = logging.getLogger(__name__)


DEFAULT_CACHE_DIR = Path(
    user_cache_dir(
        "cached-excel-reader",
        appauthor=False,
    ),
)


class CachedExcelReader:
    """Reads and caches Excel sheet data in alternative formats
    to improve access times.

    Parameters
    ----------
    root_dir : Path | str | None, optional
        The root directory to search for Excel files. Default is None.
    cache_dir : Path | str | None, optional
        The directory to store cached data. Default is None.
    cache_location : CacheLocation | None, optional
        Where to locate the cache directory. Default is None.
    """

    _root_dir: Path
    _cache_dir: Path
    _raw_cache_dir: Path | str | None
    _cache_location: CacheLocation | None

    def __init__(
        self,
        root_dir: Path | str | None = None,
        cache_dir: Path | str | None = None,
        *,
        cache_location: CacheLocation | None = None,
    ) -> None:
        # Set internal values
        self._root_dir = self._validate_root_dir(root_dir)
        self._cache_location = cache_location
        self._raw_cache_dir = cache_dir
        self._cache_dir = self._vaildate_cache_dir(
            self.root_dir,
            cache_dir,
            self.cache_location,
        )

    #
    # Properties and property validating methods
    #

    @staticmethod
    def _validate_root_dir(root_dir: Path | str | None) -> Path:
        """Validates and returns the root directory.

        Parameters
        ----------
        root_dir : Path | str | None
            The root directory to validate.

        Returns
        -------
        Path
            The validated root directory.

        Raises
        ------
        FileNotFoundError
            If the root directory does not exist.
        TypeError
            If the root directory is not a directory.
        """
        if root_dir is None:
            root_dir = Path()
        elif isinstance(root_dir, str):
            root_dir = Path(root_dir)

        if not root_dir.exists():
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                str(root_dir),
            )

        if not root_dir.is_dir():
            msg = "'root_dir' must be a directory, received: %s"
            raise TypeError(msg, root_dir)

        return root_dir

    @staticmethod
    def _vaildate_cache_dir(
        root_dir: Path,
        cache_dir: Path | str | None,
        cache_location: CacheLocation | None,
    ) -> Path:
        """Validate and returns the cache directory.

        Parameters
        ----------
        root_dir : Path
            The root directory.
        cache_dir : Path | str | None
            The directory for caching Excel files.
        cache_location : CacheLocation | None
            The location where the cache is stored.

        Returns
        -------
        Path
            The validated cache directory.
        """
        if isinstance(cache_dir, Path):
            if cache_location is not None:
                cache_location = None
                module_logger.debug(
                    "'cache_location' is ignored when 'cache_dir' is a path.",
                )
        else:  # cache_dir is not a `Path`
            cache_location = (  # Default to "root_dir" when unspecified
                cache_location if cache_location is not None else "root_dir"
            )
            leaf = (
                cache_dir  # Cache folder with user's provided name
                if cache_dir is not None
                else ".cache"  # Hidden cache folder in root_dir
                if cache_location == "root_dir"
                else ""  # No leaf otherwise
            )

            if cache_location == "system":
                cache_dir = DEFAULT_CACHE_DIR / leaf
            elif cache_location == "root_dir":
                cache_dir = root_dir / leaf

        # Make cache_dir if necessary
        if not cache_dir.exists():
            cache_dir.mkdir(parents=True)
            # Hide if it's in the root folder
            if cache_location == "root_dir":
                cache_dir = hide_file(cache_dir)

        return cache_dir

    @property
    def root_dir(self) -> Path:
        """Get or set the root directory.

        When setting, validates the input and updates the cache directory as necessary.
        """
        return self._root_dir

    @root_dir.setter
    def root_dir(self, root_dir: Path | str | None) -> None:
        root_dir = self._validate_root_dir(root_dir)
        if root_dir == self._root_dir:
            return  # No change
        self._root_dir = root_dir
        self.cache_dir = self._raw_cache_dir  # Revalidate cache_dir

    @property
    def cache_dir(self) -> Path:
        """Get or set the cache directory.

        When setting, validates the input.
        """
        return self._cache_dir

    @cache_dir.setter
    def cache_dir(self, cache_dir: Path | str | None) -> None:
        if cache_dir == self._raw_cache_dir:
            return  # No change
        self._raw_cache_dir = cache_dir
        self._cache_dir = self._vaildate_cache_dir(
            self.root_dir,
            cache_dir,
            self.cache_location,
        )

    @property
    def cache_location(self) -> CacheLocation | None:
        """Get or set the cache location.

        When setting, updates the cache directory as necessary..
        """
        return self._cache_location

    @cache_location.setter
    def cache_location(self, cache_location: CacheLocation | None) -> None:
        if cache_location == self._cache_location:
            return  # No change
        self._cache_location = cache_location
        self.cache_dir = self._raw_cache_dir  # Revalidate cache_dir

    #
    # Methods
    #

    @staticmethod
    def _map_paths(
        root_dir: Path,
        cache_dir: Path,
        input_file: str | Path | ExcelFile,
        sheet_name: int | str,
        cacher_suffix: str,
    ) -> tuple[ExcelFile | Path, Path]:
        """Map the input file and cache file paths.

        Parameters
        ----------
        input_file : str | Path | ExcelFile
            The input file path.
        sheet_name : int | str
            The sheet name or index.
        cacher_suffix : str
            The suffix to append to the cache file name.

        Returns
        -------
        tuple[ExcelFile | Path, Path]
            (input file or path, cache file path)
        """
        if isinstance(input_file, Path):
            input_path = input_file
        elif isinstance(input_file, str):
            input_path = root_dir / (
                input_file if input_file.endswith(".xlsx") else (input_file + ".xlsx")
            )
            input_file = input_path
        elif isinstance(input_file, pd.ExcelFile):
            input_path = Path(input_file)

        cache_file = cache_dir / f"{input_path.stem}-{sheet_name}{cacher_suffix}"
        return input_file, cache_file

    @overload
    def read_excel(
        self,
        input_file: str | Path | ExcelFile,
        sheet_name: int | str = 0,
        *,
        cacher: Cacher = DEFAULT_CACHER,
        strategy: CacheStrategyProtocol = CacheStrategy.CHECK_CACHE,
    ) -> DataFrame:
        ...

    @overload
    def read_excel(
        self,
        input_file: str | Path | ExcelFile,
        sheet_name: list[int] | list[str] | None,
        *,
        cacher: Cacher = DEFAULT_CACHER,
        strategy: CacheStrategyProtocol = CacheStrategy.CHECK_CACHE,
    ) -> dict[int | str, DataFrame]:
        ...

    @overload
    def read_excel(
        self,
        input_file: str | Path | ExcelFile,
        sheet_name: int | str | list[int] | list[str] | None = 0,
        *,
        cacher: Cacher = DEFAULT_CACHER,
        strategy: CacheStrategyProtocol = CacheStrategy.CHECK_CACHE,
    ) -> DataFrame | dict[int | str, DataFrame]:
        ...

    def read_excel(
        self,
        input_file: str | Path | ExcelFile,
        sheet_name: int | str | list[int] | list[str] | None = 0,
        *,
        cacher: Cacher = DEFAULT_CACHER,
        strategy: CacheStrategyProtocol = CacheStrategy.CHECK_CACHE,
    ) -> DataFrame | dict[int | str, DataFrame]:
        """Read an Excel file and cache the result.

        Parameters
        ----------
        input_file : str | Path
            The input file path.
        sheet_name : int | str, default 0
            Strings are used for sheet names. Integers are used in zero-indexed
            sheet positions (chart sheets do not count as a sheet position).
        cacher : Cacher, default DEFAULT_CACHER
            The cacher object for cache operations.
        strategy : CacheStrategyProto, default CacheStrategy.CHECK_CACHE
            A callable that determines how the input and cache are handled.
            See `CacheStrategy` for more information and an enumeration of default
            strategies.

        Returns
        -------
        DataFrame | dict[int | str, DataFrame]
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
        if sheet_name is None or not isinstance(sheet_name, (int, str)):
            with pd.ExcelFile(input_file) as excel_file:
                return {
                    sheet_name: self.read_excel(
                        excel_file,
                        sheet_name,
                        cacher=cacher,
                        strategy=strategy,
                    )
                    for sheet_name in excel_file.sheet_names
                }

        input_file, cache_file = self._map_paths(
            root_dir=self.root_dir,
            cache_dir=self.cache_dir,
            input_file=input_file,
            sheet_name=sheet_name,
            cacher_suffix=cacher.suffix,
        )
        reader = partial(pd.read_excel, input_file, sheet_name)

        return strategy(Path(input_file), cache_file, cacher, reader)


if __name__ == "__main__":
    pass
