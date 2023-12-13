"""Module for reading and caching Excel files in alternative formats."""
from __future__ import annotations

import errno
import logging
import os
from abc import ABC, abstractmethod
from enum import Enum, member
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol

import pandas as pd
from platformdirs import user_cache_dir

from data_pull_tools.file_utils import hide_file

if TYPE_CHECKING:
    from collections.abc import Callable

    from pandas import DataFrame

CacheLocation = Literal["system", "root_dir"]
module_logger = logging.getLogger(__name__)


class Cacher(ABC):
    """
    Abstract base class for caching dataframes.

    Parameters
    ----------
    pre_process : Callable[[DataFrame], DataFrame], optional
        Function to preprocess the dataframe before caching, by default None
    post_process : Callable[[DataFrame], DataFrame], optional
        Function to postprocess the dataframe after caching, by default None
    """

    def __init__(
        self,
        pre_process: Callable[[DataFrame], DataFrame] | None = None,
        post_process: Callable[[DataFrame], DataFrame] | None = None,
    ) -> None:
        self._pre_process = pre_process
        self._post_process = post_process

    @property
    @abstractmethod
    def suffix(self) -> str:
        """Get the file suffix/extension for the cache file."""
        ...

    @property
    def pre_process(self) -> Callable[[DataFrame], DataFrame]:
        """Get the pre-process function."""
        if self._pre_process is None:
            return lambda df: df
        return self._pre_process

    @property
    def post_process(self) -> Callable[[DataFrame], DataFrame]:
        """Get the post-process function."""
        if self._post_process is None:
            return lambda df: df
        return self._post_process

    @abstractmethod
    def read_cache(self, cache_file: Path) -> DataFrame:
        """
        Read the cached dataframe from file.

        Parameters
        ----------
        cache_file : Path
            Path to the cache file.

        Returns
        -------
        pd.DataFrame
            The cached dataframe.
        """
        ...

    @abstractmethod
    def write_cache(self, cache_file: Path, df: DataFrame) -> None:
        """
        Write the dataframe to the cache file.

        Parameters
        ----------
        cache_file : Path
            Path to write the cache file to.
        df : pd.DataFrame
            Dataframe to cache.
        """
        ...


class ParquetCacher(Cacher):
    """
    Cacher using the Parquet file format.
    Converts object columns of input DataFrame to strings before caching.

    Parameters
    ----------
    pre_process : Callable[[pd.DataFrame], pd.DataFrame], optional
        Function to preprocess the dataframe before caching, by default None
    post_process : Callable[[pd.DataFrame], pd.DataFrame], optional
        Function to postprocess the dataframe after caching, by default None
    """

    def __init__(
        self,
        pre_process: Callable[[DataFrame], DataFrame] | None = None,
        post_process: Callable[[DataFrame], DataFrame] | None = None,
    ) -> None:
        self._user_pre_process = pre_process
        super().__init__(self._pq_pre_process, post_process)

    def _pq_pre_process(self, input_data: DataFrame) -> DataFrame:
        """
        Preprocess dataframe before caching to Parquet format.

        Converts object columns to strings.

        Parameters
        ----------
        df : pd.DataFrame
            Input dataframe.

        Returns
        -------
        pd.DataFrame
            Preprocessed dataframe.
        """
        # Convert object dtypes to str
        input_data = input_data.convert_dtypes()
        obj_cols = input_data.select_dtypes(include="object").columns
        input_data[obj_cols] = input_data[obj_cols].astype(str)
        input_data[obj_cols] = input_data[obj_cols].replace("nan", pd.NA)

        if self._user_pre_process is not None:
            input_data = self._user_pre_process(input_data)

        return input_data

    @property
    def suffix(self) -> str:  # noqa: D102
        return ".parquet"

    def read_cache(self, cache_file: Path) -> DataFrame:  # noqa: D102
        return pd.read_parquet(cache_file)

    def write_cache(self, cache_file: Path, df: DataFrame) -> None:  # noqa: D102
        df.to_parquet(cache_file)


class CSVCacher(Cacher):
    """
    Cacher using the CSV file format.

    Parameters
    ----------
    pre_process : Callable[[pd.DataFrame], pd.DataFrame], optional
        Function to preprocess the dataframe before caching, by default None
    post_process : Callable[[pd.DataFrame], pd.DataFrame], optional
        Function to postprocess the dataframe after caching, by default None
    """

    @property
    def suffix(self) -> str:  # noqa: D102
        return ".csv"

    def read_cache(self, cache_file: Path) -> DataFrame:  # noqa: D102
        return pd.read_csv(cache_file)

    def write_cache(self, cache_file: Path, df: DataFrame) -> None:  # noqa: D102
        df.to_csv(cache_file, index=False)


DEFAULT_CACHER = ParquetCacher()
DEFAULT_CACHE_DIR = Path(
    user_cache_dir(
        "cached-excel-reader",
        appauthor=False,
    ),
)


def _cache_miss(input_file: Path, cache_file: Path) -> bool:
    """Checks if the input file is already cached.

    Parameters
    ----------
    input_file : Path
        The input file path.
    cache_file : Path
        The cache file path.

    Returns
    -------
    bool
        True if the input_file does not have a valid cache, False otherwise.
    """
    return not _cache_hit(input_file, cache_file)


def _cache_hit(input_file: Path, cache_file: Path) -> bool:
    """Checks if the input file is already cached.

    Parameters
    ----------
    input_file : Path
        The input file path.
    cache_file : Path
        The cache file path.

    Returns
    -------
    bool
        True if the input_file has a valid cache, False otherwise.
    """
    return cache_file.exists() and (
        input_file.stat().st_mtime < cache_file.stat().st_mtime
    )


class CacheBehaviorProto(Protocol):
    """Cache function protocol."""

    def __call__(
        self,
        input_file: Path,
        sheet_name: int | str,
        cacher: Cacher,
        cache_file: Path,
    ) -> DataFrame:
        """Perform some action, such as reading the input file and saving it to the
        specified cache location, eventually returning a DataFrame.

        Parameters
        ----------
        input_file : Path
            The input file path.
        sheet_name : int | str
            Strings are used for sheet names. Integers are used in zero-indexed
            sheet positions (chart sheets do not count as a sheet position).
        cacher : Cacher, default DEFAULT_CACHER
            The cacher object for cache operations.
        cache_file : Path
            The cache file path.

        Returns
        -------
        DataFrame
            The final DataFrame.
        """
        ...


def _check_cache(
    input_file: Path,
    sheet_name: int | str,
    cacher: Cacher,
    cache_file: Path,
) -> DataFrame:
    """Checks cache and returns DataFrame, reading input only when necessary.

    Checks if the cache is valid. If cache is valid, returns cached data.
    If not, reads fresh data, caches it, and returns it.

    Parameters
    ----------
    input_file : Path
        The input file path.
    sheet_name : str | int
        The sheet name or index.
    cacher : Cacher
        The cacher object.
    cache_file : Path
        The cache file path.

    Returns
    -------
    DataFrame
        The cached or freshly read DataFrame.
    """
    if _cache_hit(input_file, cache_file):
        return cacher.post_process(cacher.read_cache(cache_file))

    data = pd.read_excel(input_file, sheet_name=sheet_name)
    data = cacher.pre_process(data)
    cacher.write_cache(cache_file, data)

    return cacher.post_process(data)


def _fallback_to_cache(
    input_file: Path,
    sheet_name: int | str,
    cacher: Cacher,
    cache_file: Path,
) -> DataFrame:
    """Checks cache and returns DataFrame, reading input only when necessary.
    If input reading fails, attempts to re-use cache if available.

    Checks if the cache is valid. If cache is valid, returns cached data.
    If not, reads fresh data, caches it, and returns it.
    If reading fresh data fails, attempts to re-use cache instead of raising.
    Re-raises the initial reading error if the cache is unavailable.

    Parameters
    ----------
    input_file : Path
        The input file path.
    sheet_name : str | int
        The sheet name or index.
    cacher : Cacher
        The cacher object.
    cache_file : Path
        The cache file path.

    Returns
    -------
    DataFrame
        The cached or freshly read DataFrame.
    """
    if _cache_hit(input_file, cache_file):
        return cacher.post_process(cacher.read_cache(cache_file))

    try:
        data = pd.read_excel(input_file, sheet_name=sheet_name)
    except Exception as e:
        module_logger.exception("Failed to read Excel file: '%s'", input_file)
        if not cache_file.exists():
            # Not even the cache can save us
            raise e from None
        # Fall back to the cache
        return cacher.post_process(cacher.read_cache(cache_file))

    # There was a cache miss and we successfully loaded input data
    data = cacher.pre_process(data)
    cacher.write_cache(cache_file, data)
    return cacher.post_process(data)


def _force_cache_update(
    input_file: Path,
    sheet_name: int | str,
    cacher: Cacher,
    cache_file: Path,
) -> DataFrame:
    """Forces an update of the cache from the input data.

    Reads the input data, caches it, and returns the DataFrame.

    Parameters
    ----------
    input_file : Path
        The input file path.
    sheet_name : str | int
        The sheet name or index.
    cacher : Cacher
        The cacher object.
    cache_file : Path
        The cache file path.

    Returns
    -------
    pd.DataFrame
        The freshly cached DataFrame.
    """
    data = pd.read_excel(input_file, sheet_name=sheet_name)
    data = cacher.pre_process(data)
    cacher.write_cache(cache_file, data)
    return cacher.post_process(data)


def _skip_cache(
    input_file: Path,
    sheet_name: int | str,
    cacher: Cacher,
    cache_file: Path,  # noqa: ARG001
) -> DataFrame:
    """Reads the input file directly, skips all interaction with the cache.

    Parameters
    ----------
    input_file : Path
        The input file path.
    sheet_name : str | int
        The sheet name or index.
    cacher : Cacher
        The cacher object.
    cache_file : Path
        The cache file path.

    Returns
    -------
    pd.DataFrame
        The freshly read DataFrame.
    """
    data = pd.read_excel(input_file, sheet_name=sheet_name)
    data = cacher.pre_process(data)
    return cacher.post_process(data)


def _from_cache(
    input_file: Path,  # noqa: ARG001
    sheet_name: int | str,  # noqa: ARG001
    cacher: Cacher,
    cache_file: Path,
) -> DataFrame:
    """Reads the cached DataFrame directly, ignoring input data.

    Parameters
    ----------
    input_file : Path
        The input Excel file path (unused).
    sheet_name : str | int
        The sheet name or index (unused).
    cacher : Cacher
        The cacher object.
    cache_file : Path
        The cache file path.

    Returns
    -------
    pd.DataFrame
        The cached DataFrame.
    """
    data = cacher.read_cache(cache_file)
    return cacher.post_process(data)


class CacheBehavior(Enum):
    """Enumeration of several default cache behaviors."""

    def __call__(
        self,
        input_file: Path,
        sheet_name: int | str,
        cacher: Cacher,
        cache_file: Path,
    ) -> DataFrame:
        """Call the associated cache function."""
        return self.value(input_file, sheet_name, cacher, cache_file)  # type: ignore reportGeneralTypeIssues

    CHECK_CACHE = member(_check_cache)
    FALLBACK_TO_CACHE = member(_fallback_to_cache)
    FORCE_CACHE_UPDATE = member(_force_cache_update)
    SKIP_CACHE = member(_skip_cache)
    FROM_CACHE = member(_from_cache)


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
        input_file: str | Path,
        sheet_name: int | str,
        cacher_suffix: str,
    ) -> tuple[Path, Path]:
        """Map the input file and cache file paths.

        Parameters
        ----------
        input_file : str | Path
            The input file path.
        sheet_name : int | str
            The sheet name or index.
        cacher_suffix : str
            The suffix to append to the cache file name.

        Returns
        -------
        tuple[Path, Path]
            The input file path and cache file path.
        """
        if isinstance(input_file, str):
            input_file = root_dir / (
                input_file if input_file.endswith(".xlsx") else (input_file + ".xlsx")
            )
        cache_file = cache_dir / f"{input_file.stem}-{sheet_name}{cacher_suffix}"
        return input_file, cache_file

    def read_excel(
        self,
        input_file: str | Path,
        sheet_name: int | str = 0,
        *,
        cacher: Cacher = DEFAULT_CACHER,
        behavior: CacheBehaviorProto = CacheBehavior.CHECK_CACHE,
    ) -> DataFrame:
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
        behavior : CacheBehaviorProto, default CacheBehavior.CHECK_CACHE
            A callable that determines how the input and cache are handled.
            See `CacheBehavior` for more information and an enumeration of behaviors
            default behaviors.

        Returns
        -------
        DataFrame
            The data read from the Excel file, after any preprocessing
            or postprocessing performed by the `cacher`.

        Examples
        --------
        >>> reader = CachedExcelReader()
        >>> reader.read_excel("input.xlsx", 0)
        >>> reader.read_excel("input.xlsx", "Sheet1")

        These create two different cache files, even if they point to the same sheet.
        """
        input_file, cache_file = self._map_paths(
            root_dir=self.root_dir,
            cache_dir=self.cache_dir,
            input_file=input_file,
            sheet_name=sheet_name,
            cacher_suffix=cacher.suffix,
        )

        return behavior(input_file, sheet_name, cacher, cache_file)


if __name__ == "__main__":
    ...
