from __future__ import annotations

from enum import Enum, member
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from pandas import DataFrame

    from .cacher import Cacher


class CacheBehaviorProtocol(Protocol):
    """Some callable that performs an action, such as reading the input file and saving
    it to the specified cache location, eventually returning a DataFrame.

    Parameters
    ----------
    input_file : Path
        The input file path.
    cache_file : Path
        The cache file path.
    cacher : Cacher, default=DEFAULT_CACHER
        The cacher object for cache operations.
    reader : Callable[[], DataFrame]
        The function to read in new data, assumedly from the input file.

    Returns
    -------
    DataFrame
        The final DataFrame.
    """

    def __call__(
        self,
        input_file: Path,
        cache_file: Path,
        cacher: Cacher,
        reader: Callable[[], DataFrame],
    ) -> DataFrame:
        """Perform some action, such as reading the input file and saving it to the
        specified cache location, eventually returning a DataFrame.

        Parameters
        ----------
        input_file : Path
            The input file path.
        cache_file : Path
            The cache file path.
        cacher : Cacher, default=DEFAULT_CACHER
            The cacher object for cache operations.
        reader : Callable[[], DataFrame]
            The function to read in new data, assumedly from the input file.

        Returns
        -------
        DataFrame
            The final DataFrame.
        """
        ...


def _check_cache(
    input_file: Path,
    cache_file: Path,
    cacher: Cacher,
    reader: Callable[[], DataFrame],
) -> DataFrame:
    """Checks cache and returns DataFrame, reading input only when necessary.

    Checks if the cache is valid. If cache is valid, returns cached data.
    If not, reads fresh data, caches it, and returns it.

    Parameters
    ----------
    input_file : Path
        The input file path.
    cache_file : Path
        The cache file path.
    cacher : Cacher, default=DEFAULT_CACHER
        The cacher object for cache operations.
    reader : Callable[[], DataFrame]
        The function to read in new data, assumedly from the input file.

    Returns
    -------
    DataFrame
        The cached or freshly read DataFrame.
    """
    if cacher.cache_hit(input_file, cache_file):
        return cacher.post_process(cacher.read_cache(cache_file))

    data = reader()
    data = cacher.pre_process(data)
    cacher.write_cache(cache_file, data)

    return cacher.post_process(data)


def _fallback_to_cache(
    input_file: Path,
    cache_file: Path,
    cacher: Cacher,
    reader: Callable[[], DataFrame],
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
    cache_file : Path
        The cache file path.
    cacher : Cacher, default=DEFAULT_CACHER
        The cacher object for cache operations.
    reader : Callable[[], DataFrame]
        The function to read in new data, assumedly from the input file.

    Returns
    -------
    DataFrame
        The cached or freshly read DataFrame.
    """
    if cacher.cache_hit(input_file, cache_file):
        return cacher.post_process(cacher.read_cache(cache_file))

    try:
        data = reader()
    except Exception as e:
        if not cache_file.exists():
            # Not even the cache can save us
            raise e from None
        # Fall back to the cache
        return cacher.post_process(cacher.read_cache(cache_file))

    # There was a cache miss but we successfully read input data
    data = cacher.pre_process(data)
    cacher.write_cache(cache_file, data)
    return cacher.post_process(data)


def _force_cache_update(
    input_file: Path,  # noqa: ARG001
    cache_file: Path,
    cacher: Cacher,
    reader: Callable[[], DataFrame],
) -> DataFrame:
    """Forces an update of the cache from the input data.

    Reads the input data, caches it, and returns the DataFrame.

    Parameters
    ----------
    input_file : Path
        The input file path.
    cache_file : Path
        The cache file path.
    cacher : Cacher, default=DEFAULT_CACHER
        The cacher object for cache operations.
    reader : Callable[[], DataFrame]
        The function to read in new data, assumedly from the input file.

    Returns
    -------
    DataFrame
        The freshly cached DataFrame.
    """
    data = reader()
    data = cacher.pre_process(data)
    cacher.write_cache(cache_file, data)
    return cacher.post_process(data)


def _skip_cache(
    input_file: Path,  # noqa: ARG001
    cache_file: Path,  # noqa: ARG001
    cacher: Cacher,
    reader: Callable[[], DataFrame],
) -> DataFrame:
    """Reads the input file directly, skips all interaction with the cache.

    Parameters
    ----------
    input_file : Path
        The input file path.
    cache_file : Path
        The cache file path.
    cacher : Cacher, default=DEFAULT_CACHER
        The cacher object for cache operations.
    reader : Callable[[], DataFrame]
        The function to read in new data, assumedly from the input file.

    Returns
    -------
    DataFrame
        The freshly read DataFrame.
    """
    data = reader()
    data = cacher.pre_process(data)
    return cacher.post_process(data)


def _from_cache(
    input_file: Path,  # noqa: ARG001
    cache_file: Path,  # noqa: ARG001
    cacher: Cacher,
    reader: Callable[[], DataFrame],
) -> DataFrame:
    """Reads the cached DataFrame directly, ignoring input data.

    Parameters
    ----------
    input_file : Path
        The input file path.
    cache_file : Path
        The cache file path.
    cacher : Cacher, default=DEFAULT_CACHER
        The cacher object for cache operations.
    reader : Callable[[], DataFrame]
        The function to read in new data, assumedly from the input file.

    Returns
    -------
    DataFrame
        The cached DataFrame.
    """
    data = reader()
    return cacher.post_process(data)


class CacheBehavior(Enum):
    """Enumeration of several default cache behaviors."""

    def __call__(
        self,
        input_file: Path,
        cache_file: Path,
        cacher: Cacher,
        reader: Callable[[], DataFrame],
    ) -> DataFrame:
        """Call the associated cache function."""
        return self.value(input_file, cache_file, cacher, reader)  # type: ignore reportGeneralTypeIssues

    CHECK_CACHE = member(_check_cache)
    FALLBACK_TO_CACHE = member(_fallback_to_cache)
    FORCE_CACHE_UPDATE = member(_force_cache_update)
    SKIP_CACHE = member(_skip_cache)
    FROM_CACHE = member(_from_cache)
