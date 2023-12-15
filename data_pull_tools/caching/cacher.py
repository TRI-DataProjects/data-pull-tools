from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING

import pandas as pd
from pandas import DataFrame

if TYPE_CHECKING:
    from collections.abc import MutableSequence
    from pathlib import Path


Processor = Callable[[DataFrame], DataFrame]


class Cacher(ABC):
    """
    Abstract base class for caching dataframes.

    Parameters
    ----------
    pre_process: Processor | MutableSequence[Processor] | None, optional
        Function to preprocess the dataframe before caching, by default None
    post_process: Processor | MutableSequence[Processor] | None, optional
        Function to postprocess the dataframe after caching, by default None
    """

    def __init__(
        self,
        pre_process: Processor | MutableSequence[Processor] | None = None,
        post_process: Processor | MutableSequence[Processor] | None = None,
    ) -> None:
        if callable(pre_process):
            pre_process = [pre_process]
        if callable(post_process):
            post_process = [post_process]
        self._pre_process = pre_process
        self._post_process = post_process

    @property
    @abstractmethod
    def suffix(self) -> str:
        """Get the file suffix/extension for the cache file."""
        ...

    def pre_process(self, df: DataFrame) -> DataFrame:
        if self._pre_process is None:
            return df

        for processor in self._pre_process:
            df = processor(df)  # noqa: PD901
        return df

    def register_pre_process(self, processor: Processor) -> None:
        if self._pre_process is None:
            self._pre_process = [processor]
        else:
            self._pre_process.append(processor)

    def post_process(self, df: DataFrame) -> DataFrame:
        if self._post_process is None:
            return df

        for processor in self._post_process:
            df = processor(df)  # noqa: PD901
        return df

    def register_post_process(self, processor: Processor) -> None:
        if self._post_process is None:
            self._post_process = [processor]
        else:
            self._post_process.append(processor)

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
        DataFrame
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
        df : DataFrame
            Dataframe to cache.
        """
        ...

    def cache_hit(self, input_file: Path, cache_file: Path) -> bool:
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

    def cache_miss(self, input_file: Path, cache_file: Path) -> bool:
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
        return not self.cache_hit(input_file, cache_file)


class ParquetCacher(Cacher):
    """
    Cacher using the Parquet file format.
    Converts object columns of input DataFrame to strings before caching.

    Parameters
    ----------
    pre_process: Processor | MutableSequence[Processor] | None, optional
        Function to preprocess the dataframe before caching, by default None
    post_process: Processor | MutableSequence[Processor] | None, optional
        Function to postprocess the dataframe after caching, by default None
    """

    def pre_process(self, df: DataFrame) -> DataFrame:
        df = super().pre_process(df)
        return self._pq_pre_process(df)

    def _pq_pre_process(self, input_data: DataFrame) -> DataFrame:
        """
        Preprocess dataframe before caching to Parquet format.

        Converts object columns to strings.

        Parameters
        ----------
        df : DataFrame
            Input dataframe.

        Returns
        -------
        DataFrame
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
    pre_process: Processor | MutableSequence[Processor] | None, optional
        Function to preprocess the dataframe before caching, by default None
    post_process: Processor | MutableSequence[Processor] | None, optional
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
