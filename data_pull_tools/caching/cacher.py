from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from collections.abc import MutableSequence
    from pathlib import Path

    from . import DataFrame, Processor

module_logger = logging.getLogger(__name__)


class Cacher(ABC):
    """Abstract base class for caching dataframes.

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
        """Apply any registered pre-processors to the DataFrame.

        Parameters
        ----------
        df: DataFrame
            The DataFrame to pre-process.

        Returns
        -------
        DataFrame
            The pre-processed DataFrame.
        """
        if self._pre_process is None:
            return df

        for processor in self._pre_process:
            df = processor(df)  # noqa: PD901
        return df

    def register_pre_process(self, processor: Processor) -> None:
        """Register a processor to run before caching the dataframe.

        Parameters
        ----------
        processor : Processor
            The processor function to run before caching.
        """
        if self._pre_process is None:
            self._pre_process = [processor]
        else:
            self._pre_process.append(processor)

    def post_process(self, df: DataFrame) -> DataFrame:
        """Apply any registered post-processors to the DataFrame.

        Parameters
        ----------
        df: DataFrame
            The DataFrame to post-process.

        Returns
        -------
        DataFrame
            The post-processed DataFrame.
        """
        if self._post_process is None:
            return df

        for processor in self._post_process:
            df = processor(df)  # noqa: PD901
        return df

    def register_post_process(self, processor: Processor) -> None:
        """Register a processor to run after reading the dataframe from the cache.

        Parameters
        ----------
        processor : Processor
            The post-processor function to register.
        """
        if self._post_process is None:
            self._post_process = [processor]
        else:
            self._post_process.append(processor)

    def read_cache(self, cache_file: Path) -> DataFrame:
        """Read the cached dataframe from file and applies any registered
        post-processors.

        Parameters
        ----------
        cache_file: Path
            Path to the cache file.

        Returns
        -------
        DataFrame
            The cached dataframe.
        """
        return self.post_process(self._read_cache(cache_file))

    @abstractmethod
    def _read_cache(self, cache_file: Path) -> DataFrame:
        """Specific implementation of reading the cache file."""
        ...

    def write_cache(self, cache_file: Path, df: DataFrame) -> DataFrame:
        """Applies any registered pre-processors to the DataFrame and writes it to the
        cache file.

        Parameters
        ----------
        cache_file: Path
            Path to write the cache file to.
        df: DataFrame
            Dataframe to cache.
        """
        df = self.pre_process(df)
        self._write_cache(cache_file, df)
        return df

    @abstractmethod
    def _write_cache(self, cache_file: Path, df: DataFrame) -> None:
        """Specific implementation of writing the cache file."""
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
    """Cacher using the Parquet file format.
    Converts object columns of input DataFrame to strings before caching.

    Parameters
    ----------
    pre_process: Processor | MutableSequence[Processor] | None, optional
        Function to preprocess the dataframe before caching, by default None
    post_process: Processor | MutableSequence[Processor] | None, optional
        Function to postprocess the dataframe after caching, by default None
    """

    def pre_process(self, df: DataFrame) -> DataFrame:  # noqa: D102
        df = super().pre_process(df)  # noqa: PD901
        return self._obj_cols_to_str(df)

    def _obj_cols_to_str(self, df: DataFrame) -> DataFrame:
        """Converts a DataFrame's object columns to strings columns.
        Necessary for caching as Parquet does not support object columns.

        Parameters
        ----------
        df: DataFrame
            Input dataframe.

        Returns
        -------
        DataFrame
            Dataframe with object columns converted to strings.
        """
        df = df.convert_dtypes()
        obj_cols = df.select_dtypes(include="object").columns
        df[obj_cols] = df[obj_cols].astype(str)
        df[obj_cols] = df[obj_cols].replace("nan", pd.NA)
        return df

    @property
    def suffix(self) -> str:  # noqa: D102
        return ".parquet"

    def _read_cache(self, cache_file: Path) -> DataFrame:
        return pd.read_parquet(cache_file)

    def _write_cache(self, cache_file: Path, df: DataFrame) -> None:
        df.to_parquet(cache_file)


class CSVCacher(Cacher):
    """Cacher using the CSV file format.

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

    def _read_cache(self, cache_file: Path) -> DataFrame:
        return pd.read_csv(cache_file)

    def _write_cache(self, cache_file: Path, df: DataFrame) -> None:
        df.to_csv(cache_file, index=False)


DEFAULT_CACHER = ParquetCacher
