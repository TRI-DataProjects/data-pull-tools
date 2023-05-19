import errno
import os
from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path

import pandas as pd

from data_pull_tools.file_utils import hide_file


class Cacher(ABC):
    def __init__(
        self,
        pre_process: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
        post_process: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    ) -> None:
        self._pre_process = pre_process
        self._post_process = post_process

    @property
    @abstractmethod
    def suffix(self) -> str:
        ...

    @property
    def pre_process(self) -> Callable[[pd.DataFrame], pd.DataFrame] | None:
        return self._pre_process

    @property
    def post_process(self) -> Callable[[pd.DataFrame], pd.DataFrame] | None:
        return self._post_process

    @abstractmethod
    def read_cache(self, cache_file: Path) -> pd.DataFrame:
        ...

    @abstractmethod
    def write_cache(self, cache_file: Path, df: pd.DataFrame) -> None:
        ...


class ParquetCacher(Cacher):
    def __init__(
        self,
        pre_process: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
        post_process: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    ) -> None:
        self._user_pre_process = pre_process
        super().__init__(self._pq_pre_process, post_process)

    def _pq_pre_process(self, df: pd.DataFrame) -> pd.DataFrame:
        # Convert object dtypes to str
        df = df.convert_dtypes()
        obj_cols = df.select_dtypes(include="object").columns
        df[obj_cols] = df[obj_cols].astype(str)
        df[obj_cols] = df[obj_cols].replace("nan", pd.NA)

        if self._user_pre_process is not None:
            df = self._user_pre_process(df)

        return df

    @property
    def suffix(self) -> str:
        return ".parquet"

    def read_cache(self, cache_file: Path) -> pd.DataFrame:
        return pd.read_parquet(cache_file)

    def write_cache(self, cache_file: Path, df: pd.DataFrame) -> None:
        df.to_parquet(cache_file)


class CSVCacher(Cacher):
    @property
    def suffix(self) -> str:
        return ".csv"

    def read_cache(self, cache_file: Path) -> pd.DataFrame:
        return pd.read_csv(cache_file)

    def write_cache(self, cache_file: Path, df: pd.DataFrame) -> None:
        df.to_csv(cache_file, index=False)


DEFAULT_CACHER = ParquetCacher()


class CachedExcelReader:
    def __init__(
        self,
        root_dir: Path | str | None = None,
        cache_dir: Path | str = ".cache",
    ) -> None:
        # Handle root_dir
        if root_dir is None:
            root_dir = Path()
        elif isinstance(root_dir, str):
            root_dir = Path(root_dir)

        if not os.path.exists(root_dir):
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                str(root_dir),
            )

        # Handle cache_dir
        if isinstance(cache_dir, str):
            cache_dir = root_dir / cache_dir

        if not os.path.exists(cache_dir):
            os.mkdir(cache_dir)
            cache_dir = hide_file(cache_dir)

        # Set internal values
        self.root_dir = root_dir
        self.cache_dir = cache_dir

    def _cache_file(
        self,
        file_name: str,
        cacher_suffix: str,
        user_suffix: str | None = None,
    ) -> Path:
        if user_suffix is None:
            cache_file_name = file_name + cacher_suffix
        else:
            cache_file_name = file_name + user_suffix + cacher_suffix
        return self.cache_dir / (cache_file_name)

    def _should_update_cache(self, input_file: Path, cache_file: Path) -> bool:
        return not os.path.exists(cache_file) or (
            os.path.getmtime(input_file) > os.path.getmtime(cache_file)
        )

    def read_excel(
        self,
        file_name: str,
        sheet: str | int = 0,
        cache_suffix: str | None = None,
        *,
        cacher: Cacher = DEFAULT_CACHER,
        root_rel_offset: str | None = None,
        force_cache_update: bool = False,
    ) -> pd.DataFrame:
        input_file = self.root_dir
        if root_rel_offset is not None:
            input_file /= root_rel_offset
        if file_name.endswith(".xlsx"):
            input_file /= file_name
        else:
            input_file /= file_name + ".xlsx"
        cache_file = self._cache_file(
            file_name=file_name,
            user_suffix=cache_suffix,
            cacher_suffix=cacher.suffix,
        )

        if force_cache_update or self._should_update_cache(input_file, cache_file):
            df = pd.read_excel(input_file, sheet_name=sheet)
            if cacher.pre_process is not None:
                df = cacher.pre_process(df)
            cacher.write_cache(cache_file, df)
        else:
            df = cacher.read_cache(cache_file)

        if cacher.post_process is not None:
            df = cacher.post_process(df)

        return df


if __name__ == "__main__":
    ...
