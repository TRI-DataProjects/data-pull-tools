import errno
import os
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from InferIndex import CleaningInferrer


class Cacher(ABC):
    @property
    @abstractmethod
    def suffix(self) -> str:
        ...

    @abstractmethod
    def read_cache(self, cache_file: Path) -> pd.DataFrame:
        ...

    @abstractmethod
    def write_cache(self, cache_file: Path, df: pd.DataFrame) -> None:
        ...


class ParquetCacher(Cacher):
    @property
    def suffix(self) -> str:
        return ".parquet"

    def read_cache(self, cache_file: Path) -> pd.DataFrame:
        return pd.read_parquet(cache_file)

    def write_cache(self, cache_file: Path, df: pd.DataFrame) -> None:
        # Convert object dtypes to str
        df = df.convert_dtypes()
        obj_cols = df.select_dtypes(include="object").columns
        df[obj_cols] = df[obj_cols].astype(str)
        df[obj_cols] = df[obj_cols].replace("nan", pd.NA)
        df.to_parquet(cache_file)


class CSVCacher(Cacher):
    @property
    def suffix(self) -> str:
        return ".csv"

    def read_cache(self, cache_file: Path) -> pd.DataFrame:
        return pd.read_csv(cache_file)

    def write_cache(self, cache_file: Path, df: pd.DataFrame) -> None:
        df.to_csv(cache_file, index=False)


class UnstackingParquetCacher(ParquetCacher):
    def write_cache(self, cache_file: Path, df: pd.DataFrame) -> None:
        inferrer = CleaningInferrer()
        df = inferrer.infer_index(df).T.unstack(0).T.convert_dtypes()  # type: ignore
        super().write_cache(cache_file, df)


class UnstackingCSVCacher(CSVCacher):
    def read_cache(self, cache_file: Path) -> pd.DataFrame:
        df = super().read_cache(cache_file)
        inferrer = CleaningInferrer()
        df = inferrer.infer_index(df).T.unstack(0).T.convert_dtypes()  # type: ignore
        return df  # type: ignore

    def write_cache(self, cache_file: Path, df: pd.DataFrame) -> None:
        super().write_cache(cache_file, df)
        inferrer = CleaningInferrer()
        df = inferrer.infer_index(df).T.unstack(0).T.convert_dtypes()  # type: ignore


class CachedExcelReader:
    def __init__(
        self,
        root_dir: Path | str | None = None,
        cache_dir: Path | str = ".cache",
        cacher: Cacher = ParquetCacher(),
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

        # Set internal values
        self.root_dir = root_dir
        self.cache_dir = cache_dir
        self.cacher = cacher

    def __cache_file__(
        self,
        file_name: str,
        cache_suffix: str | None = None,
    ) -> Path:
        if cache_suffix is None:
            cache_file_name = file_name + self.cacher.suffix
        else:
            cache_file_name = file_name + cache_suffix + self.cacher.suffix
        return self.cache_dir / (cache_file_name)

    def __should_update_cache__(self, input_file: Path, cache_file: Path) -> bool:
        return not os.path.exists(cache_file) or (
            os.path.getmtime(input_file) > os.path.getmtime(cache_file)
        )

    def read_excel(
        self,
        file_name: str,
        sheet: str | int = 0,
        cache_suffix: str | None = None,
        force_cache_update: bool = False,
    ) -> pd.DataFrame | pd.Series:

        input_file = self.root_dir / (file_name + ".xlsx")
        cache_file = self.__cache_file__(file_name, cache_suffix)

        if force_cache_update or self.__should_update_cache__(input_file, cache_file):
            df = pd.read_excel(input_file, sheet_name=sheet)
            self.cacher.write_cache(cache_file, df)
        else:
            df = self.cacher.read_cache(cache_file)

        return df.copy()

    def empty_cache_directory(self) -> None:
        for f in os.scandir(self.cache_dir):
            os.remove(f.path)


if __name__ == "__main__":
    ...
