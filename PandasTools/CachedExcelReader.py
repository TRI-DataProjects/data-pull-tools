import errno
import os
from enum import Enum
from pathlib import Path

import pandas as pd


class CacheType(Enum):
    CSV = ".csv"
    PARQUET = ".parquet"


class CachedExcelReader:
    def __init__(
        self,
        root_dir: Path | str | None = None,
        cache_dir: Path | str = ".cache",
        cache_type: CacheType = CacheType.PARQUET,
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
        self.cache_type = cache_type

    def __cache_file__(
        self,
        file_name: str,
        cache_suffix: str | None = None,
    ) -> Path:
        if cache_suffix is None:
            cache_file_name = file_name + self.cache_type.value
        else:
            cache_file_name = file_name + cache_suffix + self.cache_type.value
        return self.cache_dir / (cache_file_name)

    def __should_update_cache__(self, input_file: Path, cache_file: Path) -> bool:
        return not os.path.exists(cache_file) or (
            os.path.getmtime(input_file) > os.path.getmtime(cache_file)
        )

    def __write_cache__(self, cache_file: Path, df: pd.DataFrame) -> None:
        if self.cache_type == CacheType.PARQUET:
            # Convert object dtypes to str
            df = df.convert_dtypes()
            obj_cols = df.select_dtypes(include="object").columns
            df[obj_cols] = df[obj_cols].astype(str)
            df[obj_cols] = df[obj_cols].replace("nan", pd.NA)
            df.to_parquet(cache_file)
        elif self.cache_type == CacheType.CSV:
            df.to_csv(cache_file, index=False)
        else:
            raise NotImplementedError(f"{self.cache_type} is not a supported CacheType")

    def __read_cache__(self, cache_file: Path) -> pd.DataFrame:
        if self.cache_type == CacheType.PARQUET:
            df = pd.read_parquet(cache_file)
        elif self.cache_type == CacheType.CSV:
            df = pd.read_csv(cache_file)
        else:
            raise NotImplementedError(f"{self.cache_type} is not a supported CacheType")

        return df

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
            self.__write_cache__(cache_file, df)
        else:
            df = self.__read_cache__(cache_file)

        return df.copy()

    def empty_cache_directory(self) -> None:
        for f in os.scandir(self.cache_dir):
            os.remove(f.path)


if __name__ == "__main__":
    ...
