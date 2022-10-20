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
        root_dir: Path | str,
        cache_dir: Path | str | None = ".cache",
        cache_type: CacheType = CacheType.PARQUET,
    ) -> None:

        # Handle root_dir
        if isinstance(root_dir, str):
            root_dir = Path(root_dir)

        if not os.path.exists(root_dir):
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                str(root_dir),
            )

        # Handle cache_dir
        if cache_dir is None:
            cache_dir = root_dir
        elif isinstance(cache_dir, str):
            cache_dir = root_dir / cache_dir

        if not os.path.exists(cache_dir):
            os.mkdir(cache_dir)

        self.root_dir = root_dir
        self.cache_dir = cache_dir
        self.cache_type = cache_type

    def read_excel(
        self,
        file_name: str,
        sheet: str | int = 0,
        cache_suffix: str | None = None,
        header: int | list[int] | None = 0,
        index_col: int | list[int] | None = None,
    ) -> pd.DataFrame | pd.Series:

        xlsx_file = str(self.root_dir / (file_name + ".xlsx"))
        cache_file_name = (
            file_name + cache_suffix if cache_suffix is not None else file_name
        )
        cache_file = str(self.cache_dir / (cache_file_name + self.cache_type.value))

        if not os.path.exists(cache_file) or (
            os.path.getmtime(xlsx_file) > os.path.getmtime(cache_file)
        ):
            df = pd.read_excel(
                xlsx_file,
                sheet_name=sheet,
                header=header,
                index_col=index_col,
            )
            if self.cache_type == CacheType.PARQUET:
                # Convert object dtypes to str
                df = df.convert_dtypes()
                obj_cols = df.select_dtypes(include="object").columns
                df[obj_cols] = df[obj_cols].astype(str)
                df.to_parquet(cache_file)
            elif self.cache_type == CacheType.CSV:
                df.to_csv(cache_file, index=False)
            else:
                raise NotImplementedError(f"{self.cache_type} is not a supported CacheType")
        else:
            if self.cache_type == CacheType.PARQUET:
                df = pd.read_parquet(cache_file)
            elif self.cache_type == CacheType.CSV:
                df = pd.read_csv(cache_file)
            else:
                raise NotImplementedError(f"{self.cache_type} is not a supported CacheType")

        return df.copy()


if __name__ == "__main__":
    ...
