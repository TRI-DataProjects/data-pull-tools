import errno
import os
from pathlib import Path

import pandas as pd


class CachedExcelReader:
    def __init__(
        self,
        root_dir: Path | str,
        cache_dir: Path | str | None = ".cache",
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

    def read_excel(
        self,
        file_name: str,
        sheet: str | int = 0,
        csv_suffix: str | None = None,
    ) -> pd.DataFrame | pd.Series:

        xlsx_file = str(self.root_dir / (file_name + "xlsx"))
        csv_file_name = file_name + csv_suffix if csv_suffix is not None else file_name
        csv_file = str(self.cache_dir / (csv_file_name + ".csv"))

        if not os.path.exists(csv_file) or (
            os.path.getmtime(xlsx_file) > os.path.getmtime(csv_file)
        ):
            df = pd.read_excel(xlsx_file, sheet)
            df.to_csv(csv_file)
        else:
            df = pd.read_csv(csv_file)

        return df.copy()
