import os
from pathlib import Path

import pandas as pd
from cached_excel_reader import CachedExcelReader


class PartialCollector:
    def __init__(self, input_root: Path, input_name: str) -> None:
        self.lists_dir = input_root / input_name
        self.out_file = input_root / f"{input_name}.xlsx"
        if self.out_file.exists():
            self.out_st_mtime = self.out_file.stat().st_mtime

    @property
    def _should_collect(self) -> bool:
        if not self.out_file.exists():
            return True

        for entry in os.scandir(self.lists_dir):
            if entry.is_file() and entry.name.endswith(".xlsx"):
                if entry.stat().st_mtime > self.out_st_mtime:
                    return True

        return False

    def _perform_collect(self) -> None:
        reader = CachedExcelReader(self.lists_dir)
        collected = None
        for entry in os.scandir(self.lists_dir):
            if entry.is_file() and entry.name.endswith(".xlsx"):
                data = reader.read_excel(
                    file_name=entry.name[: -len(".xlsx")],
                )
                if collected is None:
                    collected = data.copy()
                else:
                    collected = pd.concat([collected, data])  # type: ignore
        if collected is not None:
            collected = collected.reset_index(drop=True).convert_dtypes()
            collected.to_excel(str(self.out_file), index=False)
            self.out_st_mtime = self.out_file.stat().st_mtime

    def collect_partial(self) -> None:
        if self._should_collect:
            self._perform_collect()
