from __future__ import annotations

import timeit
from os import scandir
from pathlib import Path
from typing import Literal

import pandas as pd

from data_pull_tools.cached_excel_reader import CachedExcelReader
from data_pull_tools.prompt_utils import DirPrompt

ProcessMode = Literal["single", "batch"]


def process_action_logs(
    process_mode: ProcessMode,
    process_root: Path,
) -> None:
    st_time: float = timeit.default_timer()

    if process_mode == "batch":
        excel_reader = CachedExcelReader(process_root)
        frames = [
            excel_reader.read_excel(entry.name)
            for entry in scandir(process_root)
            if entry.name.endswith(".xlsx")
        ]
        action_logs = pd.concat(frames, ignore_index=True).convert_dtypes()
    else:
        excel_reader = CachedExcelReader(process_root.parent)
        action_logs = excel_reader.read_excel(process_root.name).convert_dtypes()

    elapsed: float = timeit.default_timer() - st_time
    n_read: int = len(action_logs.index)
    noun: str = "row" if n_read == 1 else "rows"
    print(f"Read {n_read} {noun} in {elapsed:.3f}s")


if __name__ == "__main__":
    from prompt_utils import FilePrompt
    from rich.prompt import Confirm

    al_root: Path | None = None
    process_mode: ProcessMode | None = None

    config_path = Path(__file__).parent / "action_log_config.toml"
    if config_path.exists():
        from toml_utils import load_toml

        config = load_toml(config_path)
        if "process_mode" in config:
            process_mode = config["process_mode"]  # type: ignore
        if "action_log_root" in config:
            al_root = Path(config["action_log_root"])  # type: ignore

    if process_mode is None:
        if Confirm.ask("Are you processing a single file?"):
            process_mode = "single"
        else:
            process_mode = "batch"
    if al_root is None:
        if process_mode == "single":
            al_root = FilePrompt.ask("Please enter the file path")
        else:
            al_root = DirPrompt.ask("Please enter the directory path")

    process_action_logs(process_mode, al_root)
