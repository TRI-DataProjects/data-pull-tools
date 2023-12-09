"""Module for processing action logs."""

from __future__ import annotations

import logging
import timeit
from pathlib import Path
from typing import Literal

from platformdirs import user_downloads_dir

from data_pull_tools.cached_excel_reader import CachedExcelReader, ParquetCacher
from data_pull_tools.partial_collector import PartialCollector
from data_pull_tools.prompt_utils import DirPrompt

ProcessMode = Literal["single", "batch"]
module_logger = logging.getLogger(__name__)


def process_action_logs(
    process_mode: ProcessMode,
    process_root: Path,
) -> None:
    """
    Process action logs from Excel files and save the result to a CSV file.

    Parameters
    ----------
    process_mode : ProcessMode
        The processing mode, either 'batch' or 'single'.
    process_root : Path
        The root path for processing, either the batch directory or a
    single file.

    Returns
    -------
    None

    Examples
    --------
    >>> process_action_logs(ProcessMode.BATCH, Path("data/batch_logs"))

    >>> process_action_logs(ProcessMode.SINGLE, Path("data/single_log.xlsx"))
    """
    st_time: float = timeit.default_timer()

    renamer = {
        "WLS ID": "Program ID",
        "Date of Action": "Action Date",
    }
    out_cols = [
        "Program ID",
        "Recorded By",
        "Recorded Date",
        "Action Date",
        "Action Log Name",
        "Action Log Type",
        "Notes",
    ]
    renaming_cacher = ParquetCacher(
        pre_process=lambda df: df.rename(columns=renamer)[out_cols],
    )

    if process_mode == "batch":
        collector = PartialCollector(
            process_root,
            "action_logs",
            cache_dir="action_logs",
            cache_location="system",
            collection_cacher=renaming_cacher,
        )
        action_logs = collector.collect()
    elif process_mode == "single":
        action_logs = CachedExcelReader(
            process_root.parent,
            cache_location="system",
        ).read_excel(process_root.name, cacher=renaming_cacher)
    else:
        msg = f"Invalid process mode: {process_mode}"
        raise ValueError(msg)

    elapsed: float = timeit.default_timer() - st_time
    n_read: int = len(action_logs.index)
    noun: str = "row" if n_read == 1 else "rows"
    module_logger.info("Read %s %s in %.3fs", n_read, noun, elapsed)
    action_logs.to_csv(Path(user_downloads_dir()) / "action_logs.csv", index=True)


if __name__ == "__main__":
    from prompt_utils import FilePrompt
    from rich.prompt import Confirm

    module_logger.setLevel(logging.DEBUG)
    module_logger.addHandler(logging.StreamHandler())

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
