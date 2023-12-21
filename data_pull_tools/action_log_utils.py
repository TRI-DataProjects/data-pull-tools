"""Module for processing action logs."""

from __future__ import annotations

import logging
import timeit
from pathlib import Path
from typing import TYPE_CHECKING

from platformdirs import user_downloads_dir

from data_pull_tools.caching import (
    DEFAULT_CACHER,
    ExcelCollector,
    ExcelReader,
    ResolveStrategyType,
)

if TYPE_CHECKING:
    from pandas import DataFrame

module_logger = logging.getLogger(__name__)


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


def _rename_columns(df: DataFrame) -> DataFrame:
    return df.rename(columns=renamer)[out_cols]


def process_action_logs(
    process_root: Path,
) -> None:
    """
    Process action logs from Excel files and save the result to a CSV file.

    Parameters
    ----------
    process_mode : ProcessMode
        The processing mode.
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
    module_logger.info("Reading action logs from '%s'", process_root)
    st_time: float = timeit.default_timer()

    renaming_cacher = DEFAULT_CACHER(
        pre_process=_rename_columns,
    )

    if process_root.is_dir():
        action_logs = ExcelCollector(
            root_dir=process_root,
            cache_dir="action_logs",
            cache_resolver=ResolveStrategyType.RESOLVE_TO_SYSTEM,
        ).collect(
            cacher=renaming_cacher,
        )
    elif process_root.is_file():
        action_logs = ExcelReader(
            root_dir=process_root.parent,
            cache_dir="action_logs",
            cache_resolver=ResolveStrategyType.RESOLVE_TO_SYSTEM,
        ).read_excel(
            input_file=process_root.name,
            cacher=renaming_cacher,
        )
    else:
        msg = "process_root of '%s' not supported."
        raise ValueError(msg, process_root)

    elapsed: float = timeit.default_timer() - st_time
    n_read: int = len(action_logs.index)
    noun: str = "row" if n_read == 1 else "rows"
    module_logger.info("Read %d %s in %.3fs", n_read, noun, elapsed)

    module_logger.info("Writing action logs to CSV")
    st_time: float = timeit.default_timer()
    action_logs.to_csv(Path(user_downloads_dir()) / "action_logs.csv", index=True)
    elapsed: float = timeit.default_timer() - st_time
    module_logger.info("Output written in %.3fs", elapsed)


if __name__ == "__main__":
    from prompt_utils import DirPrompt, FilePrompt
    from rich.prompt import Confirm
    from toml_utils import get_toml_item, load_toml, update_toml_file_value

    module_logger.setLevel(logging.DEBUG)
    module_logger.addHandler(logging.StreamHandler())

    cache_logger = logging.getLogger("data_pull_tools.caching.excel_collector")
    cache_logger.setLevel(logging.DEBUG)
    cache_logger.addHandler(logging.StreamHandler())

    al_root: Path | None = None

    config_path = Path(__file__).parent / "_run_config.toml"

    if config_path.exists():
        toml = load_toml(config_path)
        root = get_toml_item(toml, ["action_log", "root"])
        if isinstance(root, str):
            al_root = Path(root)

    if (
        al_root is None or not al_root.exists()
        # or not Confirm.ask("Would you like to use the config file?", default=True)
    ):
        module_logger.debug("No action log root found, prompting user")
        if Confirm.ask("Are you processing a single file?"):
            al_root = FilePrompt.ask("Please enter the file path")
        else:
            al_root = DirPrompt.ask("Please enter the directory path")

        if Confirm.ask("Would you like to remember this path?"):
            update_toml_file_value(
                config_path,
                ["action_log", "root"],
                al_root.as_posix(),
            )

    process_action_logs(al_root)
