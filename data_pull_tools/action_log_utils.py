"""Module for processing action logs."""

from __future__ import annotations

import logging
import timeit
from pathlib import Path

from platformdirs import user_downloads_dir

from data_pull_tools.cached_excel_reader import CachedExcelReader, ParquetCacher
from data_pull_tools.map_utils import traverse_map
from data_pull_tools.partial_collector import PartialCollector
from data_pull_tools.prompt_utils import DirPrompt

module_logger = logging.getLogger(__name__)


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

    if process_root.is_dir():
        collector = PartialCollector(
            process_root,
            "action_logs",
            cache_dir="action_logs",
            cache_location="system",
            collection_cacher=renaming_cacher,
        )
        action_logs = collector.collect()
    elif process_root.is_file():
        action_logs = CachedExcelReader(
            process_root.parent,
            cache_location="system",
        ).read_excel(
            process_root.name,
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


def _parse_toml_config(toml_path: Path) -> Path | None:
    """Parse a TOML configuration file for action log processing."""
    if not toml_path.exists():
        module_logger.debug("No config file found at '%s'", toml_path)
        return None

    from toml_utils import load_toml

    module_logger.debug("Loading config file at '%s'", toml_path)
    config = load_toml(toml_path)

    root = traverse_map(config, ["action_log", "root"])
    if root is None:
        module_logger.debug("No action log root found in config")
        return None
    if not isinstance(root, str):
        module_logger.debug(
            "Ignoring invalid action log root from config: '%s'",
            root,
        )
        return None
    root = Path(root)
    module_logger.debug("Action log root: '%s'", root)
    return root


def _update_toml_config(toml_path: Path, al_root: Path) -> None:
    """Update a TOML configuration file for action log processing."""
    from toml_utils import manage_toml_file, update_toml_values

    with manage_toml_file(toml_path) as toml_file:
        update_toml_values(
            toml_file,
            {
                "action_log": {
                    "root": al_root.as_posix(),
                },
            },
        )


if __name__ == "__main__":
    from prompt_utils import FilePrompt
    from rich.prompt import Confirm

    module_logger.setLevel(logging.DEBUG)
    module_logger.addHandler(logging.StreamHandler())

    al_root: Path | None = None

    config_path = Path(__file__).parent / "_run_config.toml"
    al_root = _parse_toml_config(config_path)

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
            _update_toml_config(config_path, al_root)

    process_action_logs(al_root)
