from __future__ import annotations

import json
import logging
import timeit
from collections.abc import Callable, Mapping
from functools import partial
from pathlib import Path
from typing import TypeVar

import pandas as pd
from pandas import DataFrame

from data_pull_tools.caching import (
    DEFAULT_CACHER,
    ExcelCollector,
    ExcelReader,
    ResolveStrategyType,
)

module_logger = logging.getLogger(__name__)

T = TypeVar("T")

_search_in_minute = "Referral Search Number In Minute"
_max_searches_per_minute = 10


def process_referral_action_logs(df: DataFrame) -> DataFrame:
    # Remove email addresses, only care if guest or not
    df.loc[df["RecordedBy"] != "guest", ["RecordedBy"]] = "user"

    # Fix datetime
    date_fmt: str = r"%m/%d/%Y %I:%M %p"
    df["Date of Action"] = pd.to_datetime(df["Date of Action"], format=date_fmt)

    # Assign each referral's search a unique number
    # This number is not ordinal and solely an identifier
    df[_search_in_minute] = (
        df.sort_values(["Date of Action"], ascending=[True])
        .groupby(["ReferralID", "Date of Action"])
        .cumcount()
        + 1
    )

    # Limit to columns
    cols: list[str] = [
        "ReferralID",
        "Date of Action",
        _search_in_minute,
        "RecordedBy",
        "Type",
        "County",
        "City",
        "Zipcode",
        "Action Log Name",
        "Action Log Type",
        "Notes",
    ]

    return df[cols]


def clean_referral_action_logs(df: DataFrame) -> DataFrame:
    df = df.drop_duplicates()

    group = ["ReferralID", "Date of Action"]
    mask = df[_search_in_minute] > _max_searches_per_minute
    # Find all groups of records that have too many searches per minute
    erroneous_groups = (
        df.loc[mask, group]
        .groupby(group)
        .size()
        .reset_index(name="Count")
        .drop("Count", axis=1)
    )
    df = df.merge(erroneous_groups, how="outer", on=group, indicator=True)
    # Only keep record groups not part of the erroneous_groups set
    df = df[df["_merge"] == "left_only"]
    return df.drop("_merge", axis=1)


def try_parse_filters(s):
    try:
        data = json.loads(s)
    except json.JSONDecodeError:
        return pd.NA

    if isinstance(data, Mapping) and "Filters" in data and len(data["Filters"]) > 0:
        return data["Filters"]

    return pd.NA


def process_referral_notes(df: DataFrame) -> DataFrame:
    df = df.loc[df["Notes"].notna()]
    df["Notes"] = df["Notes"].apply(try_parse_filters)
    df = df.loc[df["Notes"].notna()]

    df = df.explode("Notes").reset_index(drop=True)
    filters = pd.json_normalize(df["Notes"].to_list())
    return pd.concat(
        [
            df.drop(["Notes"], axis=1),
            filters,
        ],
        axis=1,
    )


def measure_function(
    func: Callable[[], T],
    measure: Callable[[T], float | int],
    verb: str,
    noun: tuple[str, str],
) -> T:
    st_time: float = timeit.default_timer()

    result = func()

    elapsed: float = timeit.default_timer() - st_time
    count = measure(result)
    chosen_noun = noun[0] if count == 1 else noun[1]
    module_logger.info("%s %d %s in %.3fs", verb, count, chosen_noun, elapsed)

    return result


def _read_action_logs(
    process_root: Path,
) -> DataFrame:
    action_logs_keep = [
        "Parent Searched for Programs",
        "Specialist Performed Search",
    ]
    filtering_cacher = DEFAULT_CACHER(
        pre_process=lambda df: df.loc[df["Action Log Name"].isin(action_logs_keep)],
    )

    if process_root.is_dir():
        return ExcelCollector(
            root_dir=process_root,
            cache_dir="referrals",
            cache_resolver=ResolveStrategyType.RESOLVE_TO_SYSTEM,
        ).collect(
            cacher=filtering_cacher,
        )

    if process_root.is_file():
        return ExcelReader(
            root_dir=process_root.parent,
            cache_dir="referrals",
            cache_resolver=ResolveStrategyType.RESOLVE_TO_SYSTEM,
        ).read_excel(
            input_file=process_root.name,
            cacher=filtering_cacher,
        )

    msg = "process_root of '%s' not supported."
    raise ValueError(msg, process_root)


def process_referrals(
    process_root: Path,
) -> None:
    module_logger.info("Reading action logs from '%s'", process_root)
    part = partial(_read_action_logs, process_root)
    action_logs = measure_function(
        part,
        lambda df: len(df.index),
        "Read",
        ("row", "rows"),
    )

    if process_root.is_file():
        process_root = process_root.parent

    module_logger.info("Processing action logs")
    part = partial(process_referral_action_logs, action_logs)
    action_logs = measure_function(
        part,
        lambda df: len(df.index),
        "Processed",
        ("referral", "referrals"),
    )

    rows_before_clean = len(action_logs.index)
    module_logger.info("Cleaning action logs file")
    part = partial(clean_referral_action_logs, action_logs)
    action_logs = measure_function(
        part,
        lambda df: rows_before_clean - len(df.index),
        "Removed",
        ("unclean row", "unclean rows"),
    )

    module_logger.debug("Saving referrals")
    action_logs.drop("Notes", axis=1).to_csv(
        process_root / "ProcessedReferrals.csv",
        index=False,
    )
    module_logger.debug("Save successful")

    module_logger.info("Parsing filters")
    part = partial(process_referral_notes, action_logs)
    notes = measure_function(
        part,
        lambda df: len(df.index),
        "Parsed",
        ("filter", "filters"),
    )

    module_logger.debug("Saving filters")
    notes.to_csv(
        process_root / "ProcessedReferralFiltersOnly.csv",
        index=False,
    )
    module_logger.debug("Save successful")


if __name__ == "__main__":
    from prompt_utils import DirPrompt, FilePrompt
    from rich.prompt import Confirm
    from toml_utils import get_toml_item, load_toml, update_toml_file_value

    module_logger.setLevel(logging.DEBUG)
    module_logger.addHandler(logging.StreamHandler())

    ref_root: Path | None = None

    config_path = Path(__file__).parent / "_run_config.toml"

    if config_path.exists():
        toml = load_toml(config_path)
        root = get_toml_item(toml, ["referral", "root"])
        if isinstance(root, str):
            ref_root = Path(root)

    # Prompt user for any settings not loaded
    if ref_root is None:
        ref_root = DirPrompt.ask("Enter referral directory path")

    if (
        ref_root is None or not ref_root.exists()
        # or not Confirm.ask("Would you like to use the config file?", default=True)
    ):
        module_logger.debug("No referral root found, prompting user")
        if Confirm.ask("Are you processing a single file?"):
            ref_root = FilePrompt.ask("Please enter the file path")
        else:
            ref_root = DirPrompt.ask("Please enter the directory path")

        if Confirm.ask("Would you like to remember this path?"):
            update_toml_file_value(
                config_path,
                ["referral", "root"],
                ref_root.as_posix(),
            )

    # Process those referrals!
    process_referrals(ref_root)
