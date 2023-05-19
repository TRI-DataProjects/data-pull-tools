import json
import timeit
from collections.abc import Mapping
from concurrent.futures import ProcessPoolExecutor
from os import PathLike, scandir
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
from cached_excel_reader import CachedExcelReader
from pandas import DataFrame

StrPath = Union[str, PathLike[str]]
_search_in_minute = "Referral Search Number In Minute"
_row_key = ["ReferralID", "Date of Action", _search_in_minute]


def process_referral_action_logs(df: DataFrame) -> DataFrame:
    # Remove email addresses, only care if guest or not
    df.loc[df["RecordedBy"] != "guest", ["RecordedBy"]] = "user"

    # Fix datetime
    date_fmt: str = r"%m/%d/%Y %I:%M %p"
    df["Date of Action"] = pd.to_datetime(df["Date of Action"], format=date_fmt)

    # Assign each referral's search a number
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
    mask = df[_search_in_minute] > 10
    erroneous_uniques = (
        df.loc[mask, group]
        .groupby(group)
        .size()
        .reset_index(name="Count")
        .drop("Count", axis=1)
    )
    df = pd.merge(df, erroneous_uniques, how="outer", on=group, indicator=True)
    # Only keep records not part of the erroneous_uniques set
    df = df[df["_merge"] == "left_only"]
    return df.drop("_merge", axis=1)


def parse_referral_chunk(df: DataFrame) -> list[dict]:
    list_notes = []

    for _, row in df.iterrows():
        try:
            obj = json.loads(row["Notes"])
        except json.JSONDecodeError:
            break

        if isinstance(obj, Mapping) and "Filters" in obj:
            list_notes.extend(
                row[_row_key]
                .to_frame()
                .T.merge(
                    right=pd.json_normalize(obj["Filters"]),
                    how="cross",
                )
                .to_dict(orient="records")
            )

    return list_notes


def multiprocess_referral_notes(df: DataFrame) -> DataFrame:
    chunk_size = 100
    results = []

    with ProcessPoolExecutor() as executor:
        splits = list(range(chunk_size, len(df.index), chunk_size))
        chunks = np.split(df, splits)
        results = executor.map(parse_referral_chunk, chunks)

    frame = []
    for result in results:
        frame.extend(result)

    return pd.DataFrame(frame)


def process_referrals(
    referrals_root: StrPath,
    process_mode: str | bool = True,
) -> None:
    referrals_root = Path(referrals_root)
    excel_reader = CachedExcelReader(referrals_root)
    print("Reading action logs file")
    st_time: float = timeit.default_timer()

    if isinstance(process_mode, bool):
        if not process_mode:
            raise ValueError(f"process_mode of '{process_mode}' not supported.")
        frames = []
        for entry in scandir(referrals_root):
            if entry.is_file and entry.name.endswith(".xlsx"):
                frames.append(excel_reader.read_excel(entry.name))
        df = pd.concat(frames, ignore_index=True).convert_dtypes()
    elif isinstance(process_mode, str):
        df: DataFrame = excel_reader.read_excel(process_mode).convert_dtypes()
    else:
        raise ValueError(f"process_mode of '{process_mode}' not supported.")

    elapsed: float = timeit.default_timer() - st_time

    n_read: int = len(df.index)
    noun: str = "row" if n_read == 1 else "rows"
    print(f"Read {n_read} {noun} in {elapsed:.3f}s")

    print("Processing action logs")
    st_time: float = timeit.default_timer()
    action_logs_keep = [
        "Parent Searched for Programs",
        "Specialist Performed Search",
    ]
    df = df.loc[df["Action Log Name"].isin(action_logs_keep)]
    df = process_referral_action_logs(df)
    elapsed: float = timeit.default_timer() - st_time
    print(f"Processed {len(df.index)} referrals in {elapsed:.3f}s")

    print("Cleaning action logs file")
    st_time: float = timeit.default_timer()
    df = clean_referral_action_logs(df)
    elapsed: float = timeit.default_timer() - st_time

    n_dropped = n_read - len(df.index)
    noun: str = "row" if n_dropped == 1 else "rows"
    print(f"Removed {n_dropped} unclean {noun} in {elapsed:.3f}")

    print("Saving referrals")
    df.drop("Notes", axis=1).to_csv(
        referrals_root / r"ProcessedReferrals.csv", index=False
    )
    print("Save successful")

    print("Parsing filters")
    st_time: float = timeit.default_timer()
    notes: DataFrame = multiprocess_referral_notes(df)
    elapsed: float = timeit.default_timer() - st_time

    from_rows: int = len(df.index)
    to_rows: int = len(notes.index)
    print(f"Parsed {from_rows} referrals with {to_rows} filters in {elapsed:.3f}s")

    print("Saving filters")
    notes.to_csv(referrals_root / r"FiltersOnly.csv", index=False)
    print("Save successful")


if __name__ == "__main__":
    from prompt_utils import DirPrompt, FilePrompt
    from rich.pretty import pprint
    from rich.prompt import Confirm
    from toml_utils import LoadTOML

    ref_root: Path | None = None
    process_mode: str | bool | None = None

    # Try to load a sibling config file
    config_path = Path(__file__).parent / "referral_config.toml"
    if config_path.exists():
        config = LoadTOML(config_path)
        if "process_mode" in config:
            process_mode = config["process_mode"]
        if "referral_root" in config:
            ref_root = Path(config["referral_root"])

    # Prompt user for any settings not loaded
    if ref_root is None:
        ref_root = DirPrompt.ask("Enter referral directory path")

    if process_mode is None:
        if Confirm.ask("Are you processing a single file?"):
            process_mode = FilePrompt.ask("Please enter the file name", ref_root).name
        else:
            if Confirm.ask(f"Process all Excel files in '{ref_root}'"):
                process_mode = True
            else:
                print("Cannot proceed with given configuration of:")
                print("config_path: ", end="")
                pprint(config_path)
                print("process_mode: ", end="")
                pprint(process_mode)
                print("Exiting")
                exit(1)

    # Process those referrals!
    process_referrals(ref_root, process_mode)
