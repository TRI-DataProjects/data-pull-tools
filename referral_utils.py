import json
import timeit
from concurrent.futures import ProcessPoolExecutor
from os import PathLike, scandir
from pathlib import Path
from typing import Mapping, Union

import numpy as np
import pandas as pd
from dotenv import dotenv_values
from pandas import DataFrame

from cached_excel_reader import CachedExcelReader as Reader

StrPath = Union[str, PathLike[str]]
_search_on_day = "Referal Search Number In Minute"
_row_key = ["ReferralID", "Date of Action", _search_on_day]


def process_referral_action_logs(df: DataFrame) -> DataFrame:
    # Remove email addresses, only care if guest or not
    df.loc[df["RecordedBy"] != "guest", ["RecordedBy"]] = "user"

    # Fix datetime
    date_fmt: str = r"%m/%d/%Y %I:%M %p"
    df["Date of Action"] = pd.to_datetime(df["Date of Action"], format=date_fmt)

    # Asign each referral's search a number
    df[_search_on_day] = (
        df.sort_values(["Date of Action"], ascending=[True])
        .groupby(["ReferralID", "Date of Action"])
        .cumcount()
        + 1
    )

    # Limit to columns
    cols: list[str] = [
        "ReferralID",
        "Date of Action",
        _search_on_day,
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


def parse_referral_chunk(df: DataFrame) -> list[dict]:
    list_notes = []

    for _, row in df.iterrows():
        obj = json.loads(row["Notes"])
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


def process_referrals(dotenv_path: StrPath | None = None) -> None:
    config: dict[str, str | None] = dotenv_values(dotenv_path)
    ref_root = config["REFERRAL_ROOT"]
    ref_file_name = config["REFERRAL_FILE"]
    ref_collect = config["REFERRAL_COLLECT"]
    if ref_root is not None and (ref_file_name is not None or ref_collect is not None):
        ref_root = Path(ref_root)
        reader = Reader(ref_root)

        print("Reading action logs file")
        st_time: float = timeit.default_timer()
        if ref_collect is not None:
            frames = []
            for entry in scandir(ref_root):
                if entry.is_file and entry.name.endswith(".xlsx"):
                    frames.append(reader.read_excel(entry.name))
            df = pd.concat(frames, ignore_index=True).convert_dtypes()
        elif ref_file_name is not None:
            df: DataFrame = reader.read_excel(ref_file_name).convert_dtypes()
        else:
            raise ValueError("Invalid configuration")
        elapsed: float = timeit.default_timer() - st_time

        n_read: int = len(df.index)
        noun: str = "row" if n_read == 1 else "rows"
        print(f"Read {n_read} {noun} in {elapsed:.3f}s")

        df = df.drop_duplicates()
        n_dropped = n_read - len(df.index)
        noun: str = "duplicate" if n_dropped == 1 else "duplicates"
        print(f"Dropped {n_dropped} {noun}")

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

        print("Saving referrals")
        df.drop("Notes", axis=1).to_csv(
            ref_root / r"ProcessedReferrals.csv", index=False
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
        notes.to_csv(ref_root / r"FiltersOnly.csv", index=False)
        print("Save successful")


if __name__ == "__main__":
    process_referrals()
