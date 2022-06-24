import os
import sys
import time
from multiprocessing import Pool
from pathlib import Path

from ArchiveCollector import *
from CollectorMap import ARCHIVE_COLLECTORS, Month
from PromptSpinner import *


class CollectorSignal(Enum):
    DONE = 0


def collect_with_stats(year, month, collection_path, overwrite_existing=False):
    if year in ARCHIVE_COLLECTORS:
        year_dict = ARCHIVE_COLLECTORS[year]
        if month in year_dict:
            file_exists = os.path.isfile(collection_path / f"{year}-{month.value}.xlsx")
            if (not file_exists) or (file_exists and overwrite_existing):
                # Collected file does not exist
                # Collect it using the mapping in CollectorMap
                print(f"Collecting {year}-{month.value}")
                month_start = time.time()
                month_df = year_dict[month].collect(
                    collection_path,
                    f"{year}-{month.value}",
                )
                month_end = time.time()
                print(
                    f"Collected {year}-{month.value} ({month_end - month_start:.3f}s)"
                )
            else:
                # Collected file exists
                print(f"Reading {year}-{month.value} to analyze columns")
                # Read in the columns of the archived month's data
                month_df = pd.read_excel(
                    collection_path / f"{year}-{month.value}.xlsx", nrows=0
                )

            # Assemble column names into a dataframe
            cols = []
            for col in month_df.columns:
                cols.append([col, year, month.value])
            return pd.DataFrame(cols, columns=["Column Name", "Year", "Month"])


if __name__ == "__main__":
    collection_path = Path(os.path.dirname(os.path.realpath(sys.argv[0]))) / "collected"
    if not os.path.isdir(collection_path):
        os.mkdir(collection_path)

    targets = []
    overwrite = False
    for year in ARCHIVE_COLLECTORS:
        for month in ARCHIVE_COLLECTORS[year]:
            targets.append(
                [
                    year,
                    month,
                    collection_path,
                    overwrite,
                ]
            )

    print(f"Collecting archived data")
    start_time = time.time()
    with Pool() as pool:
        dfs = pool.starmap(collect_with_stats, targets)
    end_time = time.time()
    print(f"Completed collection in {end_time-start_time:.3f}s")

    data = pd.concat(dfs, ignore_index=True)
    data.to_csv(collection_path / "_column_stats.csv", index=False)
