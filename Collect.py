# https://www.deanmontgomery.com/2022/03/24/rich-progress-and-multiprocessing/

import multiprocessing
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing.managers import DictProxy
from pathlib import Path

from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from ArchiveCollector import *
from CollectorMap import ARCHIVE_COLLECTORS, Month


def collect_with_stats(
    year: int,
    month: Month,
    collection_path: Path,
    progress: DictProxy,
    sub_task_id: TaskID,
    overwrite_existing: bool = False,
):
    if year in ARCHIVE_COLLECTORS:
        year_dict = ARCHIVE_COLLECTORS[year]
        if month in year_dict:
            file_exists = os.path.isfile(collection_path / f"{year}-{month.value}.xlsx")
            _upd = progress[sub_task_id]
            if (not file_exists) or (file_exists and overwrite_existing):
                # Collected file does not exist
                # Collect it using the mapping in CollectorMap

                _upd["description"] = f"Collecting {year}-{month.value}"
                progress[sub_task_id] = _upd

                month_df = year_dict[month].collect(
                    collection_path, f"{year}-{month.value}", progress, sub_task_id
                )

            else:
                # Collected file exists
                _upd = progress[sub_task_id]
                _upd["description"] = f"Reading {year}-{month.value} to analyze columns"
                _upd["total"] = 1
                progress[sub_task_id] = _upd

                # Read in the columns of the archived month's data
                month_df = pd.read_excel(
                    str(collection_path / (f"{year}-{month.value}.xlsx")), nrows=0
                )
                _upd["completed"] = 1
                progress[sub_task_id] = _upd

            # Assemble column names into a dataframe
            cols = []
            for col in month_df.columns:
                cols.append([col, year, month.value])

            _upd["result"] = pd.DataFrame(
                cols, columns=["Column Name", "Year", "Month"]
            )
            progress[sub_task_id] = _upd


if __name__ == "__main__":

    collection_path = Path(os.path.dirname(os.path.realpath(sys.argv[0]))) / "collected"
    if not os.path.isdir(collection_path):
        os.mkdir(collection_path)

    overwrite = True

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        "Elapsed:",
        TimeElapsedColumn(),
        refresh_per_second=2,  # Slower updates
    ) as progress_bar:
        futures = []  # Track jobs
        with multiprocessing.Manager() as manager:
            # Share state between main process and worker functions
            _progress = manager.dict()

            # Overall progress tracking
            main_task = progress_bar.add_task("[bold]Collecting archived data...")

            with ProcessPoolExecutor() as executor:
                # Create processes and progress bars
                for year in ARCHIVE_COLLECTORS:
                    for month in ARCHIVE_COLLECTORS[year]:
                        sub_task_id = progress_bar.add_task(
                            f"Sub_Task {year}-{month.value}", visible=False, start=False
                        )
                        _progress[sub_task_id] = {
                            "description": None,
                            "completed": 0,
                            "total": 0,
                            "result": None,
                        }
                        futures.append(
                            executor.submit(
                                collect_with_stats,
                                year,
                                month,
                                collection_path,
                                _progress,
                                sub_task_id,
                                overwrite,
                            )
                        )

                progress_bar.update(main_task, total=len(futures))

                # Monitor progress
                while True:
                    n_completed = sum([future.done() for future in futures])

                    progress_bar.update(main_task, completed=n_completed)

                    for task_id, update_data in _progress.items():
                        description = update_data["description"]
                        completed = update_data["completed"]
                        total = update_data["total"]
                        if total != 0 and not progress_bar.tasks[task_id].started:
                            progress_bar.start_task(task_id)
                        progress_bar.update(
                            task_id,
                            description=description,
                            completed=completed,
                            total=total,
                            visible=completed < total,
                        )

                    time.sleep(0.5)
                    # All jobs completed, stop monitoring
                    if n_completed == len(futures):
                        break

            # Raise any errors
            for future in futures:
                future.result()

            data = None

            for task_id, update_data in _progress.items():
                df = update_data["result"]
                if data is None:
                    data = df
                else:
                    data = pd.concat([data, df], ignore_index=True)

            if data is not None:
                data.to_csv(collection_path / "_column_stats.csv", index=False)
