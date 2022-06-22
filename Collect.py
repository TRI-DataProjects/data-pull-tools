import multiprocessing
import os
import sys
import time
from pathlib import Path

from ArchiveCollector import *
from CollectorMap import ARCHIVE_COLLECTORS, Month
from PromptSpinner import *

collection_path = Path(os.path.dirname(os.path.realpath(sys.argv[0]))) / 'collected'
def collect_all(concurrent=False):
    tot_start = time.time()
    all_procs = []
    for year in ARCHIVE_COLLECTORS:
        if concurrent:
            process = multiprocessing.Process(target=collect_year, args=(year,True,))
            process.start()
            all_procs.append(process)
        else:
            collect_year(year,concurrent)
    for process in all_procs:
        process.join()
    tot_end = time.time()
    print(f'Operation completed ({tot_end - tot_start:.3f}s)')

def collect_year(year, concurrent=False):
    all_procs = []
    if (year in ARCHIVE_COLLECTORS):
        year_start = time.time()
        year_dict = ARCHIVE_COLLECTORS[year]
        print(f'{year} Begin')
        for month in year_dict:
            if concurrent:
                process = multiprocessing.Process(target=collect, args=(year,month,))
                process.start()
                all_procs.append(process)
            else:
                collect(year, month,True)
        for process in all_procs:
            process.join()
        year_end = time.time()
        print(f'{year} End ({year_end - year_start:.3f}s)')
        return
    print(f'Year {year} not found in ARCHIVE_COLLECTORS dictionary')

def collect(year, month):
    if (year in ARCHIVE_COLLECTORS):
        year_dict = ARCHIVE_COLLECTORS[year]
        if (month in year_dict):
            print(f'{year}-{month.value} Begin')
            month_start = time.time()
            year_dict[month].collect(collection_path, f'{year}-{month.value}')
            month_end = time.time()
            print(f'{year}-{month.value} End ({month_end - month_start:.3f}s)')
            return
    print(f'Year-Month combination {year}-{month.value} not found in ARCHIVE_COLLECTORS dictionary')
    return

if __name__ == '__main__':
    multiprocessing.freeze_support()
    # collect_year(2017, True)
    collect_all(True)
    # collect(2020, Month.JANUARY)
