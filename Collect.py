import os
import sys
import time
from pathlib import Path

from ArchiveCollector import *
from CollectorMap import ARCHIVE_COLLECTORS, Month
from PromptSpinner import *

collection_path = Path(os.path.dirname(os.path.realpath(sys.argv[0]))) / 'collected'
def collect_all():
    tot_start = time.time()
    for year in ARCHIVE_COLLECTORS:
        print(f'{year} Begin')
        year_start = time.time()
        year_dict = ARCHIVE_COLLECTORS[year]
        for month in year_dict:
            print(f'{year}-{month.value} Begin')
            month_start = time.time()
            year_dict[month].collect(collection_path, f'{year}-{month.value}')
            month_end = time.time()
            print(f'{year}-{month.value} End ({month_end - month_start:.3f}s)')
        year_end = time.time()
        print(f'{year} End ({year_end - year_start:.3f}s)')

    tot_end = time.time()
    print(f'Operation completed ({tot_end - tot_start:.3f}s)')

def collect_year(year):
    if (year in ARCHIVE_COLLECTORS):
        year_start = time.time()
        year_dict = ARCHIVE_COLLECTORS[year]
        for month in year_dict:
            print(f'{year}-{month.value} Begin')
            month_start = time.time()
            year_dict[month].collect(collection_path, f'{year}-{month.value}')
            month_end = time.time()
            print(f'{year}-{month.value} End ({month_end - month_start:.3f}s)')
        year_end = time.time()
        print(f'{year} End ({year_end - year_start:.3f}s)')

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

collect_year(2017)
# collect(2020, Month.JANUARY)
