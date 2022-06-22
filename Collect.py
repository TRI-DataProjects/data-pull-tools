from multiprocessing import Process, Queue, freeze_support
import os
import sys
import time
from pathlib import Path

from ArchiveCollector import *
from CollectorMap import ARCHIVE_COLLECTORS, Month
from PromptSpinner import *

class CollectorSignal(Enum):
    DONE = 0

collection_path = Path(os.path.dirname(os.path.realpath(sys.argv[0]))) / 'collected'

def collect_all():
    tot_start = time.time()
    for year in ARCHIVE_COLLECTORS:
        collect_year(year)
    tot_end = time.time()
    print(f'Operation completed ({tot_end - tot_start:.3f}s)')

def collect_year(year):
    if (year in ARCHIVE_COLLECTORS):
        year_start = time.time()
        year_dict = ARCHIVE_COLLECTORS[year]
        print(f'{year} Begin')
        for month in year_dict:
            collect(year, month)
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

def collector_proc(queue):
    while True:
        msg = queue.get()
        if msg != CollectorSignal.DONE:
            collect(msg['year'], msg['month'])
        else:
            break

def start_collector_procs(proc_queue, num_procs):
    all_procs = list()
    for i in range(0, num_procs):
        col_proc = Process(target=collector_proc, args=(proc_queue,))
        col_proc.daemon = True
        col_proc.start()
        all_procs.append(col_proc)
    return all_procs


if __name__ == '__main__':
    num_procs = 6
    proc_queue = Queue()
    start_time = time.time()
    # Build the queue
    for year in ARCHIVE_COLLECTORS:
        for month in ARCHIVE_COLLECTORS[year]:
            msg = {
                'year': year,
                'month': month
            }
            proc_queue.put(msg)
    # Create the procs
    all_procs = start_collector_procs(proc_queue, num_procs)
    # Add signals for each proc to terminate
    for proc in enumerate(all_procs):
        proc_queue.put(CollectorSignal.DONE)

    # Wait for procs to complete
    for idx, proc in enumerate(all_procs):
        proc.join()
    end_time = time.time()
    print(f"Completed collection in {end_time-start_time:.3f}s")
