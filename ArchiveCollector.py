import time
from abc import ABC, abstractmethod

import pandas as pd

from PromptSpinner import DotsSpinner


class ArchiveCollector(ABC):
    def __init__(self, folder_root):
        self.folder_root = folder_root

    @abstractmethod
    def collect(self, out_dir, out_file):
        pass

    def read_excel(self, file_path, sheet_name=None):
        if sheet_name is None:
            return pd.read_excel(file_path).convert_dtypes()
        else:
            return pd.read_excel(file_path, sheet_name).convert_dtypes()

class ArchiveComposer(ArchiveCollector):
    def __init__(self, folder_root, general_file, care_center_file, address_file, general_sheet=None, care_center_sheet=None, address_sheet=None):
        super().__init__(folder_root)
        self.general_file = general_file
        self.care_center_file = care_center_file
        self.address_file = address_file
        self.general_sheet = general_sheet
        self.care_center_sheet = care_center_sheet
        self.address_sheet = address_sheet

    def collect(self, out_dir, out_file):
        col_start = time.time()
        # spinner = DotsSpinner()

        # spinner.start_spin('Reading in general info')
        # start = time.time()
        general = self.read_excel(self.folder_root / (self.general_file + '.xlsx'), self.general_sheet)
        # end = time.time()
        # spinner.stop_spin()
        # print(f'({end - start:.3f}s)')

        # spinner.start_spin('Reading in care center info')
        # start = time.time()
        care_center = self.read_excel(self.folder_root / (self.care_center_file + '.xlsx'), self.care_center_sheet)
        # end = time.time()
        # spinner.stop_spin()
        # print(f'({end - start:.3f}s)')
        
        # spinner.start_spin('Reading in address info')
        # start = time.time()
        address = self.read_excel(self.folder_root / (self.address_file + '.xlsx'), self.address_sheet)
        # end = time.time()
        # spinner.stop_spin()
        # print(f'({end - start:.3f}s)')
        
        # spinner.start_spin('Merging the info')
        # start = time.time()
        merged = general.merge(care_center, on='Provider UID', how='left').merge(address, on='Provider UID', how='left')
        # end = time.time()
        # spinner.stop_spin()
        # print(f'({end - start:.3f}s)')
        
        # spinner.start_spin('Writing info to output file')
        # start = time.time()
        merged.to_excel(out_dir / (out_file + '.xlsx'), index=False)
        # end = time.time()
        # spinner.stop_spin()
        # print(f'({end - start:.3f}s)')
        
        col_end = time.time()
        print(f'Data collection successful ({col_end - col_start:.3f}s)')

class ArchiveCopier(ArchiveCollector):
    def __init__(self, folder_root, file_name, file_sheet=None):
        super().__init__(folder_root)
        self.file_name = file_name
        self.file_sheet = file_sheet

    def collect(self, out_dir, out_file):
        col_start = time.time()
        # spinner = DotsSpinner()

        # spinner.start_spin('Reading in all info')
        # start = time.time()
        all_data = self.read_excel(self.folder_root / (self.file_name + '.xlsx'), self.file_sheet)
        # end = time.time()
        # spinner.stop_spin()
        # print(f'({end - start:.3f}s)')

        # spinner.start_spin('Writing info to output file')
        # start = time.time()
        all_data.to_excel(out_dir / (out_file + '.xlsx'), index=False)
        # end = time.time()
        # spinner.stop_spin()
        # print(f'({end - start:.3f}s)')

        col_end = time.time()
        print(f'Data collection successful ({col_end - col_start:.3f}s)')
