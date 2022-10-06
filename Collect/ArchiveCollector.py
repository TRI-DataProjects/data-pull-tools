from abc import ABC, abstractmethod
from multiprocessing.managers import DictProxy

import pandas as pd
from rich.progress import TaskID


class ArchiveCollector(ABC):
    def __init__(self, folder_root):
        self.folder_root = folder_root

    @abstractmethod
    def collect(self, out_dir, out_file, progress: DictProxy, sub_task_id: TaskID):
        pass

    def read_excel(self, file_path, sheet_name=None):
        if sheet_name is None:
            return pd.read_excel(file_path).convert_dtypes()
        else:
            return pd.read_excel(file_path, sheet_name).convert_dtypes()

    def clean_columns(self, collected: pd.DataFrame):
        if all(
            w in collected.columns
            for w in [
                "Provider Unique ID",
                "Provider UID",
            ]
        ):
            collected.drop("License Type", axis=1, inplace=True)
            collected.rename(
                columns={
                    "Licensure": "License Type",
                    "Total Desired Capacity": "Desired Capacity",
                    "Total Licensed Capacity": "Licensed Capacity",
                    "Total Vacancies": "Total Openings",
                },
                inplace=True,
            )

            check_columns = ["Funding", "Organized Structure", "Business Name"]
            fin_mask = None
            for column in check_columns:
                if column in collected.columns:
                    upper = collected[column].str.upper()
                    head_start_mask = pd.Series(
                        (upper.str.find("HEADSTART") >= 0)
                        | (upper.str.find("HEAD START") >= 0)
                    )
                    if fin_mask is None:
                        fin_mask = head_start_mask
                    else:
                        fin_mask |= head_start_mask
            if fin_mask is not None and "Program Types" not in collected.columns:
                collected["Program Types"] = ""
                collected.loc[fin_mask, "Program Types"] = "Head Start (OPK)"

        if "License Type" in collected.columns:
            collected["License Type"] = (
                (collected["License Type"]).str.split(",", n=1).str.strip()
            )

            fin_mask = collected["License Type"] == "Unlicensed - TBD (School Partners)"
            (collected[fin_mask])["License Type"] = "Unlicensed - Exempt"
        
        return collected.copy()


class ArchiveComposer(ArchiveCollector):
    def __init__(
        self,
        folder_root,
        general_file,
        care_center_file,
        address_file,
        general_sheet=None,
        care_center_sheet=None,
        address_sheet=None,
    ):
        super().__init__(folder_root)
        self.general_file = general_file
        self.care_center_file = care_center_file
        self.address_file = address_file
        self.general_sheet = general_sheet
        self.care_center_sheet = care_center_sheet
        self.address_sheet = address_sheet

    def collect(self, out_dir, out_file, progress: DictProxy, sub_task_id: TaskID):
        _upd = progress[sub_task_id]
        _upd["total"] = 6
        progress[sub_task_id] = _upd
        steps_done = 0
        general = self.read_excel(
            self.folder_root / (self.general_file + ".xlsx"), self.general_sheet
        )
        steps_done += 1
        _upd["completed"] = steps_done
        progress[sub_task_id] = _upd

        care_center = self.read_excel(
            self.folder_root / (self.care_center_file + ".xlsx"), self.care_center_sheet
        )
        steps_done += 1
        _upd["completed"] = steps_done
        progress[sub_task_id] = _upd

        address = self.read_excel(
            self.folder_root / (self.address_file + ".xlsx"), self.address_sheet
        )
        steps_done += 1
        _upd["completed"] = steps_done
        progress[sub_task_id] = _upd

        merged = general.merge(care_center, on="Provider UID", how="left",).merge(
            address,
            on="Provider UID",
            how="left",
        )
        steps_done += 1
        _upd["completed"] = steps_done
        progress[sub_task_id] = _upd

        merged.dropna(how="all", axis=1, inplace=True)
        steps_done += 1
        _upd["completed"] = steps_done
        progress[sub_task_id] = _upd

        merged = self.clean_columns(merged)

        merged.to_csv(out_dir / (out_file + ".csv"), index=False)
        steps_done += 1
        _upd["completed"] = steps_done
        progress[sub_task_id] = _upd

        return merged


class ArchiveCopier(ArchiveCollector):
    def __init__(self, folder_root, file_name, file_sheet=None):
        super().__init__(folder_root)
        self.file_name = file_name
        self.file_sheet = file_sheet

    def collect(self, out_dir, out_file, progress: DictProxy, sub_task_id: TaskID):
        _upd = progress[sub_task_id]
        _upd["total"] = 3
        progress[sub_task_id] = _upd
        steps_done = 0
        all_data = self.read_excel(
            self.folder_root / (self.file_name + ".xlsx"), self.file_sheet
        )
        steps_done += 1
        _upd["completed"] = steps_done
        progress[sub_task_id] = _upd

        all_data.dropna(how="all", axis=1, inplace=True)
        steps_done += 1
        _upd["completed"] = steps_done
        progress[sub_task_id] = _upd

        all_data = self.clean_columns(all_data)

        all_data.to_csv(out_dir / (out_file + ".csv"), index=False)
        steps_done += 1
        _upd["completed"] = steps_done
        progress[sub_task_id] = _upd

        return all_data
