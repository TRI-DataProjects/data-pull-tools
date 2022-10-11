from abc import ABC, abstractmethod
from collections import defaultdict
from email.headerregistry import Group
import re
from re import Match
from tokenize import group
from typing import Callable, Tuple

import numpy as np
import pandas as pd


class PivotInferrer(ABC):
    @abstractmethod
    def infer_grouping(self, df: pd.DataFrame) -> InferredGrouping:
        ...


class Row0Inferrer(PivotInferrer):
    def __init__(
        self,
        pattern: str | re.Pattern[str] | None = None,
        repl: str | Callable[[Match[str]], str] = "",
    ) -> None:
        if isinstance(pattern, str):
            pattern = re.compile(pattern)
        self._pattern = pattern
        self._repl = repl

    def infer_grouping(
        self,
        df: pd.DataFrame,
    ) -> InferredGrouping:
        cols = df.columns.tolist()
        row0 = df.iloc[0].copy().tolist()

        unique_cols = dict()
        groups = dict()

        # Separate the unique columns from grouped columns
        for idx in range(0, len(cols)):
            group = cols[idx]
            data = row0[idx]
            if group == data:
                unique_cols[group] = idx
            else:
                if self._pattern is not None:
                    group = self._pattern.sub(self._repl, group)
                if group not in groups:
                    groups[group] = {
                        "column_index": list(),
                        "data_names": list(),
                    }
                groups[group]["column_index"].append(idx)
                groups[group]["data_names"].append(data)

        # There were no groups, no operation could be done
        if len(groups) == 0:
            raise Exception("Could not infer grouping.")

        # Make sure all groups have the same columns
        all_match = True
        first_list = list(groups.values())[0]["data_names"]

        for val in groups.values():
            if val["data_names"] != first_list:
                all_match = False
                break

        if not all_match:
            raise Exception("Could not infer grouping.")

        grouping = GroupedColumns(first_list)

        for key, val in groups.items():
            grouping.add_group(
                key,
                val["column_index"],
            )

        return InferredGrouping(
            left=list(unique_cols.values()),
            groups=grouping,
            row_offset=1,
        )


class ExcelInferrer(Row0Inferrer):
    def __init__(self, repl: str | Callable[[Match[str]], str] = "") -> None:
        super().__init__(r"\.\d+$", repl)


class StackInferrer(ABC):
    @abstractmethod
    def infer_stack(self, df: pd.DataFrame, max_depth: int):
        ...


class CleaningInferrer(StackInferrer):
    def __init__(
        self,
        pattern: str | re.Pattern[str] | None = None,
        repl: str | Callable[[Match[str]], str] = "",
    ) -> None:
        if isinstance(pattern, str):
            pattern = re.compile(pattern)
        self._pattern = pattern
        self._repl = repl

    def infer_stack(self, df: pd.DataFrame, max_depth: int):
        if isinstance(df.columns, pd.MultiIndex):
            raise NotImplementedError(
                "This method not implemented for MultiIndexed columns"
            )

        index_cols: list[int] = list()

        cols_list = df.columns.to_list()
        if self._pattern is not None:
            cols_list = [self._pattern.sub(self._repl, x) for x in cols_list]
        cols_df = pd.DataFrame([cols_list], columns=cols_list)

        # Look for header rows
        header_rows = -1
        while True:
            header_rows += 1
            if df.iloc[header_rows, 0] != cols_list[0]:
                break

        pivot_rows = header_rows
        if header_rows + 1 >= max_depth:
            pivot_rows = max_depth - 1

        # Use headers to determin index columns
        if header_rows > 0:
            headers = df.iloc[0:header_rows, :]
            if self._pattern is not None:
                #
                #
                # TODO: Clean all header cells with pattern
                #
                #
                headers = headers["Test"].str
                ...
            
            cols_df = pd.concat([cols_df, headers])

            for idx in range(0, len(cols_list)):
                data = cols_list[idx]
                column = headers.iloc[:, idx]
                col_matches = True
                for row in column:
                    if data != row:
                        col_matches = False
                        break

                if col_matches:
                    index_cols.append(idx)

        # Determine index columns based on repeat column names
        else:
            tally = defaultdict(list)
            for i, item in enumerate(cols_list):
                tally[item].append(i)

            for locs in tally.values():
                if len(locs) == 1:
                    index_cols.append(locs[0])

        cols_df = cols_df.set_index(list(cols_df.columns[index_cols]))
        zipped = zip(*cols_df.values)

        df = df.iloc[header_rows:, :]
        df = df.set_index(list(df.columns[index_cols]))
        df.columns = pd.MultiIndex.from_tuples(zipped)

        return df.stack(list(range(0, pivot_rows + 1)))


if __name__ == "__main__":
    col = [
        "Company",
        "Year 1",
        "Year 1",
        "Year 1",
        "Year 1",
        "Year 2",
        "Year 2",
        "Year 2",
        "Year 2",
        "Year 3",
        "Year 3",
        "Year 3",
        "Year 3",
        "Year 4",
        "Year 4",
        "Year 4",
        "Year 4",
    ]

    data = [
        [
            "Company",
            "Q1",
            "Q2",
            "Q3",
            "Q4",
            "Q1",
            "Q2",
            "Q3",
            "Q4",
            "Q1",
            "Q2",
            "Q3",
            "Q4",
            "Q1",
            "Q2",
            "Q3",
            "Q4",
        ],
        ["Lesch Ltd", 21, 10, 77, 58, 58, 95, 96, 45, 84, 73, 12, 87, 43, 50, 86, 24],
        ["Ratke Ltd", 60, 56, 89, 66, 76, 35, 41, 49, 19, 63, 47, 83, 53, 69, 99, 95],
        ["Willms", 98, 25, 74, 28, 87, 13, 99, 51, 52, 39, 30, 75, 14, 70, 78, 61],
        ["InaVerit", 64, 39, 67, 89, 60, 15, 63, 16, 45, 67, 46, 15, 10, 00, 49, 41],
    ]

    df = pd.DataFrame(data, columns=col)

    print("Original:")
    print(df)
    print("Inferred:")

    print(infer_stack(df, 0))
    print(infer_stack(df, 1))
    print(infer_stack(df, 2))
