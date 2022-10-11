from abc import ABC, abstractmethod
from collections import defaultdict
from email.headerregistry import Group
import re
from re import Match
from tokenize import group
from typing import Callable, Tuple

import numpy as np
import pandas as pd


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

    def infer_stack(self, df: pd.DataFrame, max_depth: int) -> pd.DataFrame | pd.Series:

        # Bail early if it's a series by mistake!
        if isinstance(df, pd.Series):
            return df

        if not isinstance(df.columns, pd.MultiIndex):
            index_cols: list[int] = list()

            pattern = self._pattern
            repl = self._repl

            cols_list = df.columns.to_list()
            if pattern is not None:
                cols_list = [pattern.sub(repl, x) for x in cols_list]
            cols_df = pd.DataFrame([cols_list], columns=cols_list)

            # Look for header rows
            header_rows = -1
            while True:
                header_rows += 1
                if df.iloc[header_rows, 0] != cols_list[0]:
                    break

            pivot_rows = header_rows + 1
            if pivot_rows > max_depth:
                pivot_rows = max_depth

            # Use headers to determin index columns
            if header_rows > 0:
                headers = df.iloc[0:header_rows, :]
                if pattern is not None:
                    headers = headers.applymap(lambda x: pattern.sub(self._repl, x))

                headers.columns = cols_df.columns
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

            # Create a multi-index if necessary
            if len(cols_df.index) > 1:
                cols_df = cols_df.set_index(list(cols_df.columns[index_cols]))  # type: ignore
                df = df.iloc[header_rows:, :]
                df = df.set_index(list(df.columns[index_cols]))  # type: ignore
                zipped = zip(*cols_df.values)
                df.columns = pd.MultiIndex.from_tuples(zipped)
        else:
            pivot_rows = min(df.columns.nlevels, max_depth)

        return df.stack(list(range(0, pivot_rows)))  # type: ignore


class ExcelInferrer(CleaningInferrer):
    def __init__(
        self,
    ) -> None:
        super().__init__(r"\.\d+$", "")


if __name__ == "__main__":
    col = [
        "Company",
        "Year 1",
        "Year 1.1",
        "Year 1.2",
        "Year 1.3",
        "Year 2",
        "Year 2.1",
        "Year 2.2",
        "Year 2.3",
        "Year 3",
        "Year 3.1",
        "Year 3.2",
        "Year 3.3",
        "Year 4",
        "Year 4.1",
        "Year 4.2",
        "Year 4.3",
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
    inferrer = ExcelInferrer()

    stack0 = inferrer.infer_stack(df, 0)
    print(stack0)
    stack1 = inferrer.infer_stack(stack0, 1)
    print(stack1)
    stack2 = inferrer.infer_stack(stack1, 1)
    print(stack2)
    # stack2 is now a series!
    # CleaningInferrer, of whcih ExcelInferrer decends,
    # returns the series unchanged
    stack3 = inferrer.infer_stack(stack2, 1)
    print(stack3)
