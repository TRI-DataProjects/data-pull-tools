import re
from abc import ABC, abstractmethod
from collections import defaultdict
from re import Match
from typing import Callable

import pandas as pd


class IndexInferrer(ABC):
    @abstractmethod
    def infer_index(self, df: pd.DataFrame) -> pd.DataFrame | pd.Series:
        ...


class CleaningInferrer(IndexInferrer):
    def __init__(
        self,
        pattern: str | re.Pattern[str] | None = r"\.\d+$",
        repl: str | Callable[[Match[str]], str] = "",
    ) -> None:
        if isinstance(pattern, str):
            pattern = re.compile(pattern)
        self._pattern = pattern
        self._repl = repl

    def infer_index(self, df: pd.DataFrame) -> pd.DataFrame | pd.Series:

        # Bail early if it's a series by mistake!
        # Cannot infer index from a MultiIndex
        if isinstance(df, pd.Series) or isinstance(df.columns, pd.MultiIndex):
            return df

        return self._infer_column_index(df)  # type: ignore

    def _infer_column_index(
        self,
        df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, int]:
        index_cols: list[int]

        pattern = self._pattern
        repl = self._repl

        # Clean column names
        cols_list = df.columns.to_list()
        if pattern is not None:
            cols_list = [pattern.sub(repl, x) for x in cols_list]
            df.columns = cols_list  # type: ignore
        cols_df = pd.DataFrame([cols_list], columns=cols_list)

        # Look for header rows
        header_rows = self._find_header_rows(df, cols_list)

        # Use headers to determine index columns
        if header_rows > 0:
            cols_df, index_cols = self._index_columns_from_header_rows(
                df,
                cols_df,
                cols_list,
                header_rows,
            )

        # Determine index columns based on repeat column names
        else:
            index_cols = self._index_columns_from_repeat_columns(cols_list)

        # Create a multi-index if necessary
        if len(cols_df.index) > 1:
            cols_df = cols_df.set_index(list(cols_df.columns[index_cols]))  # type: ignore
            cols_mi = pd.MultiIndex.from_tuples(zip(*cols_df.values))

            df = df.iloc[header_rows:, :]
            df = df.set_index(list(df.columns[index_cols]))  # type: ignore
            df.columns = cols_mi

        return df  # type: ignore

    def _index_columns_from_repeat_columns(self, cols_list: list) -> list[int]:
        index_cols: list[int] = list()

        # Tally column names
        tally = defaultdict(int)
        for item in cols_list:
            tally[item] += 1

            # Append the locations with only one item
        for col, count in tally.items():
            if count == 1:
                index_cols.append(col)

        return index_cols

    def _index_columns_from_header_rows(
        self,
        df: pd.DataFrame,
        cols_df: pd.DataFrame,
        cols_list: list,
        header_rows: int,
    ) -> tuple[pd.DataFrame, list[int]]:

        pattern = self._pattern
        repl = self._repl
        index_cols: list[int] = list()

        headers = df.iloc[0:header_rows, :]
        headers.columns = cols_df.columns
        cols_df = pd.concat([cols_df, headers])
        cols_df = cols_df.ffill(axis=0)

        if pattern is not None:
            cols_df = cols_df.applymap(lambda x: pattern.sub(repl, x))

        for idx in range(0, len(cols_list)):
            data = cols_list[idx]
            column = cols_df.iloc[:, idx]
            col_matches = True
            for row in column:
                if data != row:
                    col_matches = False
                    break

            if col_matches:
                index_cols.append(idx)
        return cols_df, index_cols

    def _find_header_rows(
        self,
        df: pd.DataFrame,
        cols_list: list,
    ) -> int:
        header_rows = -1
        while True:
            header_rows += 1
            data = df.iat[header_rows, 0]
            if (
                data == data  # Value is not NaN (NaN == NaN returns false)
                and data != cols_list[0]
            ):
                break

        return header_rows


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
    inferrer = CleaningInferrer()

    inferred = inferrer.infer_index(df)
    print(inferred)
