import re
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable
from re import Match

import pandas as pd


class IndexInferrer(ABC):
    @abstractmethod
    def infer_index(self, df: pd.DataFrame) -> pd.DataFrame | pd.Series: ...


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

    def infer_index(self, df: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
        # Bail early if it's a series by mistake!
        # Cannot infer index from a MultiIndex
        if isinstance(df, pd.Series) or isinstance(df.columns, pd.MultiIndex):
            return df

        return self._infer_column_index(df)  # type: ignore

    def _infer_column_index(self, df: pd.DataFrame) -> pd.DataFrame:
        index_cols: list[int]

        pattern = self._pattern
        repl = self._repl

        # Clean column names
        cols_list = df.columns.to_list()
        if pattern is not None:
            cols_list = [pattern.sub(repl, x) for x in cols_list]
            df.columns = cols_list  # type: ignore

        # Look for header rows
        header_rows = self._find_header_rows(df, cols_list)

        # Determine index columns based on repeat column names
        if header_rows == 0:
            index_cols = self._index_columns_from_repeat_column_names(cols_list)
            if len(df.columns) == len(index_cols):
                # No header rows and all columns unique
                return df
            df = df.set_index(list(df.columns[index_cols]))  # type: ignore

        # Use headers to determine index columns
        else:
            cols_df, index_cols = self._index_columns_from_header_rows(
                df,
                cols_list,
                header_rows,
            )

            # Create a multi-index if necessary
            if len(cols_df.index) > 1:
                cols_df = cols_df.set_index(list(cols_df.columns[index_cols]))  # type: ignore
                cols_mi = pd.MultiIndex.from_tuples(zip(*cols_df.values))

                df = df.iloc[header_rows:, :]
                df = df.set_index(list(df.columns[index_cols]))  # type: ignore
                df.columns = cols_mi

        return df

    def _index_columns_from_repeat_column_names(self, cols_list: list) -> list[int]:
        index_cols: list[int] = []

        # Tally column names
        tally = defaultdict(list)
        for i, item in enumerate(cols_list):
            tally[item].append(i)

        # Append the locations with only one item
        for locs in tally.values():
            if len(locs) == 1:
                index_cols.append(locs[0])

        return index_cols

    def _index_columns_from_header_rows(
        self,
        df: pd.DataFrame,
        cols_list: list,
        header_rows: int,
    ) -> tuple[pd.DataFrame, list[int]]:
        pattern = self._pattern
        repl = self._repl
        index_cols: list[int] = []

        cols_df = pd.DataFrame([cols_list], columns=cols_list)

        headers = df.iloc[0:header_rows, :]
        headers.columns = cols_df.columns
        cols_df = pd.concat([cols_df, headers])
        cols_df = cols_df.ffill(axis=0)

        if pattern is not None:
            cols_df = cols_df.map(lambda x: pattern.sub(repl, x))

        for idx in range(len(cols_list)):
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
    from _example_data import company_quarterly_df

    df = company_quarterly_df

    print("Original:")
    print(df)

    print()

    inferrer = CleaningInferrer()
    inferred = inferrer.infer_index(df)
    print("Inferred:")
    print(inferred)
