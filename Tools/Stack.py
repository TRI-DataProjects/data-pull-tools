import pandas as pd


def stack_to_depth(df: pd.DataFrame, max_depth: int) -> pd.DataFrame:
    if isinstance(df, pd.Series):
        return df

    col_levels = 1
    if isinstance(df.columns, pd.MultiIndex):
        col_levels = df.columns.nlevels

    pivot_rows = min(col_levels, max_depth)

    return df.stack(list(range(0, pivot_rows)))  # type: ignore


if __name__ == "__main__":
    import InferIndex as ii
    import _ExampleDataSets as eds

    df = eds.company_quarterly_df

    print("Original:")
    print(df)

    print()

    inferrer = ii.CleaningInferrer()
    inferred = inferrer.infer_index(df)

    for i in range(0, 4):
        print()

        print(f"Max depth: {i}")
        print(stack_to_depth(inferred, i))
