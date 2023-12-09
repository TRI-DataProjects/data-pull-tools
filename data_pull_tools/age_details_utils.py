from pandas import DataFrame, MultiIndex

from data_pull_tools.infer_index import CleaningInferrer


def unstack_age_details(df: DataFrame) -> DataFrame:
    inferrer = CleaningInferrer()
    df = inferrer.infer_index(df)  # type: ignore
    df = df.dropna(how="all").T.unstack(0).T  # type: ignore

    # Fix multi-index
    mi: MultiIndex = df.index.copy()  # type: ignore
    names = list(mi.names)
    names[-1] = "Age Group"
    mi: MultiIndex = mi.rename(names)

    # Convert "Record ID" to numeric
    mi = mi.set_levels(mi.levels[0].astype(int), level=0)

    return df.set_index(mi)
