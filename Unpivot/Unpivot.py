import pandas as pd


def unpivot_df(df: pd.DataFrame, group_name: str) -> pd.DataFrame:
    cols = df.columns.tolist()
    row0 = df.iloc[0].copy().tolist()

    unique_cols = dict()
    groups = dict()
    group_no = 0

    # Separate the unique columns from grouped columns
    for idx in range(0, len(cols)):
        group = cols[idx]
        data = row0[idx]
        if group == data:
            unique_cols[group] = idx
        else:
            group = group.rsplit(".")[0].strip()
            if group not in groups:
                groups[group] = {
                    "group_no": group_no,
                    "column_index": list(),
                    "data_names": list(),
                }
                group_no += 1
            groups[group]["column_index"].append(idx)
            groups[group]["data_names"].append(data)

    # There were no groups, no operation could be done, return unchanged DF
    if len(groups) == 0:
        return df

    # Make sure all groups have the same columns
    all_match = True
    first_list = list(groups.values())[0]["data_names"]

    for val in groups.values():
        if val["data_names"] != first_list:
            all_match = False
            break

    if not all_match:
        raise Exception("Could not unpivot DataFrame.")

    # First row has column information not data, drop it
    df.drop(index=0, inplace=True)

    # Separate unique columns from grouped data
    left = df.iloc[:, list(unique_cols.values())]
    out_df = None

    # Unpivot grouped columns
    for key, val in groups.items():
        group = pd.DataFrame(df.iloc[:, val["column_index"]].copy())
        group.columns = val["data_names"]
        group.insert(loc=0, column="group_no", value=val["group_no"])
        group.insert(loc=1, column=group_name, value=key)
        merge = pd.concat([left, group], axis=1)
        if out_df is None:
            out_df = merge
        else:
            out_df = pd.concat([out_df, merge], axis=0)

    if out_df is not None:
        return (
            out_df.rename_axis("column_index")
            .sort_values(by=["column_index", "group_no"], ascending=[True, True])
            .drop("group_no", axis=1)
            .rename_axis(None)
        )
    else:
        raise Exception("Could not unpivot DataFrame.")


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
    ]

    data = [
        [  # Heading row 2
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
        ],
        ["Lesch Ltd", 21, 10, 77, 58, 58, 95, 96, 45, 84, 73, 12, 87],
        ["Ratke Ltd", 60, 56, 89, 66, 76, 35, 41, 49, 19, 63, 47, 83],
        ["Willms-Mosciski", 98, 25, 74, 28, 87, 13, 99, 51, 52, 39, 30, 75],
        ["InaVerit", 64, 39, 67, 89, 60, 15, 63, 16, 45, 67, 46, 15],
    ]

    df = pd.DataFrame(data, columns=col)
    print("Original:")
    print(df)
    print("Unpivoted:")
    print(unpivot_df(df, "Year"))
