import pandas as pd


def _get_company_quarterly_df() -> pd.DataFrame:
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

    return pd.DataFrame(data, columns=col)


company_quarterly_df: pd.DataFrame = _get_company_quarterly_df()
