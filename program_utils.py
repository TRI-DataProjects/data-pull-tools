from dataclasses import dataclass
from enum import Enum

import numpy as np
import pandas as pd


def prog_has_rates_caps(df: pd.DataFrame, age_details: pd.DataFrame) -> pd.DataFrame:

    rate_columns = [
        x for x in age_details.columns if isinstance(x, str) and "Rate" in x
    ]
    non_rate_columns = [
        x for x in age_details.columns if isinstance(x, str) and "Rate" not in x
    ]

    rates = age_details[rate_columns].dropna(how="all")
    capacities = age_details[non_rate_columns].dropna(how="all")

    progs_with_rates = pd.Series(
        rates.reset_index()["Record ID"].unique(),
        name="Record ID",
    )
    progs_with_capacities = pd.Series(
        capacities.reset_index()["Record ID"].unique(),
        name="Record ID",
    )

    # Find programs that have rates
    df = pd.merge(
        left=df,
        how="left",
        right=progs_with_rates,
        on="Record ID",
        indicator=True,
    )
    df = df.drop_duplicates()

    df["Has Rate"] = df["_merge"] != "left_only"
    df = df[df["_merge"] != "right_only"]
    df = df.drop("_merge", axis=1)

    # Find programs that have capacities
    df = pd.merge(
        left=df,
        how="left",
        right=progs_with_capacities,
        on="Record ID",
        indicator=True,
    )
    df = df.drop_duplicates()

    df["Has Capacity"] = df["_merge"] != "left_only"
    df = df[df["_merge"] != "right_only"]
    df = df.drop("_merge", axis=1)

    return df


def invalid_programs_mask(df: pd.DataFrame, filter_status: bool = True) -> pd.Series:
    invalid_masks = {
        "Status": (df["Status"] != "Active"),
        "TEST In License": (
            df["License"].str.contains("TEST", regex=False, case=False)
        ),
        "DUPLICATE In License": (
            df["License"].str.contains("DUPLICATE", regex=False, case=False)
        ),
        "Early Learning Hub": (
            df["Provider Type"].str.contains(
                "Early Learning Hub", regex=False, case=False
            )
        ),
        "Empty Regulation": (df["Regulation"].isna()),
        "Empty Business Name": (df["Business Name"] == ", "),
    }

    if not filter_status:
        invalid_masks.pop("Status")

    mask = pd.Series([False] * len(df.index))
    for val in invalid_masks.values():
        mask |= val.fillna(False)

    return mask


def remove_invalid_programs(
    df: pd.DataFrame,
    filter_status: bool = True,
) -> pd.DataFrame:
    # Filter programs
    mask = invalid_programs_mask(
        df=df,
        filter_status=filter_status,
    )
    return df[~mask]


def type_code_programs(df: pd.DataFrame, dropna: bool = False) -> pd.DataFrame:
    class Col(str, Enum):
        PROV_TYPE = "Provider Type"
        PROG_TYPES = "Program Types"
        LIC = "License"
        REG = "Regulation"

    # Clean columns of interest
    for col in Col:
        df[col.value] = df[col.value].str.strip()

    # Create masks for re-use
    masks: dict[str, dict[str, pd.Series[bool]]] = {
        Col.PROV_TYPE: {
            "Licensed Home": df[Col.PROV_TYPE] == "Licensed Home",
            "Licensed Center": df[Col.PROV_TYPE] == "Licensed Center",
            "License Exempt Home": df[Col.PROV_TYPE] == "License Exempt Home",
            "License Exempt Center": df[Col.PROV_TYPE] == "License Exempt Center",
            "Interim Emergency Site": df[Col.PROV_TYPE] == "Interim Emergency Site",
        },
        Col.PROG_TYPES: {
            "PS": df[Col.PROG_TYPES].str.contains("Preschool", regex=False, case=False),
            "SA": df[Col.PROG_TYPES].str.contains(
                "School Age", regex=False, case=False
            ),
        },
        Col.LIC: {
            "CC": df[Col.LIC].str[:2].str.upper() == "CC",
            "CF": df[Col.LIC].str[:2].str.upper() == "CF",
            "RF": df[Col.LIC].str[:2].str.upper() == "RF",
            "RS": df[Col.LIC].str[:2].str.upper() == "RS",
            "PS": df[Col.LIC].str[:2].str.upper() == "PS",
            "SA": df[Col.LIC].str[:2].str.upper() == "SA",
            "RA": df[Col.LIC].str[:2].str.upper() == "RA",
            "AP": df[Col.LIC].str[:2].str.upper() == "AP",
            "IQY": df[Col.LIC].str[:3].str.upper() == "IQY",
        },
        Col.REG: {
            "CC": df[Col.REG] == "Licensed Child Care Center",
            "CF": df[Col.REG] == "Certified Family Child Care",
            "RF": df[Col.REG] == "Registered Family Child Care",
            "RS": df[Col.REG] == "Regulated Subsidy",
            "PS": df[Col.REG] == "Recorded Preschool Program",
            "SA": df[Col.REG] == "Recorded School Age Program",
            "RA": df[Col.REG] == "Recorded Agency",
            "UN": df[Col.REG] == "Unlicensed",
        },
    }

    # Clean masks by filling NA's
    for m_key, m_dict in masks.items():
        for key, val in m_dict.items():
            m_dict[key] = val.fillna(False)
        masks[m_key] = m_dict

    # Create conditions and replacements
    replacement_conditions: dict[str, pd.Series[bool]] = {
        "CC": (
            (masks[Col.PROV_TYPE]["Licensed Center"])
            & (masks[Col.LIC]["CC"])
            & (masks[Col.REG]["CC"])
        ),
        "CF": (
            (masks[Col.PROV_TYPE]["Licensed Home"])
            & (masks[Col.LIC]["CF"])
            & (masks[Col.REG]["CF"])
        ),
        "RF": (
            (masks[Col.PROV_TYPE]["Licensed Home"])
            & (masks[Col.LIC]["RF"])
            & (masks[Col.REG]["RF"])
        ),
        "RS": (
            (
                (masks[Col.PROV_TYPE]["License Exempt Home"])
                | (masks[Col.PROV_TYPE]["License Exempt Center"])
            )
            & ((masks[Col.LIC]["RS"]) | (masks[Col.LIC]["IQY"]))
            & (masks[Col.REG]["RS"])
        ),
        "PSR": (
            (masks[Col.PROV_TYPE]["License Exempt Center"])
            & (masks[Col.LIC]["PS"])
            & (masks[Col.REG]["PS"])
        ),
        "SAR": (
            (masks[Col.PROV_TYPE]["License Exempt Center"])
            & (masks[Col.LIC]["SA"])
            & (masks[Col.REG]["SA"])
        ),
        "RA": (
            (
                (masks[Col.PROV_TYPE]["License Exempt Center"])
                | (masks[Col.PROV_TYPE]["Interim Emergency Site"])
            )
            & (masks[Col.LIC]["RA"])
            & (masks[Col.REG]["RA"])
        ),
        "AP": (
            (masks[Col.PROV_TYPE]["License Exempt Center"])
            & (masks[Col.LIC]["AP"])
            & (masks[Col.REG]["UN"])
        ),
        "PSE": (
            (masks[Col.PROV_TYPE]["License Exempt Center"])
            & (masks[Col.PROG_TYPES]["PS"] & ~masks[Col.PROG_TYPES]["SA"])
            & (masks[Col.REG]["UN"])
        ),
        "SAE": (
            (masks[Col.PROV_TYPE]["License Exempt Center"])
            & (~masks[Col.PROG_TYPES]["PS"] & masks[Col.PROG_TYPES]["SA"])
            & (masks[Col.REG]["UN"])
        ),
        "CE": (
            (masks[Col.PROV_TYPE]["License Exempt Center"])
            & ~(masks[Col.PROG_TYPES]["PS"] ^ masks[Col.PROG_TYPES]["SA"])
            & (masks[Col.REG]["UN"])
        ),
        "FE": ((masks[Col.PROV_TYPE]["License Exempt Home"]) & (masks[Col.REG]["UN"])),
    }

    # Break into component parts for numpy
    replacements = list(replacement_conditions.keys())
    conditions = list(replacement_conditions.values())

    # Convert conditions from a boolean series to a boolean nupmy array
    for count, _ in enumerate(conditions):
        conditions[count] = np.array(conditions[count], dtype=bool)  # type: ignore

    # Apply type coding
    df["Type Code"] = ""
    df["Type Code"] = np.select(conditions, replacements, default=None)  # type: ignore

    # Remove N/A type codes
    if dropna:
        mask = df["Type Code"].isna()
        df = df[~mask]

    return df


@dataclass(frozen=True)
class Region_SDA:
    region: str
    sda: int


class SDA_Code(Enum):
    BLUE_MOUNTAIN = Region_SDA("Blue Mountain", 1)
    MULTNOMAH = Region_SDA("Multnomah", 2)
    MPY = Region_SDA("Marion-Polk-Yamhill", 3)
    NORTH_COAST = Region_SDA("North Coast", 4)
    LBL = Region_SDA("Linn-Benton-Lincoln", 5)
    LANE = Region_SDA("Lane", 6)
    SOUTH_CENTRAL = Region_SDA("South Central", 7)
    SOUTH_COAST = Region_SDA("South Coast", 8)
    SOURTHERN = Region_SDA("Southern", 9)
    THE_GORGE = Region_SDA("The Gorge", 10)
    GRANT_HARNEY = Region_SDA("Grant-Harney", 11)
    CENTRAL = Region_SDA("Central", 12)
    EASTERN = Region_SDA("Eastern", 14)
    CLACKAMAS = Region_SDA("Clackamas", 15)
    WASHINGTON = Region_SDA("Washington", 16)


def sda_code_programs(df: pd.DataFrame) -> pd.DataFrame:
    masks: dict[int, pd.Series[bool]] = {}
    for code in SDA_Code:
        sda = code.value.sda
        region = code.value.region
        masks[sda] = (df["Region"].str.strip() == region).fillna(False)

    # Break into component parts for numpy
    replacements = list(masks.keys())
    conditions = list(masks.values())

    # Convert conditions from a boolean series to a boolean nupmy array
    for count, _ in enumerate(conditions):
        conditions[count] = np.array(conditions[count], dtype=bool)  # type: ignore

    # Apply SDA coding
    df["SDA"] = ""
    df["SDA"] = np.select(conditions, replacements, default=None)  # type: ignore

    return df


class Program_Types(Enum):
    PS = "Preschool"
    HS = "Head Start (OPK)"
    EHS = "Early Head Start (OPK)"
    SAC = "School Age Before & After Care"
    BP = "Baby Promise"
    CCI = "CCI (Multnomah)"
    SAFT = "School Age Full-Time"
    PSP = "Preschool Promise"
    FOSTER = "Foster Care"
    NURSERY = "Relief Nursery"
    TEEN = "Teen Parent"
    TRIBAL = "Tribal"


def flag_program_types(df: pd.DataFrame) -> pd.DataFrame:
    df["Program Types"] = df["Program Types"].str.strip()
    for prog_type in Program_Types:
        mask = (
            df["Program Types"]
            .str.contains(prog_type.value, regex=False, case=False)
            .fillna(False)
        )
        df[prog_type.value] = mask
    return df


def care_for_ages_to_weeks(df: pd.DataFrame) -> pd.DataFrame:
    p_from = "Care For Ages From"
    p_to = "Care For Ages To"
    df[f"{p_from} Weeks"] = (df[f"{p_from} Months"] * 4) + (df[f"{p_from} Years"] * 52)
    df[f"{p_to} Weeks"] = (df[f"{p_to} Months"] * 4) + (df[f"{p_to} Years"] * 52)

    return df


@dataclass(frozen=True)
class Age_Range:
    label: str
    lower_bound: int
    upper_bound: int | None

    def to_col_name(self) -> str:
        label = self.label
        l_bound = self.lower_bound
        u_bound = self.upper_bound

        if u_bound is not None:
            return f"{label} ({l_bound}-{u_bound})"
        else:
            return f"{label} ({l_bound}+)"


class Age_Ranges(Enum):
    INFANT = Age_Range("Infant", 0, 104)
    TODDLER = Age_Range("Toddler", 104, 208)
    PRESCHOOL = Age_Range("Preschool", 208, 260)
    SCHOOL_AGE = Age_Range("School Age", 260, None)


def care_for_flag_from_weeks(df: pd.DataFrame) -> pd.DataFrame:
    from_weeks = "Care For Ages From Weeks"
    to_weeks = "Care For Ages To Weeks"

    valid_range_mask = ((df[to_weeks] != 0) & (df[from_weeks] != df[to_weeks])).fillna(
        False
    )

    df["Unknown Care Range"] = ~valid_range_mask

    for a_range in Age_Ranges:
        a_range = a_range.value
        l_bound = a_range.lower_bound
        u_bound = a_range.upper_bound

        mask = valid_range_mask.copy()

        mask &= (l_bound <= df[to_weeks]).fillna(False)
        if u_bound is not None:
            mask &= (df[from_weeks] < u_bound).fillna(False)

        df[a_range.to_col_name()] = mask

    return df
