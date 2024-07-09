from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar

import numpy as np
import pandas as pd

from data_pull_tools.region_utils import RegionEnum


def dhs_to_odhs_names(df: pd.DataFrame) -> pd.DataFrame:
    renamer = {
        name: name.replace("DHS", "ODHS")
        for name in df.columns
        if "DHS" in name and "ODHS" not in name
    }
    return df.rename(columns=renamer)


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
    df = df.merge(
        right=progs_with_rates,
        how="left",
        on="Record ID",
        indicator=True,
    ).drop_duplicates()

    df["Has Rate"] = df["_merge"] != "left_only"
    df = df[df["_merge"] != "right_only"]
    df = df.drop("_merge", axis=1)

    # Find programs that have capacities
    df = df.merge(
        right=progs_with_capacities,
        how="left",
        on="Record ID",
        indicator=True,
    ).drop_duplicates()

    df["Has Capacity"] = df["_merge"] != "left_only"
    df = df[df["_merge"] != "right_only"]

    return df.drop("_merge", axis=1)


def invalid_programs_mask(
    df: pd.DataFrame,
    filter_status: bool = True,
) -> "pd.Series[bool]":
    invalid_masks = {
        "Status": df["Status"] != "Active",
        "TEST In License": df["License"].str.contains(
            "TEST",
            regex=False,
            case=False,
        ),
        "DUPLICATE In License": df["License"].str.contains(
            "DUPLICATE",
            regex=False,
            case=False,
        ),
        "Early Learning Hub": df["Provider Type"].str.contains(
            "Early Learning Hub",
            regex=False,
            case=False,
        ),
        "Empty Regulation": df["Regulation"].isna(),
        "Empty Business Name": df["Business Name"] == ", ",
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
    mask = invalid_programs_mask(
        df=df,
        filter_status=filter_status,
    )
    return df[~mask]


def type_code_programs(df: pd.DataFrame, dropna: bool = False) -> pd.DataFrame:
    class Col(Enum):
        PROV_TYPE = "Provider Type"
        PROG_TYPES = "Program Types"
        LIC = "License"
        REG = "Regulation"

    # Clean columns of interest
    for col in Col:
        df[col.value] = df[col.value].str.strip()

    # Create masks for re-use
    masks: dict[Col, dict[str, pd.Series[bool]]] = {
        Col.PROV_TYPE: {
            "Licensed Home": (df[Col.PROV_TYPE.value] == "Licensed Home"),
            "Licensed Center": (df[Col.PROV_TYPE.value] == "Licensed Center"),
            "License Exempt Home": (df[Col.PROV_TYPE.value] == "License Exempt Home"),
            "License Exempt Center": (
                df[Col.PROV_TYPE.value] == "License Exempt Center"
            ),
            "Interim Emergency Site": (
                df[Col.PROV_TYPE.value] == "Interim Emergency Site"
            ),
        },
        Col.PROG_TYPES: {
            "PS": df[Col.PROG_TYPES.value].str.contains(
                "Preschool",
                regex=False,
                case=False,
            ),
            "SA": df[Col.PROG_TYPES.value].str.contains(
                "School Age",
                regex=False,
                case=False,
            ),
        },
        Col.LIC: {
            "CC": df[Col.LIC.value].str[:2].str.upper() == "CC",
            "CF": df[Col.LIC.value].str[:2].str.upper() == "CF",
            "RF": df[Col.LIC.value].str[:2].str.upper() == "RF",
            "RS": df[Col.LIC.value].str[:2].str.upper() == "RS",
            "PS": df[Col.LIC.value].str[:2].str.upper() == "PS",
            "SA": df[Col.LIC.value].str[:2].str.upper() == "SA",
            "RA": df[Col.LIC.value].str[:2].str.upper() == "RA",
            "AP": df[Col.LIC.value].str[:2].str.upper() == "AP",
            "IQY": df[Col.LIC.value].str[:3].str.upper() == "IQY",
        },
        Col.REG: {
            "CC": df[Col.REG.value] == "Licensed Child Care Center",
            "CF": df[Col.REG.value] == "Certified Family Child Care",
            "RF": df[Col.REG.value] == "Registered Family Child Care",
            "RS": df[Col.REG.value] == "Regulated Subsidy",
            "PS": df[Col.REG.value] == "Recorded Preschool Program",
            "SA": df[Col.REG.value] == "Recorded School Age Program",
            "RA": df[Col.REG.value] == "Recorded Agency",
            "UN": df[Col.REG.value] == "Unlicensed",
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
        "FE": (masks[Col.PROV_TYPE]["License Exempt Home"]) & (masks[Col.REG]["UN"]),
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


def sda_code_programs(df: pd.DataFrame) -> pd.DataFrame:
    masks: dict[int, pd.Series[bool]] = {}
    for region in RegionEnum:
        val = region.value
        sda = val.sda
        name = val.region
        masks[sda] = (df["Region"].str.strip() == name).fillna(False)

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


class ProgramType(Enum):
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
    for prog_type in ProgramType:
        mask = (
            df["Program Types"]
            .str.contains(prog_type.value, regex=False, case=False)
            .fillna(False)
        )
        df[prog_type.value] = mask
    return df


MONTHS_PER_YEAR = 12


def care_for_ages_total_months(df: pd.DataFrame) -> pd.DataFrame:
    p_from = "Care For Ages From"
    p_to = "Care For Ages To"
    df[f"{p_from} Total Months"] = (df[f"{p_from} Months"]) + (
        df[f"{p_from} Years"] * MONTHS_PER_YEAR
    )
    df[f"{p_to} Total Months"] = (df[f"{p_to} Months"]) + (
        df[f"{p_to} Years"] * MONTHS_PER_YEAR
    )

    return df


RangeType = TypeVar("RangeType", int, float)


@dataclass(frozen=True)
class NamedRange(Generic[RangeType]):
    label: str
    lower_bound: RangeType
    upper_bound: RangeType | None

    def __str__(self) -> str:
        label = self.label
        l_bound = self.lower_bound
        u_bound = self.upper_bound

        if u_bound is not None:
            return f"{label} ({l_bound}-{u_bound})"
        return f"{label} ({l_bound}+)"


class AgeRange(Enum):
    INFANT = NamedRange("Infant", 0, 2 * MONTHS_PER_YEAR)
    TODDLER = NamedRange("Toddler", 2 * MONTHS_PER_YEAR, 3 * MONTHS_PER_YEAR)
    PRESCHOOL = NamedRange("Preschool", 3 * MONTHS_PER_YEAR, 5 * MONTHS_PER_YEAR)
    SCHOOL_AGE = NamedRange("School Age", 5 * MONTHS_PER_YEAR, None)


def care_for_flag_from_total_months(df: pd.DataFrame) -> pd.DataFrame:
    from_months = "Care For Ages From Total Months"
    to_months = "Care For Ages To Total Months"

    valid_range_mask = (
        (df[to_months] != 0) & (df[from_months] != df[to_months])
    ).fillna(False)

    df["Unknown Care Range"] = ~valid_range_mask

    for range_member in AgeRange:
        a_range = range_member.value
        l_bound = a_range.lower_bound
        u_bound = a_range.upper_bound

        mask = valid_range_mask.copy()

        mask &= (l_bound <= df[to_months]).fillna(False)  # noqa: FBT003
        if u_bound is not None:
            mask &= (df[from_months] < u_bound).fillna(False)  # noqa: FBT003

        df[str(a_range)] = mask

    return df
