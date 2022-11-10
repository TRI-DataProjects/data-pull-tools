from enum import Enum

import numpy as np
import pandas as pd


def remove_invalid_programs(progs: pd.DataFrame) -> pd.DataFrame:
    # Filter programs
    mask = (
        (progs["Status"] != "Active")
        | (progs["License"].str.contains("TEST", regex=False, case=False))
        | (progs["License"].str.contains("DUPLICATE", regex=False, case=False))
        | (
            progs["Provider Type"].str.contains(
                "Early Learning Hub", regex=False, case=False
            )
        )
        | (progs["Regulation"].isna())
        | (progs["Business Name"] == ", ")
    )

    return progs[~mask].copy()


def type_code_programs(progs: pd.DataFrame, dropna: bool = False) -> pd.DataFrame:
    class Col(str, Enum):
        PROV_TYPE = "Provider Type"
        PROG_TYPES = "Program Types"
        LIC = "License"
        REG = "Regulation"

    # Clean columns of interest
    for col in Col:
        progs[col.value] = progs[col.value].str.strip()

    # Create masks for re-use
    masks: dict[str, dict[str, pd.Series[bool]]] = {
        Col.PROV_TYPE: {
            "Licensed Home": progs[Col.PROV_TYPE] == "Licensed Home",
            "Licensed Center": progs[Col.PROV_TYPE] == "Licensed Center",
            "License Exempt Home": progs[Col.PROV_TYPE] == "License Exempt Home",
            "License Exempt Center": progs[Col.PROV_TYPE] == "License Exempt Center",
            "Interim Emergency Site": progs[Col.PROV_TYPE] == "Interim Emergency Site",
        },
        Col.PROG_TYPES: {
            "PS": progs[Col.PROG_TYPES].str.contains(
                "Preschool", regex=False, case=False
            ),
            "SA": progs[Col.PROG_TYPES].str.contains(
                "School Age", regex=False, case=False
            ),
        },
        Col.LIC: {
            "CC": progs[Col.LIC].str[:2].str.upper() == "CC",
            "CF": progs[Col.LIC].str[:2].str.upper() == "CF",
            "RF": progs[Col.LIC].str[:2].str.upper() == "RF",
            "RS": progs[Col.LIC].str[:2].str.upper() == "RS",
            "PS": progs[Col.LIC].str[:2].str.upper() == "PS",
            "SA": progs[Col.LIC].str[:2].str.upper() == "SA",
            "RA": progs[Col.LIC].str[:2].str.upper() == "RA",
            "AP": progs[Col.LIC].str[:2].str.upper() == "AP",
            "IQY": progs[Col.LIC].str[:3].str.upper() == "IQY",
        },
        Col.REG: {
            "CC": progs[Col.REG] == "Licensed Child Care Center",
            "CF": progs[Col.REG] == "Certified Family Child Care",
            "RF": progs[Col.REG] == "Registered Family Child Care",
            "RS": progs[Col.REG] == "Regulated Subsidy",
            "PS": progs[Col.REG] == "Recorded Preschool Program",
            "SA": progs[Col.REG] == "Recorded School Age Program",
            "RA": progs[Col.REG] == "Recorded Agency",
            "UN": progs[Col.REG] == "Unlicensed",
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
    progs["Type Code"] = ""
    progs["Type Code"] = np.select(conditions, replacements, default=None)  # type: ignore

    # Remove N/A type codes
    if dropna:
        mask = progs["Type Code"].isna()
        progs = progs[~mask]

    return progs.copy()
