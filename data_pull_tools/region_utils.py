from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True)
class Region:
    region: str
    sda: int
    counties: list[str]


class RegionEnum(Enum):
    BLUE_MOUNTAIN = Region(
        "Blue Mountain",
        1,
        ["Morrow", "Umatilla", "Union"],
    )
    MULTNOMAH = Region(
        "Multnomah",
        2,
        ["Multnomah"],
    )
    MPY = Region(
        "Marion-Polk-Yamhill",
        3,
        ["Marion", "Polk", "Yamhill"],
    )
    NORTH_COAST = Region(
        "North Coast",
        4,
        ["Clatsop", "Tillamook", "Columbia"],
    )
    LBL = Region(
        "Linn-Benton-Lincoln",
        5,
        ["Linn", "Benton", "Lincoln"],
    )
    LANE = Region(
        "Lane",
        6,
        ["Lane"],
    )
    SOUTH_CENTRAL = Region(
        "South Central",
        7,
        ["Douglas", "Klamath", "Lake"],
    )
    SOUTH_COAST = Region(
        "South Coast",
        8,
        ["Coos", "Curry"],
    )
    SOUTHERN = Region(
        "Southern",
        9,
        ["Jackson", "Josephine"],
    )
    THE_GORGE = Region(
        "The Gorge",
        10,
        ["Gilliam", "Hood River", "Sherman", "Wasco", "Wheeler"],
    )
    GRANT_HARNEY = Region(
        "Grant-Harney",
        11,
        ["Grant", "Harney"],
    )
    CENTRAL = Region(
        "Central",
        12,
        ["Crook", "Deschutes", "Jefferson"],
    )
    EASTERN = Region(
        "Eastern",
        14,
        ["Baker", "Malheur", "Wallowa"],
    )
    CLACKAMAS = Region(
        "Clackamas",
        15,
        ["Clackamas"],
    )
    WASHINGTON = Region(
        "Washington",
        16,
        ["Washington"],
    )
