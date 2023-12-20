"""Module for getting sheet names from Excel files."""
from __future__ import annotations

import html
import logging
import re
import zipfile
from os import PathLike
from pathlib import Path
from typing import IO, TypeAlias

module_logger = logging.getLogger(__name__)
Pathish: TypeAlias = str | PathLike[str] | Path


def _get_xlsx_names(file_input: Path | IO[bytes]) -> list[str]:
    """Get sheet names from an xlsx file.

    Parameters
    ----------
        file_path : Path
            Path to xlsx file.

    Returns
    -------
    list[str]
        List of sheet names.
    """
    with zipfile.ZipFile(file_input, "r") as zip_ref:
        xml = zip_ref.read("xl/workbook.xml").decode("utf-8")
    return [
        html.unescape(sheet_name)
        for sheet_name in re.findall(r'<sheet.*?name="([^"]+?)".*?/>', xml)
    ]


def _get_xlsm_names(file_input: Path | IO[bytes]) -> list[str]:
    """Get sheet names from an xlsm/xlsb file.

    Parameters
    ----------
    file_path : Path
        Path to xlsm/xlsb file.

    Returns
    -------
    list[str]
        List of sheet names.
    """
    with zipfile.ZipFile(file_input, "r") as zip_ref:
        xml = zip_ref.read("docProps/app.xml").decode("utf-8")
    return [
        html.unescape(sheet_name)
        # Find all titles
        for titles in re.findall(r"<TitlesOfParts>(.+?)</TitlesOfParts>", xml)
        # Find all sheet names in titles
        for sheet_name in re.findall(r"<vt:lpstr>(.+?)</vt:lpstr>", titles)
    ]


def get_sheet_names(
    file_path: Pathish,
    open_handle: IO[bytes] | None = None,
) -> list[str]:
    """Get sheet names from an Excel file.

    Parameters
    ----------
    file_path: Path
        Path to Excel file.
    open_handle: IO[bytes] | None, optional
        Optional open handle for the file.

    Returns
    -------
    list[str]
        List of sheet names.

    Raises
    ------
    NotImplementedError
        If the file extension is not supported.
    """
    file_path = Path(file_path)
    match file_path.suffix:
        case ".xlsx":
            return _get_xlsx_names(open_handle or file_path)
        case ".xlsm" | ".xlsb":
            return _get_xlsm_names(open_handle or file_path)
        case suffix:
            msg = "Unsupported file extension '%s' found."
            raise NotImplementedError(msg, suffix)
