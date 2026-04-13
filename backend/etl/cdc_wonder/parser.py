"""Parse CDC Wonder XML responses into tidy DataFrames.

The CDC Wonder API returns XML containing a <data-table> element with
<r> (row) elements, each containing <c> (cell) elements. When grouped
by ten-year age groups, each row has: age-group label, deaths count,
population count, and crude rate.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET

import pandas as pd


def parse_response(text: str) -> pd.DataFrame:
    """Parse a CDC Wonder XML response body.

    Parameters
    ----------
    text : str
        Full XML response body from the CDC Wonder API.

    Returns
    -------
    pandas.DataFrame
        One row per age group, with columns:
        - ``age_group`` (str, e.g. "25-34 years")
        - ``deaths`` (int; suppressed/missing -> 0)
        - ``population`` (int; "Not Applicable"/missing -> 0)
    """
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return pd.DataFrame(columns=["age_group", "deaths", "population"])

    dt = root.find(".//data-table")
    if dt is None:
        return pd.DataFrame(columns=["age_group", "deaths", "population"])

    rows: list[dict] = []
    for r in dt.findall("r"):
        cells = r.findall("c")
        if len(cells) < 3:
            continue

        label = cells[0].get("l", "").strip()
        deaths_str = cells[1].get("v", "0").strip()
        pop_str = cells[2].get("v", "0").strip()

        rows.append({
            "age_group": label,
            "deaths": _to_int(deaths_str),
            "population": _to_int(pop_str),
        })

    if not rows:
        return pd.DataFrame(columns=["age_group", "deaths", "population"])

    df = pd.DataFrame(rows)
    df["deaths"] = df["deaths"].astype("int64")
    df["population"] = df["population"].astype("int64")

    # Drop "Not Stated" rows
    df = df[~df["age_group"].str.contains("Not Stated", case=False, na=False)]
    return df.reset_index(drop=True)


def _to_int(value: str) -> int:
    """Convert a CDC Wonder cell value to int, handling commas and special values."""
    if not value:
        return 0
    v = value.replace(",", "").strip()
    if v.lower() in ("suppressed", "not applicable", "missing", ""):
        return 0
    try:
        return int(float(v))
    except ValueError:
        return 0
