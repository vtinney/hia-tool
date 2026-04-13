"""Parse CDC Wonder TSV responses into tidy DataFrames.

CDC Wonder returns tab-separated text with a header row, one row per
grouped result, a "---" separator, and footer notes describing the query.
Small cells (1-9 deaths) come back as the literal string "Suppressed".
"""

from __future__ import annotations

import io

import pandas as pd


def parse_response(text: str) -> pd.DataFrame:
    """Parse a CDC Wonder TSV response body.

    Returns DataFrame with columns: fips (str, 5-digit), deaths (int), population (int).
    """
    lines = text.splitlines()
    data_lines: list[str] = []
    for line in lines:
        stripped = line.strip().strip('"')
        if stripped == "---":
            break
        data_lines.append(line)

    if not data_lines:
        return pd.DataFrame(columns=["fips", "deaths", "population"])

    raw = pd.read_csv(
        io.StringIO("\n".join(data_lines)),
        sep="\t",
        dtype=str,
        keep_default_na=False,
    )

    for col in raw.columns:
        raw[col] = raw[col].str.strip().str.strip('"')

    col_map = {}
    for col in raw.columns:
        lower = col.lower()
        if "county code" in lower:
            col_map[col] = "fips"
        elif lower == "deaths":
            col_map[col] = "deaths"
        elif lower == "population":
            col_map[col] = "population"
    df = raw.rename(columns=col_map)

    required = {"fips", "deaths", "population"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CDC Wonder response missing required columns: {sorted(missing)}. Got: {list(df.columns)}")

    df = df[["fips", "deaths", "population"]].copy()
    df["fips"] = df["fips"].str.zfill(5)

    def _to_int(value: str) -> int:
        if value is None:
            return 0
        v = value.strip()
        if not v or v.lower() == "suppressed" or v == "Missing":
            return 0
        try:
            return int(float(v))
        except ValueError:
            return 0

    df["deaths"] = df["deaths"].map(_to_int).astype("int32")
    df["population"] = df["population"].map(_to_int).astype("int32")
    df = df[df["fips"].str.match(r"^\d{5}$")].reset_index(drop=True)
    df["fips"] = df["fips"].astype(object)

    return df
