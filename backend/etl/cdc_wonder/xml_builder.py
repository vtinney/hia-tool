"""Build CDC Wonder XML request bodies.

CDC Wonder's XML API is a POST-only endpoint that takes a fully-formed
XML request document. This builder emits only the fields needed for the
HIA baseline-rate pull: group by County, filter to a single year, single
ICD-10 group, and a list of 10-year age groups.
"""

from __future__ import annotations

from xml.sax.saxutils import escape


def _param(name: str, values: list[str]) -> str:
    """Render a single <parameter> element with one or more values."""
    value_xml = "".join(f"<value>{escape(v)}</value>" for v in values)
    return f"<parameter><name>{escape(name)}</name>{value_xml}</parameter>"


def build_request_xml(
    database: str,
    year: int,
    icd_codes: list[str],
    age_groups: list[str],
) -> str:
    """Build a CDC Wonder XML request body.

    Parameters
    ----------
    database : str
        Either "D76" (UCD 1999-2020) or "D158" (UCD 2018-2023).
    year : int
        Four-digit year.
    icd_codes : list[str]
        ICD-10 chapter/code specs (e.g. ["I20-I25"]).
    age_groups : list[str]
        10-year CDC Wonder age-group codes (e.g. ["25-34", "35-44"]).
    """
    if database not in ("D76", "D158"):
        raise ValueError(f"Unsupported database: {database}")

    prefix = database
    county_field = f"{prefix}.V1-level2"
    year_field = f"{prefix}.V1-level1"
    icd_field = f"{prefix}.V2"
    age_field = f"{prefix}.V51" if database == "D76" else f"{prefix}.V52"

    params: list[str] = []

    # Group-by: Year and County
    params.append(_param("B_1", [year_field]))
    params.append(_param("B_2", [county_field]))
    params.append(_param("B_3", ["*None*"]))
    params.append(_param("B_4", ["*None*"]))
    params.append(_param("B_5", ["*None*"]))

    # Measures: deaths and population
    params.append(_param("M_1", ["D76.M1"]))
    params.append(_param("M_2", ["D76.M2"]))

    # Filters
    params.append(_param(f"F_{year_field}", [str(year)]))
    params.append(_param(f"F_{icd_field}", icd_codes))
    params.append(_param(f"F_{age_field}", age_groups))

    # Value fields
    for name in ("V_D76.V1", "V_D76.V2", "V_D76.V5", "V_D76.V6", "V_D76.V7",
                 "V_D76.V8", "V_D76.V9", "V_D76.V10"):
        params.append(_param(name, ["*All*"]))

    # Options
    params.append(_param("O_javascript", ["on"]))
    params.append(_param("O_precision", ["1"]))
    params.append(_param("O_timeout", ["600"]))
    params.append(_param("O_title", ["HIA CDC Wonder pull"]))
    params.append(_param("O_show_totals", ["false"]))
    params.append(_param("O_show_suppressed", ["true"]))
    params.append(_param("O_show_zeros", ["true"]))
    params.append(_param("O_age", ["D76.V51" if database == "D76" else "D76.V52"]))

    # Data-use agreement
    params.append(_param("accept_datause_restrictions", ["true"]))

    body = "".join(params)
    return f'<?xml version="1.0" encoding="UTF-8"?><request-parameters>{body}</request-parameters>'
