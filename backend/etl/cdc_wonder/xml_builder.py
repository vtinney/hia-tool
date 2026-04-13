"""Build CDC Wonder XML request bodies.

CDC Wonder's XML API is a POST-only endpoint that takes a fully-formed
XML request document. This builder emits only the fields needed for the
HIA baseline-rate pull: national-level mortality data filtered by a
single year, ICD-10 group, and a list of 10-year age groups.

NOTE: The CDC Wonder API restricts vital statistics queries to
national-level data only. County/state/region grouping and filtering
is NOT available through the API — only through the web interface.
See: https://wonder.cdc.gov/wonder/help/wonder-api.html

Parameter reference (from CDC Wonder official examples):
- B_   : Group-by ("By-variables") — cannot use location variables via API
- M_   : Measures (deaths, population, crude rate)
- F_   : Filter values for finder controls (hierarchical lists)
- I_   : "Currently selected" display text for finder controls
- V_   : Value selections (limiting/where-clause); empty string for
         variables controlled by F_ filters, *All* for others
- O_   : Output options (finder mode, radio buttons, precision, etc.)
- VM_  : Variable-measure cross-selections
- finder-stage- : Finder control state
- action-Send   : Triggers the query
- stage         : Request lifecycle stage
"""

from __future__ import annotations

from xml.sax.saxutils import escape


def _param(name: str, values: list[str]) -> str:
    """Render a single <parameter> element with one or more values."""
    value_xml = "".join(f"<value>{escape(v)}</value>" for v in values)
    return f"<parameter><name>{escape(name)}</name>{value_xml}</parameter>"


# Database metadata — variable mappings differ between D76 and D158.
_DB_META: dict[str, dict] = {
    "D76": {
        "label": "Underlying Cause of Death, 1999-2020",
        "vintage": "2020",
        "age_var": "V5",  # V5 = Ten-Year Age Groups in both databases
        # D76 also has race radio buttons (V42/V43/V44) and V25 finder
        "has_race_radio": True,
        # Variables controlled by finder controls (need F_, I_, finder-stage-, O_*_fmode)
        "finder_vars": ["V1", "V2", "V9", "V10", "V25", "V27"],
        # V_ parameters and their default values
        "v_params": {
            "V1": "", "V2": "", "V4": "*All*", "V5": "*All*",
            "V51": "*All*", "V52": "*All*", "V6": "00", "V7": "*All*",
            "V8": "*All*", "V9": "", "V10": "", "V11": "*All*",
            "V12": "*All*", "V17": "*All*", "V19": "*All*",
            "V20": "*All*", "V21": "*All*", "V22": "*All*",
            "V23": "*All*", "V24": "*All*", "V25": "", "V27": "",
        },
        # VM_ parameters for M6 measure
        "vm_params": {
            "M6_V10": "", "M6_V17": "*All*", "M6_V1_S": "*All*",
            "M6_V7": "*All*", "M6_V8": "*All*",
        },
    },
    "D158": {
        "label": "Underlying Cause of Death, 2018-2023, Single Race",
        "vintage": "2023",
        "age_var": "V5",  # V5 = Ten-Year Age Groups in both databases
        # D158 uses race radio buttons (V42/V43/V44); no V8
        "has_race_radio": True,
        # Finder-controlled variables for D158
        "finder_vars": ["V1", "V2", "V9", "V25"],
        # V_ parameters and their default values for D158
        "v_params": {
            "V1": "", "V2": "", "V4": "*All*", "V5": "*All*",
            "V51": "*All*", "V52": "*All*", "V6": "00", "V7": "*All*",
            "V9": "", "V10": "", "V11": "*All*", "V12": "*All*",
            "V17": "*All*", "V18": "*All*", "V19": "*All*",
            "V20": "*All*", "V21": "*All*", "V22": "*All*",
            "V23": "*All*", "V24": "*All*", "V25": "", "V27": "",
            "V30": "", "V31": "",
            "V42": "*All*", "V43": "*All*", "V44": "*All*", "V45": "*All*",
        },
        # VM_ parameters for M6 measure (D158 uses V42 instead of V8)
        "vm_params": {
            "M6_V10": "", "M6_V17": "*All*", "M6_V1_S": "*All*",
            "M6_V42": "*All*", "M6_V7": "*All*",
        },
    },
}


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

    Returns
    -------
    str
        Complete XML request document ready to POST.

    Notes
    -----
    The CDC Wonder API restricts vital statistics to national data only.
    County/state grouping is not available via the API.  Results are
    grouped by ten-year age group so downstream code can sum across the
    requested buckets.
    """
    if database not in _DB_META:
        raise ValueError(f"Unsupported database: {database}")

    meta = _DB_META[database]
    prefix = database
    age_var = meta["age_var"]

    params: list[str] = []

    # --- Data-use agreement (must be in the XML) ---
    params.append(_param("accept_datause_restrictions", ["true"]))

    # --- Group-by: ten-year age groups ---
    # Cannot group by location (county/state) via the API.
    # Group by age so we can filter/sum the specific age buckets.
    params.append(_param("B_1", [f"{prefix}.{age_var}"]))
    params.append(_param("B_2", ["*None*"]))
    params.append(_param("B_3", ["*None*"]))
    params.append(_param("B_4", ["*None*"]))
    params.append(_param("B_5", ["*None*"]))

    # --- Measures: Deaths, Population, Crude Rate ---
    params.append(_param("M_1", [f"{prefix}.M1"]))
    params.append(_param("M_2", [f"{prefix}.M2"]))
    params.append(_param("M_3", [f"{prefix}.M3"]))

    # --- F_ filters (finder control values) ---
    params.append(_param(f"F_{prefix}.V1", [str(year)]))
    params.append(_param(f"F_{prefix}.V2", icd_codes))
    params.append(_param(f"F_{prefix}.V9", ["*All*"]))
    # D158 has V25 as a finder var; D76 has V10, V27 as finder vars
    for fv in meta["finder_vars"]:
        if fv not in ("V1", "V2", "V9"):
            params.append(_param(f"F_{prefix}.{fv}", ["*All*"]))

    # --- I_ parameters: "currently selected" display text ---
    params.append(_param(f"I_{prefix}.V1", [f"{year} ({year})"]))
    icd_display = "\n".join(icd_codes)
    params.append(_param(f"I_{prefix}.V2", [icd_display]))
    params.append(_param(f"I_{prefix}.V9", ["*All* (The United States)"]))
    for fv in meta["finder_vars"]:
        if fv not in ("V1", "V2", "V9"):
            params.append(_param(f"I_{prefix}.{fv}", ["*All*"]))

    # --- V_ parameters: value selections for where clause ---
    for var_suffix, default_val in meta["v_params"].items():
        params.append(_param(f"V_{prefix}.{var_suffix}", [default_val]))

    # --- VM_ parameters: variable-measure cross-selections ---
    for vm_suffix, vm_val in meta["vm_params"].items():
        params.append(_param(f"VM_{prefix}.{vm_suffix.replace('_', f'_{prefix}.')}", [vm_val]))

    # --- O_ output options ---
    # Finder modes for all finder-controlled variables
    for fv in meta["finder_vars"]:
        params.append(_param(f"O_{fv}_fmode", ["freg"]))

    # Radio button selections
    params.append(_param("O_age", [f"{prefix}.{age_var}"]))
    params.append(_param("O_location", [f"{prefix}.V9"]))
    params.append(_param("O_urban", [f"{prefix}.V19"]))
    params.append(_param("O_ucd", [f"{prefix}.V2"]))

    # D158 has a race radio button (required); D76 does not
    if meta["has_race_radio"]:
        params.append(_param("O_race", [f"{prefix}.V42"]))

    # Age-adjusted rates disabled (we're grouping by age)
    params.append(_param("O_aar", ["aar_none"]))
    params.append(_param("O_aar_pop", ["0000"]))

    # General output options
    params.append(_param("O_javascript", ["on"]))
    params.append(_param("O_precision", ["1"]))
    params.append(_param("O_rate_per", ["100000"]))
    params.append(_param("O_show_totals", ["false"]))
    params.append(_param("O_show_suppressed", ["true"]))
    params.append(_param("O_show_zeros", ["true"]))
    params.append(_param("O_timeout", ["600"]))
    params.append(_param("O_title", ["HIA CDC Wonder pull"]))

    # --- Finder stage parameters ---
    for fv in meta["finder_vars"]:
        params.append(_param(f"finder-stage-{prefix}.{fv}", ["codeset"]))

    # --- Action and stage ---
    params.append(_param("action-Send", ["Send"]))
    params.append(_param("stage", ["request"]))

    body = "".join(params)
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f"<request-parameters>{body}</request-parameters>"
    )
