# CDC Wonder Baseline Mortality Rates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace hardcoded global mortality baseline rates in the HIA tool with US county-level rates sourced from CDC Wonder (2015–2023, 8 ICD-10 groups, 3 age buckets).

**Architecture:** A stand-alone ETL script fetches CDC Wonder TSVs via the XML-over-HTTP API, writes a tidy county-level parquet, and a new `baseline_rates` service looks up per-CRF y0 values that the compute router stamps onto CRFs before calling the (unchanged) HIA engine.

**Tech Stack:** Python 3.11+, `requests`, `pandas`, `pyarrow`, `numpy`, `pytest`, FastAPI (existing).

**Reference spec:** `docs/superpowers/specs/2026-04-09-cdc-wonder-baseline-rates-design.md`

---

## File Structure

**New files:**
- `backend/etl/process_cdc_wonder.py` — main ETL entry point; holds constants, orchestrates fetch + consolidate.
- `backend/etl/cdc_wonder/__init__.py` — package marker.
- `backend/etl/cdc_wonder/constants.py` — databases, years, ICD groups, age buckets, output paths.
- `backend/etl/cdc_wonder/xml_builder.py` — builds CDC Wonder XML request bodies from constants.
- `backend/etl/cdc_wonder/client.py` — thin HTTP client: rate-limited POST, retries, on-disk response caching.
- `backend/etl/cdc_wonder/parser.py` — parses CDC Wonder TSV responses into `pandas.DataFrame` rows.
- `backend/etl/cdc_wonder/consolidate.py` — reads every cached TSV, normalizes, joins to FIPS master list, writes parquets.
- `backend/services/baseline_rates.py` — public `get_baseline_rate` + static CRF→(ICD group, age bucket) mapping; lazy parquet load.
- `backend/tests/test_cdc_wonder_parser.py`
- `backend/tests/test_cdc_wonder_xml_builder.py`
- `backend/tests/test_cdc_wonder_client.py`
- `backend/tests/test_cdc_wonder_consolidate.py`
- `backend/tests/test_baseline_rates.py`
- `backend/tests/test_compute_router_with_cdc_rates.py`
- `backend/tests/fixtures/cdc_wonder/sample_response.tsv` — canned fixture.
- `backend/tests/fixtures/cdc_wonder/us_county_fips.csv` — 3-county fixture for baseline_rates tests.

**Modified files:**
- `backend/routers/compute.py` — add `countryCode` and `fipsCodes` to request models; call `get_baseline_rate` before dispatching to the engine / worker.

**Output data (not in git):**
- `data/raw/cdc_wonder/{database}/{year}/{icd_group}_{age_bucket}.tsv`
- `data/processed/incidence/us/cdc_wonder_mortality.parquet`
- `data/processed/incidence/us/cdc_wonder_mortality_state.parquet`

---

## Task 1: Scaffold the cdc_wonder package and constants

**Files:**
- Create: `backend/etl/cdc_wonder/__init__.py`
- Create: `backend/etl/cdc_wonder/constants.py`

- [ ] **Step 1: Create the package marker**

Create `backend/etl/cdc_wonder/__init__.py` with one line:

```python
"""CDC Wonder ETL sub-package."""
```

- [ ] **Step 2: Create the constants module**

Create `backend/etl/cdc_wonder/constants.py`:

```python
"""Constants for the CDC Wonder ETL.

Holds the databases, years, ICD-10 groups, and age buckets that define
the full matrix of 216 CDC Wonder queries for the HIA baseline rate pull.
"""

from __future__ import annotations

from pathlib import Path

# Database IDs used in the CDC Wonder XML API
DB_UCD_1999_2020 = "D76"  # Underlying Cause of Death, 1999-2020
DB_UCD_2018_2023 = "D158"  # Underlying Cause of Death, 2018-2023, Single Race

# Year → database routing
YEAR_TO_DB: dict[int, str] = {
    2015: DB_UCD_1999_2020,
    2016: DB_UCD_1999_2020,
    2017: DB_UCD_1999_2020,
    2018: DB_UCD_2018_2023,
    2019: DB_UCD_2018_2023,
    2020: DB_UCD_2018_2023,
    2021: DB_UCD_2018_2023,
    2022: DB_UCD_2018_2023,
    2023: DB_UCD_2018_2023,
}

YEARS: list[int] = sorted(YEAR_TO_DB.keys())

# ICD-10 groups. Each entry maps a group name to a list of ICD-10
# chapter/code specifications accepted by the CDC Wonder API.
# "all_cause_nonaccidental" is synthesized as A00-R99 (excluding
# external causes S00-Y89), matching BenMAP/HRAPIE convention.
ICD_GROUPS: dict[str, list[str]] = {
    "all_cause": ["A00-Y89"],
    "all_cause_nonaccidental": ["A00-R99"],
    "cvd": ["I00-I99"],
    "ihd": ["I20-I25"],
    "stroke": ["I60-I69"],
    "respiratory": ["J00-J99"],
    "copd": ["J40-J44"],
    "lung_cancer": ["C33-C34"],
    "lri": ["J09-J22"],
}

# Age buckets and the 10-year CDC Wonder age-group codes they cover.
# CDC Wonder's 10-year age group codes: "1" = <1yr, "1-4", "5-14",
# "15-24", "25-34", "35-44", "45-54", "55-64", "65-74", "75-84", "85+".
AGE_BUCKETS: dict[str, list[str]] = {
    "all": [
        "1", "1-4", "5-14", "15-24",
        "25-34", "35-44", "45-54", "55-64",
        "65-74", "75-84", "85+",
    ],
    "25plus": [
        "25-34", "35-44", "45-54", "55-64",
        "65-74", "75-84", "85+",
    ],
    "65plus": ["65-74", "75-84", "85+"],
}

# On-disk layout
RAW_DIR = Path("data/raw/cdc_wonder")
PROCESSED_DIR = Path("data/processed/incidence/us")
COUNTY_PARQUET = PROCESSED_DIR / "cdc_wonder_mortality.parquet"
STATE_PARQUET = PROCESSED_DIR / "cdc_wonder_mortality_state.parquet"

# HTTP
CDC_WONDER_URL = "https://wonder.cdc.gov/controller/datarequest/{db}"
REQUEST_DELAY_SECONDS = 1.0
MAX_RETRIES = 5
```

- [ ] **Step 3: Commit**

```bash
git add backend/etl/cdc_wonder/__init__.py backend/etl/cdc_wonder/constants.py
git commit -m "feat(cdc-wonder): scaffold ETL package with constants"
```

---

## Task 2: TSV parser — test first

**Files:**
- Create: `backend/tests/fixtures/cdc_wonder/sample_response.tsv`
- Create: `backend/tests/test_cdc_wonder_parser.py`
- Create: `backend/etl/cdc_wonder/parser.py`

- [ ] **Step 1: Create the canned fixture**

Create `backend/tests/fixtures/cdc_wonder/sample_response.tsv` with realistic CDC Wonder export content. CDC Wonder returns TSV with a "Notes" column, suppressed cells as the literal string `Suppressed`, a trailing `---` separator row before aggregates, and footer notes after that separator. Our parser must ignore the footer.

```
"Notes"	"County"	"County Code"	"Deaths"	"Population"	"Crude Rate"
	"Autauga County, AL"	"01001"	"45"	"55200"	"81.5"
	"Baldwin County, AL"	"01003"	"210"	"218022"	"96.3"
	"Barbour County, AL"	"01005"	"Suppressed"	"24881"	"Suppressed"
"---"
"Dataset: Underlying Cause of Death, 1999-2020"
"Query Parameters:"
"Group By: County"
```

(Use literal tab characters, not spaces. Enclose the whole fixture in a file with a trailing newline.)

- [ ] **Step 2: Write failing tests**

Create `backend/tests/test_cdc_wonder_parser.py`:

```python
"""Tests for the CDC Wonder TSV parser."""

from pathlib import Path

import pandas as pd

from backend.etl.cdc_wonder.parser import parse_response

FIXTURE = Path(__file__).parent / "fixtures" / "cdc_wonder" / "sample_response.tsv"


def test_parse_response_returns_dataframe():
    df = parse_response(FIXTURE.read_text())
    assert isinstance(df, pd.DataFrame)


def test_parse_response_row_count_excludes_footer():
    df = parse_response(FIXTURE.read_text())
    # 3 data rows; footer notes are dropped
    assert len(df) == 3


def test_parse_response_columns():
    df = parse_response(FIXTURE.read_text())
    assert set(df.columns) >= {"fips", "deaths", "population"}


def test_parse_response_suppressed_becomes_zero():
    df = parse_response(FIXTURE.read_text())
    row = df[df["fips"] == "01005"].iloc[0]
    assert row["deaths"] == 0
    # Population is never suppressed in practice, but fixture uses "Suppressed"
    # for the crude rate column; we don't carry crude rate, we recompute.


def test_parse_response_fips_is_five_digit_string():
    df = parse_response(FIXTURE.read_text())
    assert df["fips"].dtype == object
    for fips in df["fips"]:
        assert isinstance(fips, str)
        assert len(fips) == 5


def test_parse_response_numeric_types():
    df = parse_response(FIXTURE.read_text())
    assert pd.api.types.is_integer_dtype(df["deaths"])
    assert pd.api.types.is_integer_dtype(df["population"])
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest backend/tests/test_cdc_wonder_parser.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backend.etl.cdc_wonder.parser'`

- [ ] **Step 4: Implement the parser**

Create `backend/etl/cdc_wonder/parser.py`:

```python
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

    Parameters
    ----------
    text : str
        Full TSV response body from the CDC Wonder XML API.

    Returns
    -------
    pandas.DataFrame
        One row per county, with columns:
        - ``fips`` (str, 5-digit zero-padded)
        - ``deaths`` (int; ``Suppressed`` → 0)
        - ``population`` (int; ``Suppressed`` → 0)
    """
    # Drop the footer: everything from the "---" separator onward.
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

    # CDC Wonder sometimes quotes every field; strip surrounding quotes
    # from every column.
    for col in raw.columns:
        raw[col] = raw[col].str.strip().str.strip('"')

    # Column names vary slightly across databases. Normalize.
    col_map = {}
    for col in raw.columns:
        lower = col.lower()
        if "county code" in lower or lower == "county code":
            col_map[col] = "fips"
        elif lower == "deaths":
            col_map[col] = "deaths"
        elif lower == "population":
            col_map[col] = "population"
    df = raw.rename(columns=col_map)

    required = {"fips", "deaths", "population"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"CDC Wonder response missing required columns: {sorted(missing)}. "
            f"Got: {list(df.columns)}"
        )

    df = df[["fips", "deaths", "population"]].copy()

    # Zero-pad FIPS to 5 digits.
    df["fips"] = df["fips"].str.zfill(5)

    # Coerce suppressed / missing / non-numeric values to 0.
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

    # Drop any rows with an empty/invalid FIPS (e.g. totals rows).
    df = df[df["fips"].str.match(r"^\d{5}$")].reset_index(drop=True)

    return df
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest backend/tests/test_cdc_wonder_parser.py -v`
Expected: all 6 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add backend/etl/cdc_wonder/parser.py backend/tests/test_cdc_wonder_parser.py backend/tests/fixtures/cdc_wonder/sample_response.tsv
git commit -m "feat(cdc-wonder): TSV parser with suppressed-cell handling"
```

---

## Task 3: XML request builder — test first

**Files:**
- Create: `backend/tests/test_cdc_wonder_xml_builder.py`
- Create: `backend/etl/cdc_wonder/xml_builder.py`

- [ ] **Step 1: Write failing tests**

Create `backend/tests/test_cdc_wonder_xml_builder.py`:

```python
"""Tests for the CDC Wonder XML request body builder."""

from backend.etl.cdc_wonder.xml_builder import build_request_xml
from backend.etl.cdc_wonder.constants import DB_UCD_2018_2023, DB_UCD_1999_2020


def test_build_request_returns_string():
    xml = build_request_xml(
        database=DB_UCD_2018_2023,
        year=2019,
        icd_codes=["I00-I99"],
        age_groups=["25-34", "35-44"],
    )
    assert isinstance(xml, str)
    assert xml.startswith("<?xml")


def test_build_request_contains_group_by_county():
    xml = build_request_xml(
        database=DB_UCD_2018_2023,
        year=2019,
        icd_codes=["I00-I99"],
        age_groups=["25-34"],
    )
    # CDC Wonder uses "B_1" / "B_2" etc. for group-by fields, with
    # county = "D158.V1-level2" (for D158) or "D76.V1-level2" (for D76).
    assert "county" in xml.lower() or "V1-level2" in xml


def test_build_request_contains_year():
    xml = build_request_xml(
        database=DB_UCD_2018_2023,
        year=2022,
        icd_codes=["I00-I99"],
        age_groups=["25-34"],
    )
    assert "2022" in xml


def test_build_request_contains_icd_codes():
    xml = build_request_xml(
        database=DB_UCD_2018_2023,
        year=2019,
        icd_codes=["I20-I25"],
        age_groups=["25-34"],
    )
    assert "I20-I25" in xml


def test_build_request_contains_all_age_groups():
    xml = build_request_xml(
        database=DB_UCD_2018_2023,
        year=2019,
        icd_codes=["I00-I99"],
        age_groups=["25-34", "35-44", "45-54"],
    )
    for age in ["25-34", "35-44", "45-54"]:
        assert age in xml


def test_build_request_accepts_assurance_flag():
    """The CDC Wonder API requires an 'accept_datause_restrictions' flag."""
    xml = build_request_xml(
        database=DB_UCD_2018_2023,
        year=2019,
        icd_codes=["I00-I99"],
        age_groups=["25-34"],
    )
    assert "accept_datause_restrictions" in xml
    assert "true" in xml.lower()


def test_build_request_d76_uses_d76_field_names():
    xml = build_request_xml(
        database=DB_UCD_1999_2020,
        year=2016,
        icd_codes=["I00-I99"],
        age_groups=["25-34"],
    )
    assert "D76." in xml


def test_build_request_d158_uses_d158_field_names():
    xml = build_request_xml(
        database=DB_UCD_2018_2023,
        year=2019,
        icd_codes=["I00-I99"],
        age_groups=["25-34"],
    )
    assert "D158." in xml
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest backend/tests/test_cdc_wonder_xml_builder.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement the XML builder**

Create `backend/etl/cdc_wonder/xml_builder.py`:

```python
"""Build CDC Wonder XML request bodies.

CDC Wonder's XML API is a POST-only endpoint that takes a fully-formed
XML request document describing: (1) which database, (2) group-by
fields, (3) measure selections, (4) filter values, and (5) various
output options. The schema is documented at
https://wonder.cdc.gov/wonder/help/WONDER-API.html.

This builder emits only the fields we need for the HIA baseline-rate
pull: group by County, filter to a single year, single ICD-10 group,
and a list of 10-year age groups. Everything else gets default values.
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
        Either ``"D76"`` (UCD 1999-2020) or ``"D158"`` (UCD 2018-2023).
    year : int
        Four-digit year. Must fall within the database's supported range.
    icd_codes : list[str]
        ICD-10 chapter/code specs (e.g. ``["I20-I25"]``).
    age_groups : list[str]
        10-year CDC Wonder age-group codes (e.g. ``["25-34", "35-44"]``).

    Returns
    -------
    str
        Full XML request body suitable for POSTing to the CDC Wonder
        data-request endpoint.
    """
    if database not in ("D76", "D158"):
        raise ValueError(f"Unsupported database: {database}")

    # Field prefixes differ between databases.
    # D76 fields are named D76.Vn; D158 fields are D158.Vn.
    # Group-by fields: county is V1-level2 on both databases.
    # Year field: V1-level1 (on D76) / V1-level1 (on D158).
    # ICD chapter/group field: D76.V2 / D158.V2.
    # 10-year age group: D76.V51 / D158.V52.
    prefix = database
    county_field = f"{prefix}.V1-level2"
    year_field = f"{prefix}.V1-level1"
    icd_field = f"{prefix}.V2"
    age_field = f"{prefix}.V51" if database == "D76" else f"{prefix}.V52"

    params: list[str] = []

    # Group-by: Year (to keep the year column) and County (primary grouping).
    params.append(_param("B_1", [year_field]))
    params.append(_param("B_2", [county_field]))
    params.append(_param("B_3", ["*None*"]))
    params.append(_param("B_4", ["*None*"]))
    params.append(_param("B_5", ["*None*"]))

    # Measures: deaths and population (crude rate is recomputed locally).
    params.append(_param("M_1", ["D76.M1"]))  # Deaths
    params.append(_param("M_2", ["D76.M2"]))  # Population

    # Filters.
    params.append(_param(f"F_{year_field}", [str(year)]))
    params.append(_param(f"F_{icd_field}", icd_codes))
    params.append(_param(f"F_{age_field}", age_groups))

    # Value fields (finer groupings we are *not* using) need empty values.
    for name in ("V_D76.V1", "V_D76.V2", "V_D76.V5", "V_D76.V6", "V_D76.V7",
                 "V_D76.V8", "V_D76.V9", "V_D76.V10"):
        params.append(_param(name, ["*All*"]))

    # Options.
    params.append(_param("O_javascript", ["on"]))
    params.append(_param("O_precision", ["1"]))
    params.append(_param("O_timeout", ["600"]))
    params.append(_param("O_title", ["HIA CDC Wonder pull"]))
    params.append(_param("O_show_totals", ["false"]))
    params.append(_param("O_show_suppressed", ["true"]))
    params.append(_param("O_show_zeros", ["true"]))
    params.append(_param("O_age", ["D76.V51" if database == "D76" else "D76.V52"]))

    # Data-use agreement. Required by the API.
    params.append(_param("accept_datause_restrictions", ["true"]))

    body = "".join(params)
    return f'<?xml version="1.0" encoding="UTF-8"?><request-parameters>{body}</request-parameters>'
```

*Implementation note for the executing engineer:* CDC Wonder's XML parameter names and field codes are finicky and occasionally change. Treat this builder as a starting point — if the first few live queries fail with `Invalid request` or `Unknown parameter`, fetch a working request body from the CDC Wonder web form (Chrome DevTools → Network tab → copy the XML payload) and diff it against what this builder emits. The test in Task 9 Step 3 runs one real query end-to-end against the live API as the final verification.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest backend/tests/test_cdc_wonder_xml_builder.py -v`
Expected: all 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/etl/cdc_wonder/xml_builder.py backend/tests/test_cdc_wonder_xml_builder.py
git commit -m "feat(cdc-wonder): XML request body builder"
```

---

## Task 4: HTTP client with caching and rate limiting — test first

**Files:**
- Create: `backend/tests/test_cdc_wonder_client.py`
- Create: `backend/etl/cdc_wonder/client.py`

- [ ] **Step 1: Write failing tests**

Create `backend/tests/test_cdc_wonder_client.py`:

```python
"""Tests for the CDC Wonder HTTP client."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from backend.etl.cdc_wonder.client import CdcWonderClient


@pytest.fixture
def tmp_cache(tmp_path: Path) -> Path:
    return tmp_path / "raw"


def _mock_response(text: str, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.text = text
    resp.ok = status < 400
    return resp


def test_client_caches_response_to_disk(tmp_cache: Path):
    client = CdcWonderClient(cache_root=tmp_cache, request_delay=0)
    with patch("backend.etl.cdc_wonder.client.requests.post") as mock_post:
        mock_post.return_value = _mock_response("BODY")
        body = client.fetch(
            database="D158",
            year=2019,
            icd_group="cvd",
            age_bucket="25plus",
            xml_body="<req/>",
        )
        assert body == "BODY"

    cached = tmp_cache / "D158" / "2019" / "cvd_25plus.tsv"
    assert cached.exists()
    assert cached.read_text() == "BODY"


def test_client_skips_http_when_cache_exists(tmp_cache: Path):
    cached = tmp_cache / "D158" / "2019" / "cvd_25plus.tsv"
    cached.parent.mkdir(parents=True)
    cached.write_text("CACHED")

    client = CdcWonderClient(cache_root=tmp_cache, request_delay=0)
    with patch("backend.etl.cdc_wonder.client.requests.post") as mock_post:
        body = client.fetch(
            database="D158",
            year=2019,
            icd_group="cvd",
            age_bucket="25plus",
            xml_body="<req/>",
        )
        assert body == "CACHED"
        mock_post.assert_not_called()


def test_client_retries_on_429(tmp_cache: Path):
    client = CdcWonderClient(cache_root=tmp_cache, request_delay=0, max_retries=3)
    responses = [
        _mock_response("", status=429),
        _mock_response("", status=429),
        _mock_response("OK"),
    ]
    with patch("backend.etl.cdc_wonder.client.requests.post") as mock_post:
        mock_post.side_effect = responses
        with patch("backend.etl.cdc_wonder.client.time.sleep"):  # skip real waits
            body = client.fetch(
                database="D158",
                year=2019,
                icd_group="cvd",
                age_bucket="25plus",
                xml_body="<req/>",
            )
        assert body == "OK"
        assert mock_post.call_count == 3


def test_client_raises_after_max_retries(tmp_cache: Path):
    client = CdcWonderClient(cache_root=tmp_cache, request_delay=0, max_retries=2)
    with patch("backend.etl.cdc_wonder.client.requests.post") as mock_post:
        mock_post.return_value = _mock_response("", status=500)
        with patch("backend.etl.cdc_wonder.client.time.sleep"):
            with pytest.raises(RuntimeError, match="CDC Wonder request failed"):
                client.fetch(
                    database="D158",
                    year=2019,
                    icd_group="cvd",
                    age_bucket="25plus",
                    xml_body="<req/>",
                )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest backend/tests/test_cdc_wonder_client.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement the client**

Create `backend/etl/cdc_wonder/client.py`:

```python
"""Thin HTTP client for the CDC Wonder XML API.

Handles:
- POSTing a prebuilt XML body
- Rate-limiting to one request per second by default
- Exponential-backoff retry on 429 and 5xx responses
- Optional on-disk caching so re-runs skip already-fetched combinations
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import requests

from backend.etl.cdc_wonder.constants import (
    CDC_WONDER_URL,
    MAX_RETRIES,
    REQUEST_DELAY_SECONDS,
)

logger = logging.getLogger("cdc_wonder.client")


class CdcWonderClient:
    """POST-and-cache client for the CDC Wonder XML API."""

    def __init__(
        self,
        cache_root: Path,
        request_delay: float = REQUEST_DELAY_SECONDS,
        max_retries: int = MAX_RETRIES,
    ) -> None:
        self.cache_root = Path(cache_root)
        self.request_delay = request_delay
        self.max_retries = max_retries

    def _cache_path(
        self, database: str, year: int, icd_group: str, age_bucket: str
    ) -> Path:
        return (
            self.cache_root
            / database
            / str(year)
            / f"{icd_group}_{age_bucket}.tsv"
        )

    def fetch(
        self,
        *,
        database: str,
        year: int,
        icd_group: str,
        age_bucket: str,
        xml_body: str,
    ) -> str:
        """Fetch a CDC Wonder query, returning the raw TSV body.

        Reads from the on-disk cache if present; otherwise POSTs, caches,
        and returns the response body.
        """
        cached = self._cache_path(database, year, icd_group, age_bucket)
        if cached.exists():
            logger.debug("cache hit: %s", cached)
            return cached.read_text()

        url = CDC_WONDER_URL.format(db=database)
        headers = {"Content-Type": "application/xml"}
        delay = self.request_delay

        last_error: str | None = None
        for attempt in range(1, self.max_retries + 1):
            if attempt > 1:
                backoff = delay * (2 ** (attempt - 1))
                logger.warning(
                    "retry %d/%d after %.1fs (reason: %s)",
                    attempt, self.max_retries, backoff, last_error,
                )
                time.sleep(backoff)
            else:
                time.sleep(delay)

            resp = requests.post(url, data=xml_body, headers=headers, timeout=120)
            if resp.ok:
                cached.parent.mkdir(parents=True, exist_ok=True)
                cached.write_text(resp.text)
                return resp.text
            last_error = f"HTTP {resp.status_code}"
            if resp.status_code not in (429, 500, 502, 503, 504):
                break

        raise RuntimeError(
            f"CDC Wonder request failed after {self.max_retries} attempts: "
            f"{last_error} (db={database} year={year} "
            f"icd={icd_group} age={age_bucket})"
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest backend/tests/test_cdc_wonder_client.py -v`
Expected: all 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/etl/cdc_wonder/client.py backend/tests/test_cdc_wonder_client.py
git commit -m "feat(cdc-wonder): HTTP client with caching and retries"
```

---

## Task 5: Consolidation — test first

**Files:**
- Create: `backend/tests/test_cdc_wonder_consolidate.py`
- Create: `backend/etl/cdc_wonder/consolidate.py`

- [ ] **Step 1: Write failing tests**

Create `backend/tests/test_cdc_wonder_consolidate.py`:

```python
"""Tests for the CDC Wonder consolidation step."""

from pathlib import Path

import pandas as pd

from backend.etl.cdc_wonder.consolidate import consolidate


def _write_tsv(path: Path, rows: list[tuple[str, str, str]]) -> None:
    """Write a minimal CDC Wonder TSV fixture with the given county rows."""
    header = '"Notes"\t"County"\t"County Code"\t"Deaths"\t"Population"\t"Crude Rate"'
    body = "\n".join(
        f'\t"{name}"\t"{fips}"\t"{deaths}"\t"{pop}"\t"0"'
        for fips, name, (deaths, pop) in rows
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{header}\n{body}\n---\n")


def test_consolidate_writes_long_county_parquet(tmp_path: Path):
    raw_root = tmp_path / "raw"
    out_county = tmp_path / "county.parquet"
    out_state = tmp_path / "state.parquet"

    _write_tsv(
        raw_root / "D158" / "2019" / "cvd_25plus.tsv",
        [("01001", "Autauga, AL", ("10", "40000"))],
    )
    _write_tsv(
        raw_root / "D158" / "2019" / "ihd_25plus.tsv",
        [("01001", "Autauga, AL", ("5", "40000"))],
    )

    consolidate(
        raw_root=raw_root,
        county_parquet=out_county,
        state_parquet=out_state,
        master_fips=["01001", "01003"],
    )

    df = pd.read_parquet(out_county)

    # One row per (fips, year, icd_group, age_bucket) combination
    # including the missing-county fill.
    assert set(df.columns) == {
        "fips", "state_fips", "year", "icd_group",
        "age_bucket", "deaths", "population", "rate_per_person_year",
    }
    # 2 counties × 2 icd groups × 1 year × 1 age bucket = 4 rows
    assert len(df) == 4

    # Missing county 01003 filled with zeros
    filled = df[df["fips"] == "01003"]
    assert (filled["deaths"] == 0).all()
    assert (filled["rate_per_person_year"] == 0).all()

    # Real row rates are computed correctly
    cvd_01001 = df[(df["fips"] == "01001") & (df["icd_group"] == "cvd")].iloc[0]
    assert cvd_01001["deaths"] == 10
    assert cvd_01001["population"] == 40000
    assert abs(cvd_01001["rate_per_person_year"] - 10 / 40000) < 1e-12


def test_consolidate_state_rollup(tmp_path: Path):
    raw_root = tmp_path / "raw"
    out_county = tmp_path / "county.parquet"
    out_state = tmp_path / "state.parquet"

    _write_tsv(
        raw_root / "D158" / "2019" / "cvd_25plus.tsv",
        [
            ("01001", "Autauga, AL", ("10", "40000")),
            ("01003", "Baldwin, AL", ("20", "60000")),
        ],
    )

    consolidate(
        raw_root=raw_root,
        county_parquet=out_county,
        state_parquet=out_state,
        master_fips=["01001", "01003"],
    )

    state = pd.read_parquet(out_state)
    row = state[
        (state["state_fips"] == "01") & (state["icd_group"] == "cvd")
    ].iloc[0]
    assert row["deaths"] == 30
    assert row["population"] == 100000
    assert abs(row["rate_per_person_year"] - 30 / 100000) < 1e-12
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest backend/tests/test_cdc_wonder_consolidate.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement consolidation**

Create `backend/etl/cdc_wonder/consolidate.py`:

```python
"""Consolidate cached CDC Wonder TSVs into tidy parquet files."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from backend.etl.cdc_wonder.parser import parse_response

logger = logging.getLogger("cdc_wonder.consolidate")


def _iter_cached(raw_root: Path):
    """Yield (database, year, icd_group, age_bucket, tsv_text) tuples."""
    if not raw_root.exists():
        return
    for db_dir in sorted(raw_root.iterdir()):
        if not db_dir.is_dir():
            continue
        for year_dir in sorted(db_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            try:
                year = int(year_dir.name)
            except ValueError:
                continue
            for tsv_path in sorted(year_dir.glob("*.tsv")):
                stem = tsv_path.stem  # "<icd_group>_<age_bucket>"
                if "_" not in stem:
                    continue
                icd_group, age_bucket = stem.rsplit("_", 1)
                yield db_dir.name, year, icd_group, age_bucket, tsv_path.read_text()


def consolidate(
    *,
    raw_root: Path,
    county_parquet: Path,
    state_parquet: Path,
    master_fips: list[str],
) -> None:
    """Build the tidy county and state parquets from cached raw TSVs.

    Parameters
    ----------
    raw_root : Path
        Directory containing ``{database}/{year}/{icd}_{age}.tsv`` files.
    county_parquet, state_parquet : Path
        Output parquet destinations.
    master_fips : list[str]
        Full list of 5-digit county FIPS codes the output should cover.
        Counties missing from CDC Wonder responses are filled with zeros.
    """
    rows: list[pd.DataFrame] = []
    for database, year, icd_group, age_bucket, text in _iter_cached(raw_root):
        parsed = parse_response(text)
        if parsed.empty:
            continue
        parsed["year"] = year
        parsed["icd_group"] = icd_group
        parsed["age_bucket"] = age_bucket
        rows.append(parsed)

    if not rows:
        raise RuntimeError(
            f"No cached CDC Wonder TSVs found under {raw_root}. "
            "Run the fetch step first."
        )

    df = pd.concat(rows, ignore_index=True)

    # Determine the full (year, icd_group, age_bucket) × master_fips grid
    # and left-join so missing counties come through as NaN → 0.
    combos = df[["year", "icd_group", "age_bucket"]].drop_duplicates()
    master = pd.DataFrame({"fips": master_fips})
    master["key"] = 1
    combos["key"] = 1
    grid = master.merge(combos, on="key").drop(columns="key")

    merged = grid.merge(
        df, on=["fips", "year", "icd_group", "age_bucket"], how="left"
    )
    merged["deaths"] = merged["deaths"].fillna(0).astype("int32")
    merged["population"] = merged["population"].fillna(0).astype("int32")
    merged["state_fips"] = merged["fips"].str[:2]
    merged["year"] = merged["year"].astype("int16")
    merged["icd_group"] = merged["icd_group"].astype("category")
    merged["age_bucket"] = merged["age_bucket"].astype("category")

    rate = merged["deaths"].astype("float64") / merged["population"].replace(
        0, pd.NA
    )
    merged["rate_per_person_year"] = rate.fillna(0).astype("float32")

    out = merged[[
        "fips", "state_fips", "year", "icd_group",
        "age_bucket", "deaths", "population", "rate_per_person_year",
    ]]

    county_parquet.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(county_parquet, index=False)
    logger.info(
        "wrote %d county rows to %s", len(out), county_parquet,
    )

    # State-level rollup: sum counts, recompute rate.
    state = (
        out.groupby(
            ["state_fips", "year", "icd_group", "age_bucket"],
            observed=True,
        )
        .agg(deaths=("deaths", "sum"), population=("population", "sum"))
        .reset_index()
    )
    state_rate = state["deaths"].astype("float64") / state["population"].replace(
        0, pd.NA
    )
    state["rate_per_person_year"] = state_rate.fillna(0).astype("float32")
    state["deaths"] = state["deaths"].astype("int64")
    state["population"] = state["population"].astype("int64")

    state_parquet.parent.mkdir(parents=True, exist_ok=True)
    state.to_parquet(state_parquet, index=False)
    logger.info(
        "wrote %d state rows to %s", len(state), state_parquet,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest backend/tests/test_cdc_wonder_consolidate.py -v`
Expected: both tests PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/etl/cdc_wonder/consolidate.py backend/tests/test_cdc_wonder_consolidate.py
git commit -m "feat(cdc-wonder): consolidation step with missing-county fill"
```

---

## Task 6: ETL entry point

**Files:**
- Create: `backend/etl/process_cdc_wonder.py`

- [ ] **Step 1: Write the entry script**

Create `backend/etl/process_cdc_wonder.py`:

```python
#!/usr/bin/env python3
"""ETL: Download CDC Wonder county-level mortality and build the HIA
baseline-rate parquets.

Runs 216 queries against the CDC Wonder XML API (9 years × 8 ICD-10
groups × 3 age buckets), caches raw TSVs under ``data/raw/cdc_wonder``,
and writes two processed parquets under
``data/processed/incidence/us``.

Usage
-----
    python -m backend.etl.process_cdc_wonder

The script is safely resumable: if a raw TSV already exists on disk
for a given (database, year, ICD group, age bucket) combination, the
HTTP call is skipped.
"""

from __future__ import annotations

import logging
import sys

from backend.etl.cdc_wonder.client import CdcWonderClient
from backend.etl.cdc_wonder.consolidate import consolidate
from backend.etl.cdc_wonder.constants import (
    AGE_BUCKETS,
    COUNTY_PARQUET,
    ICD_GROUPS,
    PROCESSED_DIR,
    RAW_DIR,
    STATE_PARQUET,
    YEARS,
    YEAR_TO_DB,
)
from backend.etl.cdc_wonder.xml_builder import build_request_xml


def _load_master_fips() -> list[str]:
    """Return the master list of 5-digit US county FIPS codes.

    Reads from the Census TIGER county shapefile if available, otherwise
    falls back to a fetch via the ``us`` or ``census`` libraries, or
    raises if nothing is found.
    """
    # Prefer an already-cached master list if present.
    from pathlib import Path
    import pandas as pd

    candidates = [
        Path("data/processed/boundaries/us_county_fips.csv"),
        Path("data/raw/boundaries/us_county_fips.csv"),
    ]
    for cand in candidates:
        if cand.exists():
            df = pd.read_csv(cand, dtype=str)
            col = "fips" if "fips" in df.columns else df.columns[0]
            return sorted({f.zfill(5) for f in df[col].dropna()})

    # Fallback: fetch from Census Bureau.
    import requests
    url = (
        "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt"
    )
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    fips: set[str] = set()
    for line in resp.text.splitlines():
        parts = line.split("|")
        if len(parts) < 3:
            continue
        if parts[0] == "STATE":
            continue
        state_fips = parts[1].strip()
        county_fips = parts[2].strip()
        if state_fips.isdigit() and county_fips.isdigit():
            fips.add(f"{state_fips.zfill(2)}{county_fips.zfill(3)}")
    if not fips:
        raise RuntimeError("Failed to load master county FIPS list")
    # Cache for next run.
    candidates[0].parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"fips": sorted(fips)}).to_csv(candidates[0], index=False)
    return sorted(fips)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger = logging.getLogger("cdc_wonder.main")

    client = CdcWonderClient(cache_root=RAW_DIR)

    combos = [
        (YEAR_TO_DB[year], year, icd_group, age_bucket)
        for year in YEARS
        for icd_group in ICD_GROUPS
        for age_bucket in AGE_BUCKETS
    ]
    total = len(combos)
    logger.info("CDC Wonder fetch: %d combinations", total)

    for i, (db, year, icd_group, age_bucket) in enumerate(combos, start=1):
        xml = build_request_xml(
            database=db,
            year=year,
            icd_codes=ICD_GROUPS[icd_group],
            age_groups=AGE_BUCKETS[age_bucket],
        )
        try:
            text = client.fetch(
                database=db,
                year=year,
                icd_group=icd_group,
                age_bucket=age_bucket,
                xml_body=xml,
            )
        except RuntimeError as exc:
            logger.error("[%d/%d] %s %d %s %s — FAILED: %s",
                         i, total, db, year, icd_group, age_bucket, exc)
            continue
        logger.info("[%d/%d] %s %d %s %s — OK (%d bytes)",
                    i, total, db, year, icd_group, age_bucket, len(text))

    logger.info("consolidating cached TSVs → %s", COUNTY_PARQUET)
    master_fips = _load_master_fips()
    consolidate(
        raw_root=RAW_DIR,
        county_parquet=COUNTY_PARQUET,
        state_parquet=STATE_PARQUET,
        master_fips=master_fips,
    )

    _print_sanity_check()
    return 0


def _print_sanity_check() -> None:
    """Print the national 2019 all-cause mortality count for a gut-check."""
    import pandas as pd

    if not COUNTY_PARQUET.exists():
        return
    df = pd.read_parquet(COUNTY_PARQUET)
    subset = df[
        (df["year"] == 2019)
        & (df["icd_group"] == "all_cause")
        & (df["age_bucket"] == "all")
    ]
    total_deaths = int(subset["deaths"].sum())
    total_pop = int(subset["population"].sum())
    print("─" * 60)
    print(f"2019 all-cause mortality sanity check:")
    print(f"  Deaths:     {total_deaths:,}")
    print(f"  Population: {total_pop:,}")
    print(f"  NCHS publishes ~2,854,838 deaths for 2019;")
    print(f"  within ±1% (±28,548) is expected.")
    print("─" * 60)


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Verify it imports without error**

Run: `python -c "import backend.etl.process_cdc_wonder"`
Expected: no output, exit code 0. (Do not run the script itself yet — that happens in Task 9.)

- [ ] **Step 3: Commit**

```bash
git add backend/etl/process_cdc_wonder.py
git commit -m "feat(cdc-wonder): ETL entry point with resumable fetch loop"
```

---

## Task 7: baseline_rates service — test first

**Files:**
- Create: `backend/tests/test_baseline_rates.py`
- Create: `backend/services/baseline_rates.py`

- [ ] **Step 1: Write failing tests**

Create `backend/tests/test_baseline_rates.py`:

```python
"""Tests for backend.services.baseline_rates."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services import baseline_rates


@pytest.fixture
def tiny_parquet(tmp_path: Path, monkeypatch) -> Path:
    """Write a 3-county parquet and point the service at it."""
    rows = [
        # fips, state_fips, year, icd_group, age_bucket, deaths, pop, rate
        ("06037", "06", 2019, "cvd", "25plus", 20000, 6_000_000, 20000 / 6_000_000),
        ("06037", "06", 2019, "ihd", "25plus",  8000, 6_000_000,  8000 / 6_000_000),
        ("36061", "36", 2019, "cvd", "25plus", 10000, 1_200_000, 10000 / 1_200_000),
        # County with population 0 → rate 0 (simulates missing)
        ("01001", "01", 2019, "cvd", "25plus",     0,         0, 0.0),
    ]
    df = pd.DataFrame(rows, columns=[
        "fips", "state_fips", "year", "icd_group",
        "age_bucket", "deaths", "population", "rate_per_person_year",
    ])
    df["year"] = df["year"].astype("int16")
    df["icd_group"] = df["icd_group"].astype("category")
    df["age_bucket"] = df["age_bucket"].astype("category")
    df["deaths"] = df["deaths"].astype("int32")
    df["population"] = df["population"].astype("int32")
    df["rate_per_person_year"] = df["rate_per_person_year"].astype("float32")

    out = tmp_path / "cdc_wonder_mortality.parquet"
    df.to_parquet(out, index=False)
    monkeypatch.setattr(baseline_rates, "_PARQUET_PATH", out)
    baseline_rates._clear_cache()
    return out


def test_non_mortality_endpoint_returns_none(tiny_parquet):
    assert baseline_rates.get_baseline_rate(
        crf_endpoint="Asthma ED visits", year=2019, fips="06037"
    ) is None


def test_none_fips_returns_none(tiny_parquet):
    assert baseline_rates.get_baseline_rate(
        crf_endpoint="Cardiovascular mortality", year=2019, fips=None
    ) is None


def test_scalar_fips_returns_float(tiny_parquet):
    rate = baseline_rates.get_baseline_rate(
        crf_endpoint="Cardiovascular mortality", year=2019, fips="06037",
    )
    assert isinstance(rate, float)
    assert abs(rate - 20000 / 6_000_000) < 1e-12


def test_missing_county_returns_zero(tiny_parquet):
    rate = baseline_rates.get_baseline_rate(
        crf_endpoint="Cardiovascular mortality", year=2019, fips="99999",
    )
    assert rate == 0.0


def test_county_with_zero_population_returns_zero(tiny_parquet):
    rate = baseline_rates.get_baseline_rate(
        crf_endpoint="Cardiovascular mortality", year=2019, fips="01001",
    )
    assert rate == 0.0


def test_list_fips_returns_array(tiny_parquet):
    rates = baseline_rates.get_baseline_rate(
        crf_endpoint="Cardiovascular mortality",
        year=2019,
        fips=["06037", "36061", "99999"],
    )
    assert isinstance(rates, np.ndarray)
    assert rates.shape == (3,)
    assert abs(rates[0] - 20000 / 6_000_000) < 1e-10
    assert abs(rates[1] - 10000 / 1_200_000) < 1e-10
    assert rates[2] == 0.0


def test_ihd_uses_25plus_bucket(tiny_parquet):
    rate = baseline_rates.get_baseline_rate(
        crf_endpoint="Ischemic heart disease", year=2019, fips="06037",
    )
    assert abs(rate - 8000 / 6_000_000) < 1e-12
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest backend/tests/test_baseline_rates.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement the service**

Create `backend/services/baseline_rates.py`:

```python
"""Per-county baseline mortality rate lookup for the HIA engine.

Loads the processed CDC Wonder parquet once (lazily), maps CRF endpoint
strings to (ICD group, age bucket) pairs, and returns y0 values for a
single county or a list of counties.

Returns ``None`` to signal "no US-specific rate available" — the caller
then falls back to the CRF's globally-published ``defaultRate``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

# ── Mapping: CRF endpoint string → (icd_group, age_bucket) ──────────
#
# See docs/superpowers/specs/2026-04-09-cdc-wonder-baseline-rates-design.md
# for rationale. Non-mortality endpoints are intentionally absent.
CRF_ENDPOINT_TO_BASELINE: dict[str, tuple[str, str]] = {
    "All-cause mortality": ("all_cause_nonaccidental", "25plus"),
    "All-cause mortality (non-accidental)": ("all_cause_nonaccidental", "25plus"),
    "All-cause mortality (short-term)": ("all_cause", "all"),
    "Cardiovascular mortality": ("cvd", "25plus"),
    "Cardiovascular mortality (short-term)": ("cvd", "all"),
    "Ischemic heart disease": ("ihd", "25plus"),
    "Stroke (cerebrovascular)": ("stroke", "25plus"),
    "Respiratory mortality": ("respiratory", "25plus"),
    "Respiratory mortality (short-term)": ("respiratory", "all"),
    "COPD mortality": ("copd", "25plus"),
    "Lung cancer": ("lung_cancer", "25plus"),
    "Lower respiratory infection": ("lri", "all"),
}

_PARQUET_PATH = Path("data/processed/incidence/us/cdc_wonder_mortality.parquet")

# Module-level cache: (icd_group, age_bucket, year) → Series indexed by fips.
_rate_cache: dict[tuple[str, str, int], pd.Series] = {}
_full_df: pd.DataFrame | None = None


def _clear_cache() -> None:
    """Reset the in-memory cache. Used by tests."""
    global _full_df
    _full_df = None
    _rate_cache.clear()


def _load_frame() -> pd.DataFrame | None:
    """Lazy-load the processed parquet. Returns ``None`` if absent."""
    global _full_df
    if _full_df is not None:
        return _full_df
    if not _PARQUET_PATH.exists():
        return None
    _full_df = pd.read_parquet(_PARQUET_PATH)
    return _full_df


def _rate_series(icd_group: str, age_bucket: str, year: int) -> pd.Series | None:
    key = (icd_group, age_bucket, year)
    if key in _rate_cache:
        return _rate_cache[key]
    df = _load_frame()
    if df is None:
        return None
    subset = df[
        (df["icd_group"] == icd_group)
        & (df["age_bucket"] == age_bucket)
        & (df["year"] == year)
    ]
    series = pd.Series(
        subset["rate_per_person_year"].to_numpy(dtype=np.float64),
        index=subset["fips"].to_numpy(),
    )
    _rate_cache[key] = series
    return series


def get_baseline_rate(
    crf_endpoint: str,
    year: int,
    fips: str | Iterable[str] | None,
) -> float | np.ndarray | None:
    """Look up the US county baseline mortality rate for a CRF.

    Parameters
    ----------
    crf_endpoint : str
        The ``endpoint`` string from the CRF library (e.g. ``"Cardiovascular
        mortality"``).
    year : int
        Analysis year.  Must be in 2015..2023 for a lookup to succeed.
    fips : str, list/tuple of str, or None
        One or more 5-digit county FIPS codes, or ``None`` for a non-US
        analysis.

    Returns
    -------
    float
        Baseline rate (deaths per person per year) when ``fips`` is a scalar.
    numpy.ndarray
        Array of rates aligned with the input FIPS order when ``fips`` is
        iterable.
    None
        The caller should fall back to the CRF's global ``defaultRate``.
        Returned when: the endpoint is not a mapped mortality endpoint,
        ``fips`` is None, or the parquet is not available on disk.

    Missing counties (not in the parquet) are returned as ``0.0``.
    """
    mapping = CRF_ENDPOINT_TO_BASELINE.get(crf_endpoint)
    if mapping is None:
        return None
    if fips is None:
        return None

    icd_group, age_bucket = mapping
    series = _rate_series(icd_group, age_bucket, year)
    if series is None:
        return None

    if isinstance(fips, str):
        return float(series.get(fips, 0.0))

    fips_list = list(fips)
    out = np.zeros(len(fips_list), dtype=np.float64)
    for i, f in enumerate(fips_list):
        if f in series.index:
            out[i] = float(series.loc[f])
    return out
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest backend/tests/test_baseline_rates.py -v`
Expected: all 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/services/baseline_rates.py backend/tests/test_baseline_rates.py
git commit -m "feat(baseline-rates): CRF→county rate lookup service"
```

---

## Task 8: Wire baseline_rates into the compute router — test first

**Files:**
- Modify: `backend/routers/compute.py`
- Create: `backend/tests/test_compute_router_with_cdc_rates.py`

- [ ] **Step 1: Write failing tests**

Create `backend/tests/test_compute_router_with_cdc_rates.py`:

```python
"""Tests that the compute router stamps per-county baseline rates onto CRFs."""

from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from backend.main import app
from backend.services import baseline_rates


@pytest.fixture
def patched_parquet(tmp_path: Path, monkeypatch):
    rows = [
        ("06037", "06", 2019, "cvd", "25plus",
         20000, 6_000_000, 20000 / 6_000_000),
        ("36061", "36", 2019, "cvd", "25plus",
         10000, 1_200_000, 10000 / 1_200_000),
    ]
    df = pd.DataFrame(rows, columns=[
        "fips", "state_fips", "year", "icd_group",
        "age_bucket", "deaths", "population", "rate_per_person_year",
    ])
    df["year"] = df["year"].astype("int16")
    df["icd_group"] = df["icd_group"].astype("category")
    df["age_bucket"] = df["age_bucket"].astype("category")
    df["deaths"] = df["deaths"].astype("int32")
    df["population"] = df["population"].astype("int32")
    df["rate_per_person_year"] = df["rate_per_person_year"].astype("float32")

    out = tmp_path / "cdc_wonder_mortality.parquet"
    df.to_parquet(out, index=False)
    monkeypatch.setattr(baseline_rates, "_PARQUET_PATH", out)
    baseline_rates._clear_cache()
    yield out


def _base_request() -> dict:
    return {
        "baselineConcentration": 12.0,
        "controlConcentration": 8.0,
        "baselineIncidence": 0.008,
        "population": 1_000_000,
        "selectedCRFs": [
            {
                "id": "krewski",
                "source": "Krewski et al. 2009",
                "endpoint": "Cardiovascular mortality",
                "beta": 0.005827,
                "betaLow": 0.003922,
                "betaHigh": 0.007716,
                "functionalForm": "log-linear",
                "defaultRate": 0.002,
            }
        ],
        "monteCarloIterations": 0,
    }


def test_scalar_request_without_country_code_uses_default_rate(patched_parquet):
    client = TestClient(app)
    resp = client.post("/api/compute", json=_base_request())
    assert resp.status_code == 200
    # No countryCode provided → global defaultRate used → non-zero cases
    data = resp.json()
    assert data["results"][0]["attributableCases"]["mean"] > 0


def test_scalar_request_with_us_fips_uses_county_rate(patched_parquet):
    client = TestClient(app)
    req = _base_request()
    req["countryCode"] = "US"
    req["fipsCodes"] = ["06037"]
    req["year"] = 2019
    resp = client.post("/api/compute", json=req)
    assert resp.status_code == 200
    data = resp.json()

    # Re-run with a high-rate county and confirm the output scales.
    req["fipsCodes"] = ["36061"]  # 36061 has 5x higher CVD rate per capita
    resp2 = client.post("/api/compute", json=req)
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert (
        data2["results"][0]["attributableCases"]["mean"]
        > data["results"][0]["attributableCases"]["mean"]
    )


def test_scalar_request_non_mortality_crf_unchanged(patched_parquet):
    client = TestClient(app)
    req = _base_request()
    req["countryCode"] = "US"
    req["fipsCodes"] = ["06037"]
    req["year"] = 2019
    req["selectedCRFs"][0]["endpoint"] = "Asthma ED visits"  # not in mapping
    resp = client.post("/api/compute", json=req)
    assert resp.status_code == 200
    # Since the endpoint is unmapped, get_baseline_rate returns None and
    # the router leaves the CRF's defaultRate alone → non-zero cases.
    assert resp.json()["results"][0]["attributableCases"]["mean"] > 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest backend/tests/test_compute_router_with_cdc_rates.py -v`
Expected: FAIL — the request model does not accept `countryCode`/`fipsCodes`/`year`.

- [ ] **Step 3: Extend the request models**

In `backend/routers/compute.py`, find the `ComputeRequest` class (around line 45) and add three optional fields:

```python
class ComputeRequest(BaseModel):
    baselineConcentration: float
    controlConcentration: float
    baselineIncidence: float
    population: float
    selectedCRFs: list[CRFInput]
    monteCarloIterations: int = Field(default=1000, ge=100, le=50_000)
    countryCode: str | None = None
    fipsCodes: list[str] | None = None
    year: int | None = None
```

Then find the `SpatialComputeRequest` class (around line 75) and add the same three fields:

```python
class SpatialComputeRequest(BaseModel):
    concentrationFileId: int
    controlFileId: int | None = None
    controlConcentration: float | None = None
    populationFileId: int
    boundaryFileId: int
    baselineIncidence: float
    selectedCRFs: list[CRFInput]
    monteCarloIterations: int = Field(default=1000, ge=100, le=50_000)
    countryCode: str | None = None
    fipsCodes: list[str] | None = None
    year: int | None = None
```

- [ ] **Step 4: Add a helper that stamps per-CRF rates**

In `backend/routers/compute.py`, just above the `@router.post("/compute", ...)` decorator, add:

```python
from backend.services.baseline_rates import get_baseline_rate


def _stamp_us_baseline_rates(
    crfs: list[dict],
    *,
    country_code: str | None,
    fips_codes: list[str] | None,
    year: int | None,
) -> None:
    """In-place: override each CRF's defaultRate with a US county rate
    when the analysis is US-based and a mapping exists.

    For scalar analyses ``fips_codes`` should be a single-element list.
    Unmapped endpoints and non-US analyses are left untouched — the HIA
    engine then uses the CRF's globally-published defaultRate.
    """
    if country_code != "US" or not fips_codes or year is None:
        return
    for crf in crfs:
        endpoint = crf.get("endpoint", "")
        rate = get_baseline_rate(endpoint, year, fips_codes)
        if rate is None:
            continue
        if hasattr(rate, "__len__"):
            # Array of per-county rates: stash for the spatial worker
            # to consume; the scalar path takes the mean.
            crf["_perZoneRates"] = list(rate)
            if len(rate) == 1:
                crf["defaultRate"] = float(rate[0])
            else:
                # Scalar-compute fallback: use population-unweighted mean
                # when the caller provided multiple FIPS but the scalar
                # engine only consumes a single y0.
                import numpy as np
                crf["defaultRate"] = float(np.mean(rate))
        else:
            crf["defaultRate"] = float(rate)
```

- [ ] **Step 5: Call the helper from the scalar endpoint**

Replace the body of `run_compute` in `backend/routers/compute.py`:

```python
@router.post("/compute", response_model=ComputeResponse)
async def run_compute(req: ComputeRequest) -> ComputeResponse:
    """Run HIA computation synchronously and return results.

    When the request specifies ``countryCode='US'`` with a ``fipsCodes``
    list and a ``year``, each selected CRF's ``defaultRate`` is overridden
    with the matching CDC Wonder county rate before the engine runs.
    """
    config = req.model_dump()
    config["selectedCRFs"] = [crf.model_dump() for crf in req.selectedCRFs]
    _stamp_us_baseline_rates(
        config["selectedCRFs"],
        country_code=req.countryCode,
        fips_codes=req.fipsCodes,
        year=req.year,
    )
    result = compute_hia(config)
    return ComputeResponse(**result)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest backend/tests/test_compute_router_with_cdc_rates.py -v`
Expected: all 3 tests PASS.

- [ ] **Step 7: Run the full test suite to confirm nothing else broke**

Run: `pytest backend/tests/ -v`
Expected: all tests PASS (new + existing).

- [ ] **Step 8: Commit**

```bash
git add backend/routers/compute.py backend/tests/test_compute_router_with_cdc_rates.py
git commit -m "feat(compute): stamp US county baseline rates onto CRFs"
```

---

## Task 9: Run the live ETL and verify outputs

**This task runs the real CDC Wonder API.** Expect ~4 minutes of wall time on a cold cache.

- [ ] **Step 1: Run the ETL**

Run: `python -m backend.etl.process_cdc_wonder`
Expected output:
- `[i/216] {db} {year} {icd_group} {age_bucket} — OK (n bytes)` progress lines
- A 2019 all-cause sanity-check block at the end
- Exit code 0

If the first few queries fail with an XML error, this is the expected point to refine the XML builder against a copy-pasted working request from the CDC Wonder web form (see the note in Task 3 Step 3). Once the first query succeeds, the remaining 215 will use the same shape.

- [ ] **Step 2: Verify the processed parquets exist**

Run: `python -c "import pandas as pd; df = pd.read_parquet('data/processed/incidence/us/cdc_wonder_mortality.parquet'); print(df.shape); print(df.head()); print(df['icd_group'].unique()); print(df['year'].unique())"`

Expected:
- Shape roughly `(~680_000, 8)` (3,143 counties × 9 years × 8 ICD × 3 age)
- `icd_group` prints all 8 group names
- `year` prints 2015..2023

- [ ] **Step 3: Confirm the national sanity check is within ±1%**

The script printed a "2019 all-cause mortality" block. Confirm deaths is within 1% of 2,854,838 (so between ~2,826,000 and ~2,883,000). If not, stop and investigate — the XML builder's ICD filter is probably wrong.

- [ ] **Step 4: Commit the processed parquets if they are small enough, otherwise add to gitignore**

Check size: `ls -lh data/processed/incidence/us/*.parquet`

If under ~50 MB total, commit:

```bash
git add data/processed/incidence/us/cdc_wonder_mortality.parquet data/processed/incidence/us/cdc_wonder_mortality_state.parquet
git commit -m "data(cdc-wonder): county + state baseline mortality parquets"
```

If larger, add to `.gitignore` instead:

```bash
echo "data/processed/incidence/us/cdc_wonder_mortality*.parquet" >> .gitignore
git add .gitignore
git commit -m "chore: ignore CDC Wonder processed parquets (too large for git)"
```

(Either way, the raw `data/raw/cdc_wonder/` cache should stay gitignored — it should already be covered by the existing `data/raw/` exclusion if one exists; add it if it isn't.)

---

## Task 10: Final end-to-end verification

- [ ] **Step 1: Run the full test suite**

Run: `pytest backend/tests/ -v`
Expected: all tests PASS.

- [ ] **Step 2: Start the dev server and hit the compute endpoint with a real US request**

Start the backend (`./start-dev.sh` or `./start-dev.bat`) and from another terminal:

```bash
curl -X POST http://localhost:8000/api/compute \
  -H "Content-Type: application/json" \
  -d '{
    "baselineConcentration": 12.0,
    "controlConcentration": 8.0,
    "baselineIncidence": 0.008,
    "population": 10039107,
    "selectedCRFs": [{
      "id": "krewski",
      "source": "Krewski et al. 2009",
      "endpoint": "Cardiovascular mortality",
      "beta": 0.005827,
      "betaLow": 0.003922,
      "betaHigh": 0.007716,
      "functionalForm": "log-linear",
      "defaultRate": 0.002
    }],
    "monteCarloIterations": 0,
    "countryCode": "US",
    "fipsCodes": ["06037"],
    "year": 2019
  }'
```

Expected: 200 response with a non-zero `attributableCases.mean`. Re-run with `"fipsCodes": ["36061"]` (New York County) and confirm the number changes, proving per-county rates are being used.

- [ ] **Step 3: Commit any final fixes**

If any adjustments were needed during verification:

```bash
git add -A  # stage the specific files touched
git commit -m "fix(cdc-wonder): verification-pass adjustments"
```

---

## Out of scope / deferred

- Spatial endpoint integration with per-zone baseline rates. The spatial worker (`_run_spatial_compute` in `backend/routers/compute.py:131`) reads `y0` once per CRF before its zone loop. Extending it to accept a `y0_per_zone` array aligned to the boundary file's zone IDs is a follow-up: the boundary file would need a FIPS-mapping layer, and the worker signature would need to change. Not required for v1 — the scalar endpoint is what the wizard currently uses for US analyses.
- Morbidity and healthcare-utilization endpoints (asthma ED, hospitalizations, T2D). Deferred per the spec.
- Year-pooled rates and state fallback. Deferred per the spec.
