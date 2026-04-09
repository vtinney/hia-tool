# GBD 2023 Download Runbook

**Purpose:** One-time manual download from the IHME GBD Results Tool to
populate `data/raw/gbd/2023/` for the HIA tool's global baseline
mortality rates.

**Tool:** <https://vizhub.healthdata.org/gbd-results/>
**Account:** Free IHME account required (sign in before starting).
**Expected wait:** Queries of this size typically process in minutes to
~1 hour. IHME emails a download link when the result is ready.

---

## Query parameters

Fill in the Results Tool selectors exactly as below. If any field isn't
mentioned, leave it at its default.

### Base query

| Field | Value |
|---|---|
| **GBD Round** | GBD 2023 |
| **Measure** | Deaths |
| **Metric** | Rate *and* Number (select both) |
| **Risk** | *(leave blank — do NOT select a risk factor)* |
| **Sex** | Both |

> **Why no risk factor:** We want total cause-specific mortality rates
> for use as `y0` (baseline incidence) in the HIA engine. Selecting
> "Ambient particulate matter pollution" as a risk would return only the
> PM2.5-*attributable* subset, which would cause double-counting when
> multiplied by a CRF β. This is the single most important thing to get
> right.

### Causes (8)

Select all of:

- Ischemic heart disease
- Stroke *(parent cause — includes both ischemic and intracerebral/hemorrhagic)*
- Lower respiratory infections
- Chronic obstructive pulmonary disease
- Tracheal, bronchus, and lung cancer
- Diabetes mellitus type 2
- Alzheimer's disease and other dementias
- All causes

### Ages (3)

Select all of:

- All ages
- 25 plus
- 65 plus

*(If the Results Tool only exposes 5-year age bands rather than
pre-aggregated 25+/65+ groups, select the relevant 5-year bands
instead: for 25+, select 25–29 through 95+; for 65+, select 65–69
through 95+. The ingest script can handle both shapes.)*

### Years

Select years **2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023**.

### Locations

Select:

- **All countries** (level 3 of the GBD location hierarchy)
- **All available subnational level 1** locations (states / provinces /
  constituent countries) for every country where GBD publishes them —
  this includes India, Brazil, Mexico, UK, Japan, Kenya, Indonesia,
  Iran, Pakistan, Italy, Poland, the Philippines, Russia, Nigeria, and
  possibly others in GBD 2023.

**Exclude:** United States and all US subnational locations. (We use
CDC Wonder for US baseline rates — see the CDC Wonder spec.)

---

## Row-count sanity check

The query should be approximately:

- **~900 locations** (countries + subnational level 1, excluding US)
- **× 9 years** (2015–2023)
- **× 24 cause×age rows** per location×year (8 causes × 3 ages — the
  Results Tool returns the full cross product; we drop the unneeded
  rows during ingest)
- **× 2 metrics** (Rate and Number)
- **= ~390,000 rows**

If the Results Tool flags this as over the row-count limit, split the
query **by year** (three submissions: 2015–2017, 2018–2020, 2021–2023)
rather than by cause or location. The ingest script concatenates
multi-file downloads automatically as long as they land in the same
drop folder.

---

## After the email arrives

1. Download the CSV/ZIP from the link IHME emails you.
2. Unzip if needed. Rename files only if you want — the ingest script
   reads any `.csv` in the drop folder and doesn't care about filenames.
3. Drop every CSV into `data/raw/gbd/2023/`. Example layout:

   ```
   data/raw/gbd/2023/
     IHME-GBD_2023_DATA-abc123-1.csv
     IHME-GBD_2023_DATA-abc123-2.csv
     IHME-GBD_2023_DATA-abc123-3.csv
   ```

4. Run `python -m backend.etl.process_gbd` (script does not yet exist —
   will be built per the GBD design spec).

---

## Things to double-check before submitting

- [ ] Measure = Deaths (not DALYs, not Prevalence, not Incidence).
- [ ] Metric includes **both** Rate and Number.
- [ ] **No risk factor** is selected anywhere in the query.
- [ ] Sex = Both (not Male or Female separately).
- [ ] Year range is 2015–2023 inclusive.
- [ ] US locations are **unchecked**.
- [ ] Upper/lower bounds are **not** selected (we don't use them).
