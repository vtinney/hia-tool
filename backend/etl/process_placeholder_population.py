#!/usr/bin/env python3
"""ETL: Placeholder country-level total population parquets.

Emits one Parquet file per (country, year) with a single row carrying
the national total population. Used as a temporary source for the
Step 3 "Built-in Data" loader until proper WorldPop rasters are
processed per country. Replace any given country's output with the
real WorldPop-derived tract/admin-level parquet when available.

On-disk layout matches the backend population endpoint's primary
lookup: ``data/processed/population/{slug}/{year}.parquet``.

Usage
-----
    venv/Scripts/python.exe -m backend.etl.process_placeholder_population

The script is idempotent — re-running overwrites the existing placeholders.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

OUTPUT_ROOT = Path("data/processed/population")
YEARS = list(range(2015, 2022))  # 2015-2021 inclusive, matches WHO AAP

# National-total population by country slug and year.
# Numbers are UN WPP 2024 revision point estimates rounded to the
# nearest thousand. Slugs align with _canonical_country in
# backend/routers/data.py: "mexico" already aliased from MEX; other
# countries fall through to lowercase ISO3.
PLACEHOLDERS: dict[str, dict[int, int]] = {
    "mexico": {
        2015: 121_858_000, 2016: 123_333_000, 2017: 124_777_000,
        2018: 126_190_000, 2019: 127_576_000, 2020: 128_932_000,
        2021: 130_263_000,
    },
    "bra": {
        2015: 205_188_000, 2016: 206_859_000, 2017: 208_504_000,
        2018: 210_166_000, 2019: 211_782_000, 2020: 213_196_000,
        2021: 214_326_000,
    },
    "ind": {
        2015: 1_322_867_000, 2016: 1_338_636_000, 2017: 1_354_196_000,
        2018: 1_369_003_000, 2019: 1_383_112_000, 2020: 1_396_387_000,
        2021: 1_407_564_000,
    },
    "chn": {
        2015: 1_393_715_000, 2016: 1_402_760_000, 2017: 1_410_276_000,
        2018: 1_417_069_000, 2019: 1_421_864_000, 2020: 1_424_930_000,
        2021: 1_425_893_000,
    },
    "idn": {
        2015: 258_383_000, 2016: 261_115_000, 2017: 263_991_000,
        2018: 266_911_000, 2019: 269_583_000, 2020: 271_858_000,
        2021: 273_753_000,
    },
    "pak": {
        2015: 210_969_000, 2016: 215_432_000, 2017: 220_119_000,
        2018: 224_944_000, 2019: 229_809_000, 2020: 234_719_000,
        2021: 239_702_000,
    },
    "nga": {
        2015: 183_995_000, 2016: 188_666_000, 2017: 193_495_000,
        2018: 198_387_000, 2019: 203_305_000, 2020: 208_327_000,
        2021: 213_401_000,
    },
    "bgd": {
        2015: 157_830_000, 2016: 159_784_000, 2017: 161_793_000,
        2018: 163_683_000, 2019: 165_517_000, 2020: 167_421_000,
        2021: 169_357_000,
    },
    "rus": {
        2015: 144_985_000, 2016: 145_275_000, 2017: 145_530_000,
        2018: 145_734_000, 2019: 145_872_000, 2020: 145_617_000,
        2021: 144_444_000,
    },
    "jpn": {
        2015: 127_985_000, 2016: 127_878_000, 2017: 127_727_000,
        2018: 127_535_000, 2019: 127_202_000, 2020: 126_261_000,
        2021: 125_682_000,
    },
    "gbr": {
        2015: 65_860_000, 2016: 66_297_000, 2017: 66_727_000,
        2018: 67_141_000, 2019: 67_530_000, 2020: 67_886_000,
        2021: 67_326_000,
    },
    "fra": {
        2015: 64_420_000, 2016: 64_634_000, 2017: 64_842_000,
        2018: 65_024_000, 2019: 65_130_000, 2020: 64_480_000,
        2021: 64_531_000,
    },
    "deu": {
        2015: 81_787_000, 2016: 82_349_000, 2017: 82_657_000,
        2018: 82_905_000, 2019: 83_093_000, 2020: 83_161_000,
        2021: 83_409_000,
    },
    "ita": {
        2015: 60_731_000, 2016: 60_628_000, 2017: 60_537_000,
        2018: 60_421_000, 2019: 60_297_000, 2020: 59_502_000,
        2021: 59_109_000,
    },
    "zaf": {
        2015: 55_386_000, 2016: 56_207_000, 2017: 57_009_000,
        2018: 57_792_000, 2019: 58_558_000, 2020: 59_308_000,
        2021: 60_041_000,
    },
    "egy": {
        2015: 93_778_000, 2016: 95_688_000, 2017: 97_488_000,
        2018: 99_375_000, 2019: 101_328_000, 2020: 103_303_000,
        2021: 105_386_000,
    },
    "vnm": {
        2015: 92_677_000, 2016: 93_640_000, 2017: 94_600_000,
        2018: 95_545_000, 2019: 96_462_000, 2020: 97_338_000,
        2021: 98_168_000,
    },
    "col": {
        2015: 47_119_000, 2016: 47_521_000, 2017: 47_910_000,
        2018: 48_290_000, 2019: 49_128_000, 2020: 50_229_000,
        2021: 51_049_000,
    },
    "arg": {
        2015: 43_075_000, 2016: 43_591_000, 2017: 44_093_000,
        2018: 44_584_000, 2019: 45_068_000, 2020: 45_036_000,
        2021: 45_277_000,
    },
    "per": {
        2015: 30_596_000, 2016: 30_965_000, 2017: 31_331_000,
        2018: 31_989_000, 2019: 32_824_000, 2020: 33_305_000,
        2021: 33_715_000,
    },
    "chl": {
        2015: 17_870_000, 2016: 18_106_000, 2017: 18_470_000,
        2018: 18_830_000, 2019: 19_107_000, 2020: 19_300_000,
        2021: 19_493_000,
    },
}

# Country-slug → full display name (used for admin_name).
COUNTRY_NAMES: dict[str, str] = {
    "mexico": "Mexico",
    "bra": "Brazil",
    "ind": "India",
    "chn": "China",
    "idn": "Indonesia",
    "pak": "Pakistan",
    "nga": "Nigeria",
    "bgd": "Bangladesh",
    "rus": "Russia",
    "jpn": "Japan",
    "gbr": "United Kingdom",
    "fra": "France",
    "deu": "Germany",
    "ita": "Italy",
    "zaf": "South Africa",
    "egy": "Egypt",
    "vnm": "Vietnam",
    "col": "Colombia",
    "arg": "Argentina",
    "per": "Peru",
    "chl": "Chile",
}


def _write_country_year(slug: str, year: int, total: int) -> Path:
    """Emit one parquet for (slug, year) with a single national row."""
    row = {
        "admin_id": slug.upper(),
        "admin_name": COUNTRY_NAMES.get(slug, slug.title()),
        "total": total,
    }
    out_path = OUTPUT_ROOT / slug / f"{year}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([row]).to_parquet(out_path, engine="pyarrow", index=False)
    return out_path


def main() -> None:
    written = 0
    for slug, by_year in PLACEHOLDERS.items():
        for year in YEARS:
            total = by_year.get(year)
            if total is None:
                continue
            path = _write_country_year(slug, year, total)
            written += 1
            print(f"wrote {path}")
    print(f"\n{written} placeholder population parquets written under {OUTPUT_ROOT}/")


if __name__ == "__main__":
    main()
