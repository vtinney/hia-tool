#!/usr/bin/env python3
"""
Download Natural Earth 1:110m boundaries and produce simplified GeoJSON files.

Outputs:
  frontend/public/data/ne_countries.geojson  – world countries (< 2 MB)
  frontend/public/data/us_states.geojson     – US states only

Can be run standalone to re-download, or the processed files can be committed
directly.  Requires: requests (download only; processing uses stdlib json).
"""

import json
import os
import sys
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "frontend" / "public" / "data"

NE_COUNTRIES_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector"
    "/master/geojson/ne_110m_admin_0_countries.geojson"
)
NE_STATES_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector"
    "/master/geojson/ne_110m_admin_1_states_provinces.geojson"
)

# Properties to keep for each feature type
COUNTRY_KEEP = {"NAME", "ISO_A3", "ISO_A2", "POP_EST", "GDP_MD", "CONTINENT", "SUBREGION"}
STATE_KEEP = {"name", "iso_a2", "fips", "adm1_code", "admin", "iso_3166_2"}


def download(url: str, dest: Path) -> None:
    print(f"  Downloading {url.split('/')[-1]} ...")
    urllib.request.urlretrieve(url, dest)
    size_kb = dest.stat().st_size / 1024
    print(f"  -> {dest.name} ({size_kb:.0f} KB)")


def strip_properties(feature: dict, keep: set) -> dict:
    """Return a new feature with only the listed property keys."""
    props = {k: v for k, v in feature.get("properties", {}).items() if k in keep}
    return {
        "type": "Feature",
        "properties": props,
        "geometry": feature["geometry"],
    }


def round_coords(obj, precision: int = 4):
    """Recursively round coordinate values to reduce file size."""
    if isinstance(obj, float):
        return round(obj, precision)
    if isinstance(obj, list):
        return [round_coords(x, precision) for x in obj]
    return obj


def simplify_geometry(feature: dict, precision: int = 4) -> dict:
    """Round coordinates in the geometry to *precision* decimal places."""
    geom = feature.get("geometry")
    if geom and "coordinates" in geom:
        geom = {**geom, "coordinates": round_coords(geom["coordinates"], precision)}
    return {**feature, "geometry": geom}


def process_countries(raw_path: Path, out_path: Path) -> None:
    print("Processing countries ...")
    with open(raw_path, encoding="utf-8") as f:
        data = json.load(f)

    features = []
    for feat in data["features"]:
        feat = strip_properties(feat, COUNTRY_KEEP)
        feat = simplify_geometry(feat, precision=3)
        features.append(feat)

    collection = {"type": "FeatureCollection", "features": features}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(collection, f, separators=(",", ":"))

    size_kb = out_path.stat().st_size / 1024
    print(f"  -> {out_path.name}: {len(features)} countries, {size_kb:.0f} KB")


def process_us_states(raw_path: Path, out_path: Path) -> None:
    print("Processing US states ...")
    with open(raw_path, encoding="utf-8") as f:
        data = json.load(f)

    features = []
    for feat in data["features"]:
        props = feat.get("properties", {})
        # Filter to US features only
        if props.get("iso_a2") != "US" and props.get("admin") != "United States of America":
            continue
        feat = strip_properties(feat, STATE_KEEP)
        feat = simplify_geometry(feat, precision=3)
        features.append(feat)

    collection = {"type": "FeatureCollection", "features": features}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(collection, f, separators=(",", ":"))

    size_kb = out_path.stat().st_size / 1024
    print(f"  -> {out_path.name}: {len(features)} states/territories, {size_kb:.0f} KB")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    raw_countries = OUT_DIR / "ne_countries_raw.geojson"
    raw_states = OUT_DIR / "ne_states_raw.geojson"

    # Download if raw files don't already exist
    if not raw_countries.exists():
        download(NE_COUNTRIES_URL, raw_countries)
    else:
        print(f"  Using cached {raw_countries.name}")

    if not raw_states.exists():
        download(NE_STATES_URL, raw_states)
    else:
        print(f"  Using cached {raw_states.name}")

    # Process
    process_countries(raw_countries, OUT_DIR / "ne_countries.geojson")
    process_us_states(raw_states, OUT_DIR / "us_states.geojson")

    # Clean up raw files
    raw_countries.unlink(missing_ok=True)
    raw_states.unlink(missing_ok=True)
    print("\nDone. Raw files removed.")


if __name__ == "__main__":
    main()
