"""Convert PM2.5 GEE export CSVs into long-format Parquet files."""
from __future__ import annotations

import argparse
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

AGE_BINS = [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90]
AGE_COLUMNS = [f"age_{b}" for b in AGE_BINS]

INT_COLUMNS = ["year", "pop_source_year"]
FLOAT_COLUMNS = ["pop_total", "pm25_x_pop", "pm25_mean", *AGE_COLUMNS]
STRING_COLUMNS = ["feature_id", "name"]

# Optional passthrough columns the GEE script may attach (e.g. country_iso3
# from CONFIG.boundaries entries with a countryField). Coerced to string when
# present; absence is not an error.
OPTIONAL_STRING_COLUMNS = ["country_iso3"]

REQUIRED_COLUMNS = STRING_COLUMNS + INT_COLUMNS + FLOAT_COLUMNS


def load_csv(path: Path) -> pd.DataFrame:
    """Read a GEE-exported PM2.5 CSV and coerce dtypes."""
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{path}: missing columns {missing}")
    for col in INT_COLUMNS:
        df[col] = df[col].astype("int64")
    for col in FLOAT_COLUMNS:
        df[col] = df[col].astype("float64")
    for col in STRING_COLUMNS:
        df[col] = df[col].astype("string").astype(object)
    for col in OPTIONAL_STRING_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype("string").astype(object)
    return df


def compute_popweighted(df: pd.DataFrame) -> pd.DataFrame:
    """Add pm25_popweighted = pm25_x_pop / pop_total and drop the intermediate."""
    out = df.copy()
    out["pm25_popweighted"] = out["pm25_x_pop"] / out["pop_total"]
    out = out.drop(columns=["pm25_x_pop"])
    return out


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write the dataframe to Parquet using pyarrow."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow", index=False)


# Trailing per-task suffixes appended by the batched / re-tiled export paths,
# stripped when deriving a CSV's boundary group. Covers:
#   _2022_000            whole batch           (gee_export_pm25)
#   _2016_013_s3         5-way shard           (gee_reexport_missing)
#   _040_s3a / _040_s3b  50-feat re-tile       (relaunch_failed_shards)
#   _040_s3_x0           25-feat re-tile       (relaunch_missing_shards_v2)
#   _040_s3_z1           5-feat Arctic re-tile (relaunch_arctic_oom)
#   _040_s3_w2           1-feat per-feature    (relaunch_arctic_oom --per-feature)
_SUFFIX_RE = re.compile(r"(_\d+|_s\d[ab]?|_x\d+|_z\d+|_w\d+)+$")


def group_csvs(csvs: list[Path]) -> dict[str, list[Path]]:
    """Group CSVs by boundary name, stripping trailing per-task suffixes.

    Handles every naming convention the export + relaunch scripts produce:
      pm25_ne_countries.csv              -> group 'pm25_ne_countries'
      pm25_ne_countries_2015.csv         -> group 'pm25_ne_countries'
      pm25_ghs_smod_2022_000.csv         -> group 'pm25_ghs_smod'
      pm25_gadm_adm2_2016_013_s3.csv     -> group 'pm25_gadm_adm2'
      pm25_gadm_adm2_2018_040_s3_x0.csv  -> group 'pm25_gadm_adm2'
      pm25_gadm_adm2_2020_040_s3_z1.csv  -> group 'pm25_gadm_adm2'
    """
    groups: dict[str, list[Path]] = defaultdict(list)
    for csv in csvs:
        group_name = _SUFFIX_RE.sub("", csv.stem)
        groups[group_name].append(csv)
    return dict(groups)


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert PM2.5 CSVs to Parquet")
    parser.add_argument("--input-dir", type=Path, required=True,
                        help="Directory containing pm25_*.csv files")
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="Directory to write pm25_*.parquet files")
    parser.add_argument("--boundary", type=str, default=None,
                        help="Process only this boundary group (e.g. 'ghs_smod'). "
                             "When set, only pm25_{boundary}_*.csv files are read "
                             "and only the matching .parquet is (re)written.")
    parser.add_argument("--split-by-year", action="store_true",
                        help="Write per-year files to <output-dir>/<boundary>/"
                             "{year}.parquet instead of one combined "
                             "<output-dir>/pm25_<boundary>.parquet. Required for "
                             "the gadm_adm2 path, whose resolver reads "
                             "who_aap/gadm_adm2/{year}.parquet.")
    args = parser.parse_args()

    if args.boundary:
        csvs = sorted(args.input_dir.glob(f"pm25_{args.boundary}_*.csv"))
    else:
        csvs = sorted(args.input_dir.glob("pm25_*.csv"))
    if not csvs:
        raise SystemExit(f"No pm25_*.csv files found in {args.input_dir}")

    groups = group_csvs(csvs)
    for group_name, csv_list in sorted(groups.items()):
        frames = [load_csv(csv) for csv in sorted(csv_list)]
        df = pd.concat(frames, ignore_index=True)
        # Sharded / re-tiled exports write disjoint feature ranges to separate
        # CSVs, so (feature_id, year) is unique by construction — but dedupe
        # defensively in case a batch was ever re-exported under a colliding
        # name. Keeping "first" is arbitrary since duplicates are identical.
        before = len(df)
        df = df.drop_duplicates(subset=["feature_id", "year"], keep="first")
        dropped = before - len(df)
        df = compute_popweighted(df)
        note = f" ({dropped} dup rows dropped)" if dropped else ""

        if args.split_by_year:
            # <output-dir>/<boundary>/{year}.parquet — the layout the gadm_adm2
            # resolver reads. boundary = group name without the pm25_ prefix.
            boundary = group_name[len("pm25_"):] if group_name.startswith("pm25_") else group_name
            subdir = args.output_dir / boundary
            for year, ydf in df.groupby("year"):
                out = subdir / f"{int(year)}.parquet"
                write_parquet(ydf.reset_index(drop=True), out)
                print(f"wrote {out} ({len(ydf)} rows)")
            print(f"  {group_name}: {len(df)} rows across "
                  f"{df['year'].nunique()} years from {len(csv_list)} CSVs{note}")
        else:
            out = args.output_dir / (group_name + ".parquet")
            write_parquet(df, out)
            print(f"wrote {out} ({len(df)} rows from {len(csv_list)} CSVs){note}")


if __name__ == "__main__":
    main()
