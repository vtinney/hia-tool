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


def group_csvs(csvs: list[Path]) -> dict[str, list[Path]]:
    """Group CSVs by boundary name, stripping trailing numeric suffixes.

    Handles all naming conventions produced by the GEE script:
      pm25_ne_countries.csv              -> group 'pm25_ne_countries'
      pm25_ne_countries_2015.csv         -> group 'pm25_ne_countries'
      pm25_ghs_smod_2022_000.csv         -> group 'pm25_ghs_smod'
      pm25_ghs_smod_2022_001.csv         -> group 'pm25_ghs_smod'

    The rule: strip all trailing '_DIGITS' groups from the stem.
    """
    groups: dict[str, list[Path]] = defaultdict(list)
    for csv in csvs:
        group_name = re.sub(r"(_\d+)+$", "", csv.stem)
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
        df = compute_popweighted(df)
        out = args.output_dir / (group_name + ".parquet")
        write_parquet(df, out)
        print(f"wrote {out} ({len(df)} rows from {len(csv_list)} CSVs)")


if __name__ == "__main__":
    main()
