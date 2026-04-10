"""Spatial join: GHS SMOD urban center centroids → NE country + state."""

from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

logger = logging.getLogger("gbd_pollution.ghs_join")


def build_ghs_to_ne_join(
    *,
    ghs_smod_path: Path,
    ne_countries_path: Path,
    ne_states_path: Path,
    output_parquet: Path,
) -> None:
    """Spatial-join GHS SMOD urban centers to Natural Earth polygons.

    Each urban center's centroid is computed (in EPSG:4326 after
    reprojection), then joined to the enclosing NE country and NE
    state polygons. Writes one row per GHS uid.

    Border cases: centers whose centroid falls outside all NE polygons
    get ``ne_country_uid = NULL`` and are retained in the output.
    """
    ghs = gpd.read_file(ghs_smod_path).to_crs("EPSG:4326")
    countries = gpd.read_file(ne_countries_path).to_crs("EPSG:4326")
    states = gpd.read_file(ne_states_path).to_crs("EPSG:4326")

    # Use a points layer built from centroids — robust for cities that
    # may straddle polygon borders.
    ghs_points = ghs.copy()
    ghs_points["geometry"] = ghs_points.geometry.representative_point()

    country_cols = ["ADM0_A3"]
    joined_country = gpd.sjoin(
        ghs_points[["ID_UC_G0", "geometry"]],
        countries[country_cols + ["geometry"]],
        predicate="within", how="left",
    ).rename(columns={"ADM0_A3": "ne_country_uid"})

    state_cols = ["adm1_code"]
    joined_state = gpd.sjoin(
        ghs_points[["ID_UC_G0", "geometry"]],
        states[state_cols + ["geometry"]],
        predicate="within", how="left",
    ).rename(columns={"adm1_code": "ne_state_uid"})

    # Merge the two joins by ghs uid. Drop sjoin's index_right helper cols.
    country_lookup = (
        joined_country[["ID_UC_G0", "ne_country_uid"]]
        .drop_duplicates("ID_UC_G0")
    )
    state_lookup = (
        joined_state[["ID_UC_G0", "ne_state_uid"]]
        .drop_duplicates("ID_UC_G0")
    )

    out = (
        ghs[["ID_UC_G0"]].rename(columns={"ID_UC_G0": "ghs_uid"})
        .merge(country_lookup.rename(columns={"ID_UC_G0": "ghs_uid"}),
               on="ghs_uid", how="left")
        .merge(state_lookup.rename(columns={"ID_UC_G0": "ghs_uid"}),
               on="ghs_uid", how="left")
    )
    out["ghs_uid"] = out["ghs_uid"].astype("int64")

    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_parquet, index=False)
    logger.info("wrote %d GHS → NE rows to %s", len(out), output_parquet)
