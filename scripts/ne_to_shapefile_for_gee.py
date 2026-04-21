"""Convert Natural Earth GeoJSONs to zipped shapefiles for GEE asset upload.

GEE's Code Editor Table upload only accepts zipped Shapefile or CSV — not GeoJSON.
Outputs two .zip files ready to drag into the GEE Assets panel.
"""
from pathlib import Path
import shutil
import zipfile
import geopandas as gpd

SRC = Path(r"C:/Users/vsoutherland/Claude/hia-tool/frontend/public/data")
OUT = Path(r"C:/Users/vsoutherland/Claude/hia-tool/data/raw/boundaries/natural_earth_gee")
OUT.mkdir(parents=True, exist_ok=True)

FILES = {
    "ne_countries": SRC / "ne_countries_raw.geojson",
    "ne_states":    SRC / "ne_states_raw.geojson",
}

for name, geojson in FILES.items():
    print(f"\n{name}: reading {geojson.name} ...")
    gdf = gpd.read_file(geojson)
    print(f"  features: {len(gdf)}  crs: {gdf.crs}")

    # Ensure EPSG:4326 (GEE expects this)
    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Shapefile field names are capped at 10 chars — truncate if needed
    rename = {}
    seen = set()
    for col in gdf.columns:
        if col == "geometry":
            continue
        new = col[:10]
        i = 1
        while new in seen:
            new = f"{col[:8]}{i:02d}"
            i += 1
        seen.add(new)
        if new != col:
            rename[col] = new
    if rename:
        print(f"  renaming {len(rename)} columns to fit 10-char shapefile limit")
        gdf = gdf.rename(columns=rename)

    work = OUT / name
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    shp_path = work / f"{name}.shp"
    gdf.to_file(shp_path, engine="pyogrio")

    zip_path = OUT / f"{name}.zip"
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in work.iterdir():
            zf.write(f, arcname=f.name)
    size_mb = zip_path.stat().st_size / 1e6
    print(f"  wrote {zip_path}  ({size_mb:.2f} MB)")

print(f"\nDone. Upload these to GEE via Assets -> NEW -> Table upload (Shape files):")
for name in FILES:
    print(f"  {OUT / (name + '.zip')}")
