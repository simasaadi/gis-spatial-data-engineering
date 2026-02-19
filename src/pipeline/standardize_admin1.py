from __future__ import annotations

from pathlib import Path

import geopandas as gpd

RAW = Path("data/raw/natural_earth/ne_10m_admin_1_states_provinces.geojson")
OUT_DIR = Path("data/processed/natural_earth")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FULL = OUT_DIR / "admin1_standardized.geoparquet"
OUT_SAMPLE = Path("data/sample/admin1_canada_sample.geoparquet")  # small, ok to commit


def _snake(s: str) -> str:
    return (
        s.strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
    )


def main() -> int:
    if not RAW.exists():
        raise FileNotFoundError(f"Missing raw file: {RAW.resolve()} (run ingest first)")

    gdf = gpd.read_file(RAW)
    print("OK: read rows =", len(gdf))
    print("OK: columns =", len(gdf.columns))
    print("OK: crs =", gdf.crs)

    # Normalize CRS
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")

    # Geometry validity checks (before/after)
    invalid_before = int((~gdf.is_valid).sum())
    print("QA: invalid geometries (before) =", invalid_before)

    # Try to fix invalid geometries (works with modern GeoPandas/Shapely)
    try:
        gdf["geometry"] = gdf["geometry"].make_valid()
    except Exception:
        # Fallback: buffer(0) can fix some self-intersections
        gdf["geometry"] = gdf["geometry"].buffer(0)

    invalid_after = int((~gdf.is_valid).sum())
    print("QA: invalid geometries (after)  =", invalid_after)

    # Standardize column names
    gdf = gdf.rename(columns={c: _snake(c) for c in gdf.columns})

    # Drop empty geometries
    empty_geom = int(gdf.geometry.is_empty.sum())
    if empty_geom:
        gdf = gdf[~gdf.geometry.is_empty].copy()
    print("QA: empty geometries dropped =", empty_geom)
    print("OK: rows (final) =", len(gdf))

    # Write full standardized output (not committed)
    gdf.to_parquet(OUT_FULL, index=False)
    print("OK: wrote", OUT_FULL.as_posix())

    # Write a small sample (Canada provinces/territories) so repo has a real polygon layer committed
    # Natural Earth usually has 'admin' (country name). If not, this gracefully falls back.
    if "admin" in gdf.columns:
        sample = gdf[gdf["admin"] == "Canada"].copy()
    elif "adm0_a3" in gdf.columns:
        sample = gdf[gdf["adm0_a3"] == "CAN"].copy()
    else:
        sample = gdf.head(25).copy()

    sample.to_parquet(OUT_SAMPLE, index=False)
    print("OK: wrote sample", OUT_SAMPLE.as_posix(), "| rows =", len(sample))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
