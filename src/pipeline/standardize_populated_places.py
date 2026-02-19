from __future__ import annotations

from pathlib import Path

import geopandas as gpd

RAW = Path("data/raw/natural_earth/ne_10m_populated_places.geojson")

OUT_DIR = Path("data/processed/natural_earth")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FULL = OUT_DIR / "populated_places_standardized.geoparquet"
OUT_SAMPLE = Path("data/sample/populated_places_canada_sample.geoparquet")  # small, ok to commit


def _snake(s: str) -> str:
    return s.strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_")


def main() -> int:
    if not RAW.exists():
        raise FileNotFoundError(
            f"Missing raw file: {RAW.resolve()} (run ingest_populated_places first)"
        )

    gdf = gpd.read_file(RAW)
    print("OK: read rows =", len(gdf))
    print("OK: columns =", len(gdf.columns))
    print("OK: crs =", gdf.crs)

    # Normalize CRS to WGS84
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")

    # Standardize column names
    gdf = gdf.rename(columns={c: _snake(c) for c in gdf.columns})

    # Basic geometry QA
    null_geom = int(gdf.geometry.isna().sum())
    empty_geom = int(gdf.geometry.is_empty.sum())
    invalid_geom = int((~gdf.is_valid).sum())
    print("QA: null_geom  =", null_geom)
    print("QA: empty_geom =", empty_geom)
    print("QA: invalid    =", invalid_geom)

    # Drop bad geometries (for points, we typically drop rather than "fix")
    gdf = gdf[~gdf.geometry.isna()].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()
    gdf = gdf[gdf.is_valid].copy()
    print("OK: rows (final) =", len(gdf))

    # Write full standardized output (ignored by git)
    gdf.to_parquet(OUT_FULL, index=False)
    print("OK: wrote", OUT_FULL.as_posix())

    # Canada sample (if the dataset includes ADM0_A3 / adm0_a3)
    if "adm0_a3" in gdf.columns:
        sample = gdf[gdf["adm0_a3"] == "CAN"].copy()
    elif "sov_a3" in gdf.columns:
        sample = gdf[gdf["sov_a3"] == "CAN"].copy()
    else:
        sample = gdf.head(200).copy()

    # Keep sample reasonably small
    sample = sample.head(200).copy()
    sample.to_parquet(OUT_SAMPLE, index=False)
    print("OK: wrote sample", OUT_SAMPLE.as_posix(), "| rows =", len(sample))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
