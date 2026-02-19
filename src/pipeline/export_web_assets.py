from __future__ import annotations

from pathlib import Path
import geopandas as gpd

ADMIN1_SAMPLE = Path("data/sample/admin1_canada_sample.geoparquet")
CITIES_SAMPLE = Path("data/sample/populated_places_canada_sample.geoparquet")

OUT_DIR = Path("docs/data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_ADMIN1 = OUT_DIR / "canada_admin1.geojson"
OUT_CITIES = OUT_DIR / "canada_cities.geojson"


def simplify_for_web(gdf: gpd.GeoDataFrame, meters: float = 5000) -> gpd.GeoDataFrame:
    # Simplify in a metric CRS for better control, then return to WGS84 for web maps
    g = gdf.to_crs(3857).copy()
    g["geometry"] = g["geometry"].simplify(meters, preserve_topology=True)
    return g.to_crs(4326)


def main() -> int:
    if not ADMIN1_SAMPLE.exists():
        raise FileNotFoundError(f"Missing {ADMIN1_SAMPLE}. Run standardize_admin1 first.")
    if not CITIES_SAMPLE.exists():
        raise FileNotFoundError(f"Missing {CITIES_SAMPLE}. Run standardize_populated_places first.")

    admin1 = gpd.read_parquet(ADMIN1_SAMPLE).to_crs(4326)
    cities = gpd.read_parquet(CITIES_SAMPLE).to_crs(4326)

    # Keep only useful columns (keeps files small)
    keep_admin1 = [c for c in ["name", "adm1_code", "admin"] if c in admin1.columns]
    admin1 = admin1[keep_admin1 + ["geometry"]].copy()

    keep_cities = [c for c in ["name", "pop_max", "adm0_a3", "adm0name"] if c in cities.columns]
    cities = cities[keep_cities + ["geometry"]].copy()

    admin1 = simplify_for_web(admin1, meters=7000)

    admin1.to_file(OUT_ADMIN1, driver="GeoJSON")
    cities.to_file(OUT_CITIES, driver="GeoJSON")

    print("OK:", OUT_ADMIN1.as_posix())
    print("OK:", OUT_CITIES.as_posix())
    print("Sizes (MB):",
          round(OUT_ADMIN1.stat().st_size / 1e6, 2),
          round(OUT_CITIES.stat().st_size / 1e6, 2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
