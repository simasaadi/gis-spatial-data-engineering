from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DIR = ROOT / "data" / "sample"
SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

def main() -> int:
    # Small sample points (Toronto-ish) to prove the stack works end-to-end
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "lon": [-79.3832, -79.3871, -79.3802],
            "lat": [43.6532, 43.6510, 43.6565],
        }
    )
    gdf = gpd.GeoDataFrame(
        df,
        geometry=[Point(xy) for xy in zip(df["lon"], df["lat"])],
        crs="EPSG:4326",
    )

    out_path = SAMPLE_DIR / "toronto_points.geoparquet"
    gdf.to_parquet(out_path, index=False)

    # DuckDB spatial smoke test
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    con.execute(
        """
        CREATE VIEW pts AS
        SELECT * FROM read_parquet(?)
        """,
        [str(out_path)],
    )

    n = con.execute("SELECT COUNT(*) FROM pts").fetchone()[0]
    bbox = con.execute(
        """
        SELECT
          MIN(lon) AS min_lon, MIN(lat) AS min_lat,
          MAX(lon) AS max_lon, MAX(lat) AS max_lat
        FROM pts
        """
    ).fetchone()

    print("OK: wrote", out_path.as_posix())
    print("OK: duckdb read rows =", n)
    print("OK: bbox =", bbox)
    print("Python:", sys.version.split()[0])
    print("GeoPandas:", gpd.__version__)
    print("DuckDB:", duckdb.__version__)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
