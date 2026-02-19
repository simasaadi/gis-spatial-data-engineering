from __future__ import annotations

from pathlib import Path

import duckdb

STD = Path("data/processed/natural_earth/admin1_standardized.geoparquet")

DB_PATH = Path("data/processed/db/gis.duckdb")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

OUT_DIR = Path("docs/results")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CANADA = OUT_DIR / "admin1_canada_area_km2.csv"
OUT_EXPLAIN = OUT_DIR / "admin1_rtree_explain.txt"


def main() -> int:
    if not STD.exists():
        raise FileNotFoundError(
            f"Missing standardized file: {STD.resolve()} (run standardize first)"
        )

    con = duckdb.connect(str(DB_PATH))
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    std_path = STD.as_posix().replace("'", "''")

    # Create table, handling either:
    # - GeoParquet geometry already typed as GEOMETRY
    # - WKB blob geometry requiring ST_GeomFromWKB
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE admin1 AS
            SELECT
              * EXCLUDE (geometry),
              geometry AS geom
            FROM read_parquet('{std_path}');
            """
        )
        t = con.execute("SELECT typeof(geom) FROM admin1 LIMIT 1").fetchone()[0]
        if "GEOMETRY" not in str(t).upper():
            raise RuntimeError(f"geom is not GEOMETRY (typeof={t})")
    except Exception:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE admin1 AS
            SELECT
              * EXCLUDE (geometry),
              ST_GeomFromWKB(geometry) AS geom
            FROM read_parquet('{std_path}');
            """
        )

    # Indexes (RTREE requires GEOMETRY) :contentReference[oaicite:0]{index=0}
    con.execute("CREATE INDEX IF NOT EXISTS admin1_geom_rtree ON admin1 USING RTREE (geom);")
    con.execute("CREATE INDEX IF NOT EXISTS admin1_adm1_code_idx ON admin1(adm1_code);")

    # Example “developer-grade” metric: geodesic area in km2 for Canada provinces/territories.
    # DuckDB’s spheroid funcs assume [latitude, longitude] axis order, so we flip coords first. :contentReference[oaicite:1]{index=1}
    df = con.execute(
        """
        SELECT
          name,
          adm1_code,
          ROUND(ST_Area_Spheroid(ST_FlipCoordinates(geom)) / 1e6, 2) AS area_km2
        FROM admin1
        WHERE admin = 'Canada'
        ORDER BY area_km2 DESC;
        """
    ).df()
    df.to_csv(OUT_CANADA, index=False)
    print("OK: wrote", OUT_CANADA.as_posix(), "| rows =", len(df))

    # Prove the planner can use RTREE_INDEX_SCAN with a constant envelope. :contentReference[oaicite:2]{index=2}
    explain_rows = con.execute(
        """
        EXPLAIN SELECT COUNT(*)
        FROM admin1
        WHERE ST_Within(geom, ST_MakeEnvelope(-141, 41, -52, 84));
        """
    ).fetchall()

    # Explain output is usually a 2-col table; write something readable either way
    text = "\n".join(" | ".join(map(str, r)) for r in explain_rows)
    OUT_EXPLAIN.write_text(text, encoding="utf-8")
    print("OK: wrote", OUT_EXPLAIN.as_posix())

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
