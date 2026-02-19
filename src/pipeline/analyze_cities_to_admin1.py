from __future__ import annotations

from pathlib import Path

import duckdb

ADMIN1_STD = Path("data/processed/natural_earth/admin1_standardized.geoparquet")
CITIES_STD = Path("data/processed/natural_earth/populated_places_standardized.geoparquet")

DB_PATH = Path("data/processed/db/gis.duckdb")

OUT_DIR = Path("docs/results")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_COUNTS = OUT_DIR / "cities_by_admin1_top50.csv"
OUT_CANADA = OUT_DIR / "cities_by_canada_province.csv"
OUT_EXPLAIN = OUT_DIR / "cities_admin1_join_explain.txt"


def main() -> int:
    if not ADMIN1_STD.exists():
        raise FileNotFoundError(f"Missing admin1 standardized: {ADMIN1_STD.resolve()}")
    if not CITIES_STD.exists():
        raise FileNotFoundError(f"Missing cities standardized: {CITIES_STD.resolve()}")

    con = duckdb.connect(str(DB_PATH))
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    a_path = ADMIN1_STD.as_posix().replace("'", "''")
    c_path = CITIES_STD.as_posix().replace("'", "''")

    # Ensure admin1 table exists with GEOMETRY column 'geom'
    con.execute(
        f"""
        CREATE OR REPLACE TABLE admin1 AS
        SELECT * EXCLUDE (geometry), geometry AS geom
        FROM read_parquet('{a_path}');
        """
    )
    t = con.execute("SELECT typeof(geom) FROM admin1 LIMIT 1").fetchone()[0]
    if "GEOMETRY" not in str(t).upper():
        con.execute(
            f"""
            CREATE OR REPLACE TABLE admin1 AS
            SELECT * EXCLUDE (geometry), ST_GeomFromWKB(geometry) AS geom
            FROM read_parquet('{a_path}');
            """
        )

    # Ensure cities table exists with GEOMETRY column 'geom'
    con.execute(
        f"""
        CREATE OR REPLACE TABLE cities AS
        SELECT * EXCLUDE (geometry), geometry AS geom
        FROM read_parquet('{c_path}');
        """
    )
    t2 = con.execute("SELECT typeof(geom) FROM cities LIMIT 1").fetchone()[0]
    if "GEOMETRY" not in str(t2).upper():
        con.execute(
            f"""
            CREATE OR REPLACE TABLE cities AS
            SELECT * EXCLUDE (geometry), ST_GeomFromWKB(geometry) AS geom
            FROM read_parquet('{c_path}');
            """
        )

    # Indexes for performance
    con.execute("CREATE INDEX IF NOT EXISTS admin1_geom_rtree ON admin1 USING RTREE (geom);")
    con.execute("CREATE INDEX IF NOT EXISTS cities_geom_rtree ON cities USING RTREE (geom);")
    con.execute("CREATE INDEX IF NOT EXISTS admin1_adm1_code_idx ON admin1(adm1_code);")

    # Spatial join: assign each city to an admin1 polygon
    # Use ST_Intersects (points within polygon). (For points, intersects behaves like within if the point is inside.)
    join_sql = """
    WITH joined AS (
      SELECT
        c.name AS city_name,
        c.adm0name AS country,
        c.adm0_a3 AS country_a3,
        c.pop_max AS pop_max,
        a.admin AS admin_country,
        a.name AS admin1_name,
        a.adm1_code AS adm1_code
      FROM cities c
      JOIN admin1 a
        ON ST_Intersects(a.geom, c.geom)
    )
    SELECT
      admin_country,
      admin1_name,
      adm1_code,
      COUNT(*) AS city_count,
      ROUND(SUM(COALESCE(pop_max, 0))::DOUBLE, 0) AS sum_pop_max
    FROM joined
    GROUP BY 1,2,3
    ORDER BY city_count DESC, sum_pop_max DESC
    LIMIT 50;
    """
    df_top = con.execute(join_sql).df()
    df_top.to_csv(OUT_COUNTS, index=False)
    print("OK: wrote", OUT_COUNTS.as_posix(), "| rows =", len(df_top))

    # Canada-only breakdown (all provinces/territories)
    df_ca = con.execute(
        """
        WITH joined AS (
          SELECT
            c.name AS city_name,
            c.pop_max AS pop_max,
            a.name AS province,
            a.adm1_code AS adm1_code
          FROM cities c
          JOIN admin1 a
            ON ST_Intersects(a.geom, c.geom)
          WHERE a.admin = 'Canada'
        )
        SELECT
          province,
          adm1_code,
          COUNT(*) AS city_count,
          ROUND(SUM(COALESCE(pop_max, 0))::DOUBLE, 0) AS sum_pop_max
        FROM joined
        GROUP BY 1,2
        ORDER BY city_count DESC, sum_pop_max DESC;
        """
    ).df()
    df_ca.to_csv(OUT_CANADA, index=False)
    print("OK: wrote", OUT_CANADA.as_posix(), "| rows =", len(df_ca))

    # Explain plan for the join (for README / performance credibility)
    explain_rows = con.execute(
        """
        EXPLAIN
        SELECT COUNT(*)
        FROM cities c
        JOIN admin1 a
          ON ST_Intersects(a.geom, c.geom);
        """
    ).fetchall()
    text = "\n".join(" | ".join(map(str, r)) for r in explain_rows)
    OUT_EXPLAIN.write_text(text, encoding="utf-8")
    print("OK: wrote", OUT_EXPLAIN.as_posix())

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
