from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd

RAW = Path("data/raw/natural_earth/ne_10m_admin_1_states_provinces.geojson")
STD = Path("data/processed/natural_earth/admin1_standardized.geoparquet")

OUT_DIR = Path("docs/qa")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUT_DIR / "admin1_qa_report.csv"


def pick_unique_key(cols: list[str]) -> list[str]:
    # Prefer known stable IDs if present; fall back to a composite
    candidates = [
        ["adm1_code"],
        ["iso_3166_2"],
        ["gn_id"],
        ["adm0_a3", "name"],
        ["admin", "name"],
    ]
    colset = set(cols)
    for c in candidates:
        if all(x in colset for x in c):
            return c
    return []


def geom_counts(gdf: gpd.GeoDataFrame) -> dict:
    return {
        "rows": int(len(gdf)),
        "cols": int(len(gdf.columns)),
        "crs": str(gdf.crs) if gdf.crs else "None",
        "null_geom": int(gdf.geometry.isna().sum()),
        "empty_geom": int(gdf.geometry.is_empty.sum()),
        "invalid_geom": int((~gdf.is_valid).sum()),
    }


def null_counts(gdf: gpd.GeoDataFrame, fields: list[str]) -> dict[str, int]:
    out = {}
    for f in fields:
        if f in gdf.columns:
            out[f"nulls_{f}"] = int(gdf[f].isna().sum())
    return out


def dup_count(gdf: gpd.GeoDataFrame, key_cols: list[str]) -> int | None:
    if not key_cols:
        return None
    dups = gdf.duplicated(subset=key_cols, keep=False).sum()
    return int(dups)


def main() -> int:
    if not RAW.exists():
        raise FileNotFoundError(f"Missing raw file: {RAW.resolve()} (run ingest first)")
    if not STD.exists():
        raise FileNotFoundError(
            f"Missing standardized file: {STD.resolve()} (run standardize first)"
        )

    raw = gpd.read_file(RAW)
    std = gpd.read_parquet(STD)

    # Basic geometry QA
    raw_stats = geom_counts(raw)
    std_stats = geom_counts(std)

    # Field QA (only if fields exist)
    important_fields = [
        "admin",
        "adm0_a3",
        "name",
        "name_en",
        "iso_3166_2",
        "adm1_code",
        "gn_id",
        "type",
        "region",
        "geonunit",
    ]
    raw_stats.update(null_counts(raw, important_fields))
    std_stats.update(null_counts(std, important_fields))

    # Duplicate QA on best available key
    key_cols = pick_unique_key(list(std.columns))
    raw_dup = dup_count(raw, key_cols) if key_cols else None
    std_dup = dup_count(std, key_cols) if key_cols else None

    # Build report table
    rows = []

    def add(check: str, raw_v, std_v):
        rows.append({"check": check, "raw": raw_v, "standardized": std_v})

    add("rows", raw_stats["rows"], std_stats["rows"])
    add("cols", raw_stats["cols"], std_stats["cols"])
    add("crs", raw_stats["crs"], std_stats["crs"])
    add("null_geom", raw_stats["null_geom"], std_stats["null_geom"])
    add("empty_geom", raw_stats["empty_geom"], std_stats["empty_geom"])
    add("invalid_geom", raw_stats["invalid_geom"], std_stats["invalid_geom"])

    if key_cols:
        add("unique_key_used", " / ".join(key_cols), " / ".join(key_cols))
        add("duplicate_rows_on_key", raw_dup, std_dup)
    else:
        add("unique_key_used", "None found", "None found")

    # Add null checks for whichever important fields exist in standardized
    for f in important_fields:
        k = f"nulls_{f}"
        if k in raw_stats or k in std_stats:
            add(k, raw_stats.get(k, "n/a"), std_stats.get(k, "n/a"))

    report = pd.DataFrame(rows)
    report.to_csv(OUT_CSV, index=False)

    print("OK: wrote QA report:", OUT_CSV.as_posix())
    print(report.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
