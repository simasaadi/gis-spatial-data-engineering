from __future__ import annotations

from pathlib import Path
from urllib.request import urlretrieve

RAW_DIR = Path("data/raw/natural_earth")
RAW_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_admin_1_states_provinces.geojson"
OUT = RAW_DIR / "ne_10m_admin_1_states_provinces.geojson"

def main() -> int:
    print("Downloading:")
    print(" ", URL)
    print("To:")
    print(" ", OUT.resolve())

    urlretrieve(URL, OUT)

    size_mb = OUT.stat().st_size / (1024 * 1024)
    print(f"OK: downloaded {size_mb:.2f} MB")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
