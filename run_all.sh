#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

PY="$REPO_ROOT/.venv/bin/python"

if [[ ! -f "$PY" ]]; then
  python3 -m venv .venv
  "$PY" -m pip install --upgrade pip
  "$PY" -m pip install -r requirements.txt
fi

echo "Running pipeline..."
"$PY" src/smoke_test.py

"$PY" src/pipeline/ingest_admin1.py
"$PY" src/pipeline/ingest_populated_places.py

"$PY" src/pipeline/standardize_admin1.py
"$PY" src/pipeline/standardize_populated_places.py

"$PY" src/pipeline/validate_admin1.py

"$PY" src/pipeline/model_admin1_duckdb.py
"$PY" src/pipeline/analyze_cities_to_admin1.py

echo "DONE."