$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repoRoot

$py = Join-Path $repoRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $py)) {
  Write-Host "Creating venv..."
  python -m venv .venv
  & $py -m pip install --upgrade pip
  & $py -m pip install -r requirements.txt
}

Write-Host "Running pipeline..."

& $py "src\smoke_test.py"

& $py "src\pipeline\ingest_admin1.py"
& $py "src\pipeline\ingest_populated_places.py"

& $py "src\pipeline\standardize_admin1.py"
& $py "src\pipeline\standardize_populated_places.py"

& $py "src\pipeline\validate_admin1.py"

& $py "src\pipeline\model_admin1_duckdb.py"
& $py "src\pipeline\analyze_cities_to_admin1.py"

Write-Host "DONE. Outputs:"
Write-Host "  - docs/qa/admin1_qa_report.csv"
Write-Host "  - docs/results/*.csv"
