# 

\# GIS Spatial Data Engineering



End-to-end spatial data pipeline demo: ingest Natural Earth datasets, standardize + validate geometry, model in DuckDB Spatial, run spatial join analytics, and publish results as an interactive web map (GitHub Pages).



\## Live demo

\- Interactive map: [simasaadi.github.io/gis-spatial-data-engineering](https://simasaadi.github.io/gis-spatial-data-engineering/)



\## What this repo demonstrates

\- Reproducible spatial pipeline with deterministic outputs

\- Geometry QA (invalid/empty/null) + dataset-level checks (keys/nulls)

\- Spatial SQL in DuckDB (R-tree index + explain plans)

\- Spatial analytics: city-to-admin1 join + summary tables

\- Data publishing: web-ready GeoJSON assets + Leaflet map

Tech stack: GeoPandas, Shapely, PyProj, DuckDB Spatial, Leaflet, GitHub Actions, GitHub Pages

\## Pipeline

Scripts are in `src/pipeline/` and can be executed end-to-end via:

\- Windows: `run\_all.ps1`

\- Mac/Linux: `run\_all.sh`



\### Main steps

1\. Ingest raw Natural Earth (Admin-1 + Populated Places)

2\. Standardize → GeoParquet outputs

3\. Validate → QA report written to `docs/qa/`

4\. Model + analyze in DuckDB Spatial → results in `docs/results/`

5\. Export web assets → `docs/data/` and publish Leaflet map in `docs/index.html`



\## Outputs

\- QA report: `docs/qa/admin1\_qa\_report.csv`

\- Canada admin1 area: `docs/results/admin1\_canada\_area\_km2.csv`

\- Cities by province/territory: `docs/results/cities\_by\_canada\_province.csv`

\- Top admin1 by city count: `docs/results/cities\_by\_admin1\_top50.csv`

Data sources

Natural Earth (vector datasets). Raw files are downloaded during ingest and ignored by git; small samples are committed for fast demo loading.


## How to run (local)

```bash
python -m venv .venv

# Windows:
.venv\Scripts\activate

# Mac/Linux:
source .venv/bin/activate

python -m pip install -r requirements.txt

# Run everything:
./run_all.ps1   # Windows
./run_all.sh    # Mac/Linux

