from pathlib import Path

import duckdb


def test_expected_output_files_exist():
    assert Path("docs/qa/admin1_qa_report.csv").exists()
    assert Path("docs/results/cities_by_admin1_top50.csv").exists()
    assert Path("docs/results/cities_by_canada_province.csv").exists()


def test_duckdb_has_expected_row_counts():
    db = Path("data/processed/db/gis.duckdb")
    assert db.exists(), "Run run_all.ps1 first to build the DB"

    con = duckdb.connect(str(db))
    admin1 = con.execute("select count(*) from admin1").fetchone()[0]
    cities = con.execute("select count(*) from cities").fetchone()[0]
    con.close()

    assert admin1 == 4596
    assert cities == 7342
