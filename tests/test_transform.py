from datetime import date

import duckdb
import pandas as pd
import pytest

from ct_pipeline.transform import TABLES, transform_file

from .conftest import FIXTURE, load_studies, make_study, ndjson


@pytest.fixture
def con():
    c = duckdb.connect()
    yield c
    c.close()


def _read(con, out, table):
    return con.execute(f"SELECT * FROM read_parquet('{out / f'{table}.parquet'}')").fetchdf()


def test_transform_fixture_writes_every_table(con, tmp_path):
    counts = transform_file(con, FIXTURE, tmp_path, "page_0001")
    studies = load_studies()
    assert set(counts) == set(TABLES)
    assert counts["studies"] == len(studies)
    assert counts["study_text"] == len(studies)
    assert counts["study_locations"] > counts["studies"]
    for table in TABLES:
        assert (tmp_path / f"{table}.parquet").exists()


def test_transform_derived_columns(con, tmp_path):
    src = tmp_path / "in.ndjson"
    src.write_bytes(
        ndjson(
            [
                make_study("NCT00000001", phases=["PHASE2", "PHASE1"], start="2020-05", completion="2021-05-01"),
                make_study("NCT00000002", phases=[], start="2020-01-01", completion=None),
                make_study("NCT00000003", phases=["EARLY_PHASE1"], countries=("France", "Germany", "France")),
            ]
        )
    )
    transform_file(con, src, tmp_path / "out", "p1")
    df = _read(con, tmp_path / "out", "studies").set_index("nct_id")

    assert df.loc["NCT00000001", "phase_group"] == "PHASE1/PHASE2"
    assert df.loc["NCT00000002", "phase_group"] == "NA"
    assert df.loc["NCT00000003", "phase_group"] == "EARLY_PHASE1"
    assert df.loc["NCT00000001", "start_date"] == pd.Timestamp(2020, 5, 1)
    assert df.loc["NCT00000001", "start_end"] == (date(2021, 5, 1) - date(2020, 5, 1)).days
    assert pd.isna(df.loc["NCT00000002", "start_end"])
    assert set(_read(con, tmp_path / "out", "studies")["_src"]) == {"p1"}

    phases = _read(con, tmp_path / "out", "study_phases")
    assert sorted(phases[phases.nct_id == "NCT00000001"].phase) == ["PHASE1", "PHASE2"]
    locs = _read(con, tmp_path / "out", "study_locations")
    assert set(locs[locs.nct_id == "NCT00000003"].country) == {"France", "Germany"}
