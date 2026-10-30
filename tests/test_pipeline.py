"""End-to-end and unit tests. Runs the real pipeline on 2 days of synthetic data
in a temp directory.  Run:  pytest -q   (from the repo root)"""
from __future__ import annotations

import math
from datetime import datetime, timezone

import pytest

from bikeshare import ai_layer, fixtures, gold, quality, silver
from bikeshare.config import Settings


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    s = Settings(data_dir=tmp_path_factory.mktemp("data"))
    fixtures.generate(s, days=2)
    con = silver.connect(s)
    first = silver.load_new_files(con, s)
    overall, _ = quality.run_checks(con, live=False, s=s)
    gold.build(con, s)
    gold.export(con, s)
    ai_layer.generate_briefing(con, s)
    yield s, con, first, overall
    con.close()


def q(con, sql):
    return con.execute(sql).fetchone()[0]


def test_pipeline_builds_gold(built):
    s, con, _, overall = built
    assert overall in ("PASS", "WARN")
    for t in ("dim_station", "dim_zone", "dim_hour", "fct_station_hour", "fct_zone_hour",
              "mart_last_mile_risk", "mart_weather_effect", "pred_station_empty_next_hour"):
        assert q(con, f"SELECT COUNT(*) FROM gold.{t}") > 0, t
    assert (s.serving_dir / "briefing.md").exists()
    assert (s.serving_dir / "mart_last_mile_risk.parquet").exists()


def test_incremental_load_is_idempotent(built):
    s, con, _, _ = built
    before = q(con, "SELECT COUNT(*) FROM silver.station_status")
    second = silver.load_new_files(con, s)          # nothing new in bronze
    assert all(v["files"] == 0 for v in second.values())
    assert q(con, "SELECT COUNT(*) FROM silver.station_status") == before


def test_duplicate_poll_is_deduped(built):
    _, con, _, _ = built
    dupes = q(con, """SELECT COUNT(*) FROM (SELECT station_id, snapshot_ts_utc, COUNT(*) c
                      FROM silver.station_status GROUP BY 1,2 HAVING c > 1)""")
    assert dupes == 0


def test_bad_coordinates_quarantined_not_loaded(built):
    _, con, _, _ = built
    assert q(con, "SELECT COUNT(*) FROM silver.free_bike WHERE lat = 0 OR lon = 0") == 0
    assert q(con, "SELECT COUNT(*) FROM ops.quarantine WHERE feed = 'free_bike_status'") > 0


def test_offline_station_is_not_counted_as_empty(built):
    _, con, _, _ = built
    # stale rows are excluded from availability metrics, but show up as offline
    assert q(con, "SELECT COUNT(*) FROM silver.station_status WHERE is_station_stale") > 0
    assert q(con, "SELECT MAX(pct_offline) FROM gold.fct_station_hour") == 1.0


def test_probabilities_in_range(built):
    _, con, _, _ = built
    assert q(con, """SELECT COUNT(*) FROM gold.pred_station_empty_next_hour
                     WHERE p_empty_next_hour < 0 OR p_empty_next_hour > 1""") == 0
    assert q(con, """SELECT COUNT(*) FROM gold.mart_last_mile_risk
                     WHERE pct_hours_low_supply < 0 OR pct_hours_low_supply > 1""") == 0


def test_python_and_sql_zone_ids_match(built):
    s, con, _, _ = built
    lat, lon = 38.9012, -77.0321
    assert silver.zone_id(lat, lon, s) == q(
        con, f"SELECT CAST(floor({lat}/{s.zone_lat_deg}) AS BIGINT)||'_'||CAST(floor({lon}/{s.zone_lon_deg}) AS BIGINT)")


def test_dst_fall_back_hours_stay_distinct():
    s = Settings()
    # 2026-11-01 01:30 EDT and 01:30 EST are an hour apart in UTC but the same local clock time
    a = datetime(2026, 11, 1, 5, 30, tzinfo=timezone.utc).timestamp()
    b = datetime(2026, 11, 1, 6, 30, tzinfo=timezone.utc).timestamp()
    assert silver._local(a, s) == silver._local(b, s)          # same local wall clock
    assert silver._utc(a) != silver._utc(b)                    # which is why gold keys on UTC


def test_single_day_run_shows_risk_with_lower_threshold(tmp_path):
    # one day of data sees each hour once: hidden at the default (2), shown at 1
    s = Settings(data_dir=tmp_path, min_observed_hours=1)
    fixtures.generate(s, days=1)
    con = silver.connect(s)
    silver.load_new_files(con, s)
    gold.build(con, s)
    assert ai_layer.build_fact_pack(con, s)["highest_risk_zone_hours"]
    assert not ai_layer.build_fact_pack(con, Settings(data_dir=tmp_path))["highest_risk_zone_hours"]
    con.close()
