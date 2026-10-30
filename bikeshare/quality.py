"""Data quality gate between silver and gold.

Each check writes PASS/WARN/FAIL to ops.dq_results. Any FAIL means gold is
NOT republished (consumers keep the last good version) and the AI layer does
not run, so nobody gets an AI summary built on broken data.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from .config import SETTINGS, Settings


def _sev(value, warn, fail, higher_is_worse=True):
    if value is None:
        return "WARN"
    if higher_is_worse:
        return "FAIL" if fail is not None and value > fail else "WARN" if value > warn else "PASS"
    return "FAIL" if fail is not None and value < fail else "WARN" if value < warn else "PASS"


def run_checks(con, live: bool, s: Settings = SETTINGS) -> tuple[str, list[dict]]:
    run_id = uuid.uuid4().hex[:12]
    q = lambda sql: con.execute(sql).fetchone()[0]   # noqa: E731
    results: list[dict] = []

    def add(name, severity, observed, threshold, detail):
        results.append(dict(check_name=name, severity=severity, observed=observed,
                            threshold=threshold, detail=detail))

    # 1. Files that failed to parse (bad JSON, schema change)
    failed = q("SELECT COUNT(*) FROM ops.ingest_log WHERE status='FAILED'")
    add("bronze_parse_failures", _sev(failed, 0, 10), failed, 0,
        "Bronze files that could not be parsed; retried next run")

    # 2. Feed freshness - only meaningful when polling live
    latest = q("SELECT MAX(snapshot_ts_utc) FROM silver.station_status")
    if live and latest is not None:
        age_min = (datetime.now(timezone.utc).replace(tzinfo=None) - latest).total_seconds() / 60
        limit = 3 * s.poll_floor_seconds / 60
        add("station_status_freshness_min", _sev(age_min, limit, 6 * limit), round(age_min, 1), limit,
            "Minutes since the newest station_status snapshot")
    else:
        add("station_status_freshness_min", "PASS", None, None, "Skipped (replay/fixture mode)")

    # 3. Gaps in collection (pipeline or API downtime)
    gap = q("""SELECT MAX(gap_min) FROM (
                 SELECT date_diff('second', lag(ts) OVER (ORDER BY ts), ts) / 60.0 AS gap_min
                 FROM (SELECT DISTINCT snapshot_ts_utc ts FROM silver.station_status))""")
    lim = 3 * s.poll_floor_seconds / 60
    add("max_snapshot_gap_min", _sev(gap, lim, None), gap, lim,
        "Largest gap between snapshots; hours with gaps get fewer snapshots (see n_snapshots)")

    # 4. Stations that stopped reporting (counted as offline, not empty)
    stale = q("""SELECT AVG(CASE WHEN is_station_stale THEN 1.0 ELSE 0 END) FROM silver.station_status
                 WHERE snapshot_ts_utc = (SELECT MAX(snapshot_ts_utc) FROM silver.station_status)""")
    add("stale_station_share_latest", _sev(stale, 0.10, 0.50), stale, 0.10,
        "Share of stations in the latest snapshot that haven't reported in over an hour")

    # 5. Status rows for stations missing from station_information
    orphans = q("""SELECT COUNT(DISTINCT station_id) FROM silver.station_status ss
                   WHERE NOT EXISTS (SELECT 1 FROM silver.station_info_history h WHERE h.station_id = ss.station_id)""")
    add("orphan_station_ids", _sev(orphans, 0, 25), orphans, 0,
        "station_ids in station_status with no station_information row (new/removed stations)")

    # 6. Bikes + docks more than capacity (sensor or capacity data wrong)
    over = q("""WITH cap AS (SELECT station_id, capacity FROM silver.station_info_history
                             QUALIFY row_number() OVER (PARTITION BY station_id ORDER BY feed_last_updated DESC)=1)
                SELECT AVG(CASE WHEN ss.num_bikes_available + COALESCE(ss.num_docks_available,0)
                                     + COALESCE(ss.num_bikes_disabled,0) + COALESCE(ss.num_docks_disabled,0)
                                     > c.capacity + 2 THEN 1.0 ELSE 0 END)
                FROM silver.station_status ss JOIN cap c USING (station_id)""")
    add("capacity_violation_rate", _sev(over, 0.01, 0.10), over, 0.01,
        "Share of station rows where bikes + docks exceed capacity")

    # 7. Quarantined records
    qrate = q("""SELECT SUM(rows_quarantined) * 1.0 / NULLIF(SUM(rows_loaded + rows_quarantined), 0)
                 FROM ops.ingest_log WHERE status='SUCCESS'""")
    add("quarantine_rate", _sev(qrate, 0.01, 0.10), qrate, 0.01,
        "Share of records rejected (bad coords, missing ids); kept in ops.quarantine")

    # 8. Truncated feed: latest snapshot has far fewer stations than usual
    ratio = q("""WITH n AS (SELECT snapshot_ts_utc, COUNT(*) c FROM silver.station_status GROUP BY 1)
                 SELECT (SELECT c FROM n ORDER BY snapshot_ts_utc DESC LIMIT 1) * 1.0 / NULLIF(MEDIAN(c), 0) FROM n""")
    add("latest_snapshot_volume_ratio", _sev(ratio, 0.9, 0.5, higher_is_worse=False), ratio, 0.9,
        "Stations in latest snapshot vs. median snapshot")

    # 9. Weather enrichment coverage
    cov = q("""WITH h AS (SELECT DISTINCT date_trunc('hour', snapshot_ts_utc) hr FROM silver.station_status)
               SELECT AVG(CASE WHEN w.hour_ts_utc IS NULL THEN 0 ELSE 1.0 END)
               FROM h LEFT JOIN silver.weather_hourly w ON w.hour_ts_utc = h.hr""")
    add("weather_coverage", _sev(cov, 0.9, 0.5, higher_is_worse=False), cov, 0.9,
        "Share of observed hours with weather joined")

    # 10. Duplicate polls absorbed by the primary key (informational)
    dupes = q("""SELECT COUNT(*) - COUNT(DISTINCT feed_last_updated) FROM ops.ingest_log
                 WHERE feed='station_status' AND status='SUCCESS'""")
    add("duplicate_snapshots_ignored", "PASS", dupes, None,
        "Same feed version landed more than once (polled faster than ttl); deduped on (station_id, last_updated)")

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for r in results:
        con.execute("INSERT INTO ops.dq_results VALUES (?,?,?,?,?,?,?)",
                    [run_id, r["check_name"], r["severity"],
                     None if r["observed"] is None else float(r["observed"]),
                     None if r["threshold"] is None else float(r["threshold"]), r["detail"], now])
    overall = "FAIL" if any(r["severity"] == "FAIL" for r in results) else \
              "WARN" if any(r["severity"] == "WARN" for r in results) else "PASS"
    return overall, results
