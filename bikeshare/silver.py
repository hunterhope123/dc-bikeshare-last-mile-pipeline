"""Silver: parse, type, validate and dedupe bronze into relational tables.

Incremental: only bronze files not yet in ops.ingest_log are processed. A file
that fails to parse is logged as FAILED (pipeline keeps going) and is retried
on the next run, because only SUCCESS rows count as processed.
"""
from __future__ import annotations

import json
import logging
import math
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import holidays
import pandas as pd

from . import bronze
from .config import ROOT, SETTINGS, Settings

log = logging.getLogger(__name__)

# Feeds in load order (reference data before the facts that join to it).
FEEDS = ("system_regions", "vehicle_types", "station_information", "station_status", "free_bike_status", "weather")


def connect(s: Settings = SETTINGS) -> duckdb.DuckDBPyConnection:
    s.data_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(s.warehouse_path))
    con.execute((ROOT / "sql" / "00_schema.sql").read_text())
    return con


# ---------------------------------------------------------------- helpers
def _utc(epoch) -> datetime | None:
    if epoch is None or (isinstance(epoch, float) and math.isnan(epoch)):
        return None
    return datetime.fromtimestamp(int(epoch), tz=timezone.utc).replace(tzinfo=None)


def _local(epoch, s: Settings) -> datetime | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(int(epoch), tz=s.local_tz).replace(tzinfo=None)


def zone_id(lat: float, lon: float, s: Settings = SETTINGS) -> str:
    return f"{math.floor(lat / s.zone_lat_deg)}_{math.floor(lon / s.zone_lon_deg)}"


def _in_bbox(lat, lon, s: Settings) -> bool:
    if lat is None or lon is None:
        return False
    min_lat, min_lon, max_lat, max_lon = s.bbox
    return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon


def _insert(con, table: str, rows: list[dict], or_replace: bool = False) -> int:
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    con.register("_stage", df)
    cols = ", ".join(df.columns)
    verb = "INSERT OR REPLACE" if or_replace else "INSERT OR IGNORE"
    before = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.execute(f"{verb} INTO {table} ({cols}) SELECT {cols} FROM _stage")
    con.unregister("_stage")
    after = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return after - before if not or_replace else len(df)


def _quarantine(con, feed, lu, key, reason, rec) -> None:
    con.execute("INSERT INTO ops.quarantine VALUES (?,?,?,?,?,now())",
                [feed, lu, key, reason, json.dumps(rec, default=str)[:2000]])


# ---------------------------------------------------------------- parsers
def _parse_station_information(con, p, lu, s):
    rows, bad = [], 0
    for st in p["data"]["stations"]:
        if not st.get("station_id") or not _in_bbox(st.get("lat"), st.get("lon"), s):
            _quarantine(con, "station_information", lu, st.get("station_id"), "missing id or coords outside service area", st)
            bad += 1
            continue
        rows.append(dict(station_id=str(st["station_id"]), feed_last_updated=lu, name=st.get("name"),
                         short_name=st.get("short_name"), lat=st["lat"], lon=st["lon"],
                         capacity=st.get("capacity"), region_id=st.get("region_id")))
    return _insert(con, "silver.station_info_history", rows), bad


def _parse_vehicle_types(con, p, lu, s):
    rows = [dict(vehicle_type_id=str(v["vehicle_type_id"]), form_factor=v.get("form_factor"),
                 propulsion_type=v.get("propulsion_type"), max_range_meters=v.get("max_range_meters"),
                 name=v.get("name")) for v in p["data"]["vehicle_types"]]
    return _insert(con, "silver.vehicle_types", rows, or_replace=True), 0


def _parse_system_regions(con, p, lu, s):
    rows = [dict(region_id=str(r["region_id"]), region_name=r.get("name")) for r in p["data"]["regions"]]
    return _insert(con, "silver.regions", rows, or_replace=True), 0


def _parse_station_status(con, p, lu, s):
    snap_utc, snap_local = _utc(lu), _local(lu, s)
    rows, bad = [], 0
    for st in p["data"]["stations"]:
        req = ("station_id", "num_bikes_available", "last_reported")
        if any(st.get(k) is None for k in req) or st["num_bikes_available"] < 0:
            _quarantine(con, "station_status", lu, st.get("station_id"), "missing required field or negative count", st)
            bad += 1
            continue
        # e-bikes: Lyft publishes num_ebikes_available; GBFS 2.3 standard is vehicle_types_available.
        ebikes = st.get("num_ebikes_available")
        if ebikes is None and st.get("vehicle_types_available"):
            ebikes = sum(v.get("count", 0) for v in st["vehicle_types_available"]
                         if str(v.get("vehicle_type_id")) != "1")
        age_min = (lu - int(st["last_reported"])) / 60
        rows.append(dict(
            station_id=str(st["station_id"]), snapshot_ts_utc=snap_utc, snapshot_ts_local=snap_local,
            station_last_reported_utc=_utc(st["last_reported"]),
            num_bikes_available=st["num_bikes_available"], num_ebikes_available=ebikes,
            num_docks_available=st.get("num_docks_available"),
            num_bikes_disabled=st.get("num_bikes_disabled"), num_docks_disabled=st.get("num_docks_disabled"),
            is_installed=bool(st.get("is_installed", True)), is_renting=bool(st.get("is_renting", True)),
            is_returning=bool(st.get("is_returning", True)),
            is_station_stale=age_min > s.station_stale_minutes,
        ))
    return _insert(con, "silver.station_status", rows), bad


def _parse_free_bike_status(con, p, lu, s):
    snap_utc, snap_local = _utc(lu), _local(lu, s)
    bikes = p["data"].get("bikes") or p["data"].get("vehicles") or []   # 2.x vs 3.0 key
    rows, bad = [], 0
    for b in bikes:
        lat, lon = b.get("lat"), b.get("lon")
        if not b.get("bike_id") or not _in_bbox(lat, lon, s):
            _quarantine(con, "free_bike_status", lu, b.get("bike_id"), "missing id or coords outside service area", b)
            bad += 1
            continue
        rows.append(dict(
            snapshot_ts_utc=snap_utc, snapshot_ts_local=snap_local, bike_id=str(b["bike_id"]),
            lat=lat, lon=lon, zone_id=zone_id(lat, lon, s),
            vehicle_type_id=str(b.get("vehicle_type_id")) if b.get("vehicle_type_id") is not None else None,
            is_reserved=bool(b.get("is_reserved", False)), is_disabled=bool(b.get("is_disabled", False)),
            current_range_meters=b.get("current_range_meters"), station_id=b.get("station_id"),
        ))
    return _insert(con, "silver.free_bike", rows), bad


def _parse_weather(con, p, lu, s):
    h = p["hourly"]
    rows = []
    for i, t in enumerate(h["time"]):
        ts = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
        rows.append(dict(
            hour_ts_utc=ts.replace(tzinfo=None),
            hour_ts_local=ts.astimezone(s.local_tz).replace(tzinfo=None),
            temperature_c=h["temperature_2m"][i], precipitation_mm=h["precipitation"][i],
            wind_speed_kmh=h["wind_speed_10m"][i], weather_code=h["weather_code"][i],
            loaded_at=datetime.now(timezone.utc).replace(tzinfo=None),
        ))
    # newest load wins: forecast hours get overwritten by observed values later
    return _insert(con, "silver.weather_hourly", rows, or_replace=True), 0


PARSERS = {
    "station_information": _parse_station_information,
    "vehicle_types": _parse_vehicle_types,
    "system_regions": _parse_system_regions,
    "station_status": _parse_station_status,
    "free_bike_status": _parse_free_bike_status,
    "weather": _parse_weather,
}


def load_holidays(con, years: list[int]) -> None:
    rows = []
    for y in years:
        for d, name in holidays.US(subdiv="DC", years=y).items():   # federal + DC Emancipation Day
            rows.append(dict(holiday_date=d, holiday_name=name))
    _insert(con, "silver.calendar_holiday", rows, or_replace=True)


def load_new_files(con, s: Settings = SETTINGS) -> dict:
    done = {r[0] for r in con.execute("SELECT file_path FROM ops.ingest_log WHERE status='SUCCESS'").fetchall()}
    summary = {}
    for feed in FEEDS:
        new = [f for f in bronze.list_files(feed, s) if str(f) not in done]
        loaded = quarantined = failed = 0
        for f in new:
            lu = None
            try:
                env = json.loads(Path(f).read_text())
                lu = int(env["_meta"]["feed_last_updated"])
                n, bad = PARSERS[feed](con, env["payload"], lu, s)
                loaded += n
                quarantined += bad
                status, err = "SUCCESS", None
            except Exception as exc:          # one bad file must not stop the run
                log.exception("failed to load %s", f)
                n, bad, status, err = 0, 0, "FAILED", str(exc)[:500]
                failed += 1
            con.execute("INSERT OR REPLACE INTO ops.ingest_log VALUES (?,?,?,?,?,?,?,now())",
                        [str(f), feed, lu, n, bad, status, err])
        summary[feed] = dict(files=len(new), rows=loaded, quarantined=quarantined, failed=failed)

    years = [r[0] for r in con.execute(
        "SELECT DISTINCT year(snapshot_ts_local) FROM silver.station_status").fetchall() if r[0]]
    load_holidays(con, years or [date.today().year])
    return summary
