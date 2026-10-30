"""Build gold tables from SQL files and export them for serving."""
from __future__ import annotations

from .config import ROOT, SETTINGS, Settings

GOLD_SQL = ("10_gold.sql", "20_ai_features.sql")
SERVING_TABLES = ("dim_station", "dim_zone", "dim_hour", "fct_station_hour", "fct_zone_hour",
                  "mart_last_mile_risk", "mart_weather_effect", "pred_station_empty_next_hour")
LOW_SUPPLY_THRESHOLD = 2   # fewer than 2 bikes in the zone = can't count on getting one


def render(sql: str, s: Settings) -> str:
    return sql.format(zone_lat_deg=s.zone_lat_deg, zone_lon_deg=s.zone_lon_deg,
                      min_usable_range_m=s.min_usable_range_m,
                      low_supply_threshold=LOW_SUPPLY_THRESHOLD)


def build(con, s: Settings = SETTINGS) -> None:
    con.execute("BEGIN TRANSACTION")       # all-or-nothing: readers never see half-built gold
    try:
        for name in GOLD_SQL:
            con.execute(render((ROOT / "sql" / name).read_text(), s))
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def export(con, s: Settings = SETTINGS) -> None:
    """Parquet copies for BI tools / notebooks (the 'serving' layer)."""
    s.serving_dir.mkdir(parents=True, exist_ok=True)
    for t in SERVING_TABLES:
        con.execute(f"COPY gold.{t} TO '{s.serving_dir / (t + '.parquet')}' (FORMAT PARQUET)")
