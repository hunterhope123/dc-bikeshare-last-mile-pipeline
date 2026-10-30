"""Pipeline entry point.

  python run_pipeline.py fixtures                 # offline demo: synthetic bronze -> gold
  python run_pipeline.py live --hours 24          # poll the real feeds, process every 30 min
  python run_pipeline.py live --once              # one poll + process (good for cron / Airflow)
  python run_pipeline.py process                  # rebuild silver/gold from existing bronze (replay)
  python run_pipeline.py report                   # print the headline answers from gold

Stages: extract -> bronze (raw JSON) -> silver (typed, deduped, incremental)
        -> DQ gate -> gold (star + marts) -> AI layer (prediction + briefing) -> serving (Parquet + md)
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
import time
from datetime import datetime, timezone

from bikeshare import ai_layer, bronze, fixtures, gold, quality, silver, sources
from bikeshare.config import SETTINGS

log = logging.getLogger("pipeline")


def process(live: bool) -> int:
    con = silver.connect()
    summary = silver.load_new_files(con)
    for feed, st in summary.items():
        log.info("silver %-20s files=%-5d rows=%-7d quarantined=%-4d failed=%d",
                 feed, st["files"], st["rows"], st["quarantined"], st["failed"])

    overall, results = quality.run_checks(con, live=live)
    for r in results:
        log.info("dq %-32s %-4s observed=%s", r["check_name"], r["severity"], r["observed"])
    if overall == "FAIL":
        log.error("Data quality FAIL: gold not republished, AI layer skipped (last good gold stays live)")
        con.close()
        return 2

    gold.build(con)
    gold.export(con)
    briefing = ai_layer.generate_briefing(con)
    log.info("gold rebuilt, serving files in %s", SETTINGS.serving_dir)
    log.info("AI briefing:\n%s", briefing)
    con.close()
    return 0


def poll_once(feeds: dict[str, str], landed_reference_day: set[str]) -> int:
    """Land one snapshot of every realtime feed; reference feeds once per day. Returns next sleep."""
    ttl = 60
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for name in SETTINGS.reference_feeds:
        if name in feeds and (name, today) not in landed_reference_day:
            try:
                bronze.land(name, sources.fetch_feed(feeds[name]), feeds[name])
                landed_reference_day.add((name, today))
            except Exception as exc:
                log.warning("reference feed %s failed: %s", name, exc)
    for name in SETTINGS.realtime_feeds:
        if name not in feeds:
            log.warning("feed %s not advertised in gbfs.json, skipping", name)
            continue
        try:
            payload = sources.fetch_feed(feeds[name])
            bronze.land(name, payload, feeds[name])
            ttl = max(ttl, int(payload.get("ttl") or 0))
        except Exception as exc:        # one bad feed must not stop the others
            log.warning("realtime feed %s failed: %s", name, exc)
    return max(ttl, SETTINGS.poll_floor_seconds)


def live(hours: float, once: bool, process_every: int) -> int:
    feeds = sources.discover_feeds()
    log.info("discovered %d feeds: %s", len(feeds), ", ".join(sorted(feeds)))
    landed: set = set()
    deadline = time.time() + hours * 3600
    polls, last_weather = 0, 0.0
    while True:
        sleep_s = poll_once(feeds, landed)
        if time.time() - last_weather > 3600:
            try:
                bronze.land("weather", sources.fetch_weather(), SETTINGS.weather_url)
                last_weather = time.time()
            except Exception as exc:
                log.warning("weather fetch failed: %s", exc)
        polls += 1
        if once or polls % process_every == 0 or time.time() + sleep_s > deadline:
            process(live=True)
        if once or time.time() + sleep_s > deadline:
            return 0
        if polls % 12 == 0:
            feeds = sources.discover_feeds()       # pick up URL/version changes
        time.sleep(sleep_s)


def report() -> int:
    con = silver.connect()
    pd_opts = dict(index=False)
    print("\n== Highest last-mile risk (zone x hour) ==")
    print(con.execute("""SELECT zone_label, day_type, hour_of_day AS hr, observed_hours AS n,
                                pct_hours_low_supply AS p_low, avg_bike_supply AS supply, avg_free_ebikes_usable AS ebikes
                         FROM gold.mart_last_mile_risk WHERE observed_hours >= 2 AND zone_label IS NOT NULL
                         ORDER BY p_low DESC, supply LIMIT 10""").df().to_string(**pd_opts))
    print("\n== Weather effect ==")
    print(con.execute("""SELECT weather_condition, temp_band, day_part, observed_hours AS n, avg_station_pct_empty AS pct_empty,
                                avg_bikes_per_station AS bikes, avg_churn_per_station_hour AS churn
                         FROM gold.mart_weather_effect WHERE temp_band <> 'Unknown'
                         ORDER BY day_part, weather_condition""").df().to_string(**pd_opts))
    print("\n== Next-hour empty risk (top 5) ==")
    print(con.execute("""SELECT station_name, target_hour_local, forecast_condition, history_hours, p_empty_next_hour, risk_band
                         FROM gold.pred_station_empty_next_hour ORDER BY p_empty_next_hour DESC LIMIT 5""").df().to_string(**pd_opts))
    print("\n== Latest DQ run ==")
    print(con.execute("""SELECT check_name, severity, ROUND(observed, 4) AS observed, detail FROM ops.dq_results
                         WHERE run_id = (SELECT run_id FROM ops.dq_results ORDER BY run_at DESC LIMIT 1)""").df().to_string(**pd_opts))
    con.close()
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fixtures", help="generate synthetic data and run the full pipeline offline")
    f.add_argument("--keep", action="store_true", help="don't wipe the data dir first")
    lv = sub.add_parser("live", help="poll the real Capital Bikeshare feeds")
    lv.add_argument("--hours", type=float, default=24)
    lv.add_argument("--once", action="store_true")
    lv.add_argument("--process-every", type=int, default=6, help="process after every N polls")
    sub.add_parser("process", help="rebuild silver/gold from bronze")
    sub.add_parser("report", help="print headline results")
    a = ap.parse_args()

    if a.cmd == "fixtures":
        if not a.keep and SETTINGS.data_dir.exists():
            shutil.rmtree(SETTINGS.data_dir)
        log.info("generated synthetic bronze: %s", fixtures.generate())
        rc = process(live=False)
        return rc if rc else report()
    if a.cmd == "live":
        return live(a.hours, a.once, a.process_every)
    if a.cmd == "process":
        return process(live=False)
    return report()


if __name__ == "__main__":
    sys.exit(main())
