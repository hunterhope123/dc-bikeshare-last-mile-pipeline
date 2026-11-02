# DC Bikeshare Last-Mile Pipeline

[![tests](https://github.com/hunterhope123/dc-bikeshare-last-mile-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/hunterhope123/dc-bikeshare-last-mile-pipeline/actions/workflows/tests.yml)

An end-to-end data pipeline over Capital Bikeshare's live GBFS feeds (Washington, DC), enriched with hourly weather and holidays, with a data-quality gate and an AI layer. It answers one practical question for travellers: **can I find a usable bike close by (roughly a 5-minute walk), here, at this hour?**

**Stack:** Python, SQL, pandas, pytest, Open-Meteo API, optional Anthropic API, in a medallion architecture (bronze, silver, gold). Databricks is the target platform and my main stack; the production mapping (Auto Loader, Delta, Workflows, Unity Catalog, DLT expectations, MLflow) is in [`docs/architecture.md`](docs/architecture.md). Locally it runs on DuckDB so anyone can run it and its tests on a laptop with dummy data, without a cloud workspace.

## Run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python run_pipeline.py live --hours 24   # poll the real feeds, process every 30 min
python run_pipeline.py report            # headline results from gold
python run_pipeline.py fixtures          # offline demo on synthetic data (no network)
pytest -q                                # 10 tests
```

Optional: `export ANTHROPIC_API_KEY=...` to have the briefing written by Claude; otherwise a deterministic template is used.

The risk table only shows a zone and hour once it has been seen on at least 2 days of the same type (weekday or weekend), which takes about 48 weekday hours of collection. After a shorter run, use `MIN_OBSERVED_HOURS=1 python run_pipeline.py report`; each figure then rests on a single day.

## Findings

> **These numbers come from synthetic data** (`python run_pipeline.py fixtures`): 4 days, 58 made-up stations, 5-minute snapshots. They show what the pipeline produces and that each stage works. They are not claims about real DC bikeshare. Run `live` to get real findings.

**Run summary.** 1,152 station snapshots and 1,152 free-bike snapshots went from bronze to gold. That is 67,968 station-status rows and 149,435 free-bike rows. 115 free bikes with bad coordinates were quarantined (0.05% of records). One duplicate poll was absorbed by the primary key. The quality gate returned 9 PASS and 1 WARN (`orphan_station_ids = 1`, the planted station 9999), so gold was published with the caveat shown in the briefing.

**Highest last-mile risk** (zone x day type x hour; n = observed hours):

| Zone | Day type | Hour | n | Hours with < 2 bikes | Avg supply |
|---|---|---|---|---|---|
| Downtown 01 / Downtown 11 | Weekday | 18:00 | 2 | 100% | 1.6 |
| Downtown 07 / Downtown 18 | Weekday | 18:00 | 2 | 50% | 1.5 |
| Residential 28 | Weekday | 08:00 | 2 | 50% | 2.6 |
| Residential 34 | Weekday | 08:00 | 2 | 50% | 2.8 |
| Tourist 52 | Weekend/Holiday | 13:00 | 2 | 50% | 3.3 |

The pipeline recovers the commute pattern built into the synthetic data. Residential zones drain in the weekday morning peak (07:00-09:00), downtown zones drain in the evening peak (18:00), and the tourist zone is tightest on weekend afternoons. For a traveller, those are the zone/hour combinations where the product should suggest Metro or rideshare instead of a bike.

**Weather effect.** Stations were empty 0% of wet hours vs. about 5% of dry hours. Dry PM peak was the worst slice: 9.7-11.2% empty. In the synthetic data, rain flattens the demand curve, so fewer stations run dry. The churn usage proxy barely differs (7.89 vs. 7.53 bike moves per station-hour) because snapshot noise dominates it. Real data is needed to test whether churn is a usable demand signal.

**Next-hour prediction.** The target hour was Tuesday 00:00, and every station came out Low risk. That is expected: stations don't empty overnight in the synthetic data, and each station has only 2 hours of matching history, so the shrinkage toward zone rates keeps estimates conservative.

**Limitation to note for a live run.** By default, the risk table only shows zone/hour combinations seen on at least 2 days of the same day type. A run needs about 48 weekday hours before that table fills; for a shorter run, set `MIN_OBSERVED_HOURS=1` (see Run it).

## Pipeline

```
gbfs.json discovery -> station_status / free_bike_status (5 min) + reference feeds (daily) + Open-Meteo (hourly)
  -> BRONZE raw JSON (immutable, replayable)
  -> SILVER typed + deduped + quarantined (incremental)
  -> DQ GATE (10 checks; FAIL keeps last good gold)
  -> GOLD dim_station / dim_zone / dim_hour, fct_station_hour / fct_zone_hour,
          mart_last_mile_risk, mart_weather_effect
  -> AI   pred_station_empty_next_hour + LLM briefing
  -> SERVING Parquet + briefing.md
```

Full diagram and production mapping (Azure/Databricks, Snowflake): [`docs/architecture.md`](docs/architecture.md).

## The question this pipeline answers

> **If a traveller arrives in a DC neighbourhood at a given hour, how likely are they to find a usable bike close by (roughly a 5-minute walk), and how do time of day, day type and weather change that?**

**Why this question.** A travel company cares about last-mile transport for its customers. "How many bikes are in DC" isn't something a traveller can act on. What matters is whether there's a bike near them when they arrive. That answer can drive product decisions: when to suggest bikeshare in a trip itinerary, and when to suggest Metro or rideshare instead. It also connects back to the travel warehouse project, because its hotel locations can be joined to these zones.

**How the pipeline and schema support it**

| Layer | What it holds | Why it matters for the question |
|---|---|---|
| Bronze | Every API response as raw JSON with lineage metadata (source URL, ingest time, feed `last_updated`, ttl) | Replayable. A logic bug is fixed by reprocessing, not by losing history |
| Silver | `station_status` (station x snapshot), `free_bike` (undocked bike x snapshot), `station_info_history`, `vehicle_types`, `weather_hourly`, `calendar_holiday` | Typed, deduped on `(station_id, last_updated)`, UTC + local time, invalid rows quarantined |
| Gold dims | `dim_station`, `dim_zone` (~550 m grid cells, a proximity estimate), `dim_hour` (local hour, day type, holiday, weather) | Context for slicing |
| Gold facts | `fct_station_hour`, `fct_free_bike_zone_hour`, `fct_zone_hour` | Hourly supply per zone = docked bikes + usable dockless e-bikes |
| Gold marts | **`mart_last_mile_risk`** (zone x day type x hour: % of hours with < 2 bikes nearby), `mart_weather_effect` | Direct answers to the question |
| AI | `pred_station_empty_next_hour`, `ai_briefing` | Forward-looking risk + plain-English summary |

Design choices worth calling out:
- **Supply is measured at the zone level, not the station level.** A traveller doesn't care which dock has a bike. They care whether anything is close by, and dockless e-bikes count as supply. Zones are ~550 m straight-line grid cells: a geographic proximity estimate of a 5-minute walk, not a walking route. A river or a highway can make a bike in the same cell harder to reach; a pedestrian routing network would fix that later.
- **"Usable" is defined strictly.** Bikes that are reserved, disabled, or sitting at a dock don't count as free bikes (docked ones are already in the station counts). E-bikes with under 3 km of range don't count.
- **Offline ≠ empty.** A station that isn't renting or hasn't reported in over an hour is tracked as offline, not empty.
- **Usage proxy.** GBFS has no trip data, so the change in bike counts between snapshots ("churn") is used as an activity proxy for the weather analysis.

## Enrichment sources

| Source | What | Why | What it adds |
|---|---|---|---|
| **Open-Meteo** hourly weather (`api.open-meteo.com`, free, no key) | Temperature, precipitation, wind, WMO weather code for central DC | Weather is the biggest outside driver of bike demand | Splits availability and usage by Wet/Dry and temperature band (`mart_weather_effect`), and, once there are enough wet hours of history, lets the next-hour prediction adjust for a rain forecast |
| **US federal + DC holidays** (`holidays` Python package, OPM calendar + DC Emancipation Day) | Holiday date and name | Holidays behave like weekends, not weekdays; leaving them in "weekday" skews commute patterns | `day_type` = Weekday vs. Weekend/Holiday in `dim_hour` |
| GBFS reference feeds (`station_information`, `vehicle_types`, `system_regions`) | Station location/capacity, e-bike vs. classic, region names | Needed to locate supply and judge e-bike range | Zone assignment, capacity checks, region labels |

Next sources I'd add: WMATA Metro station locations (to measure bike supply near rail exits), a travel company's own hotel and booking locations from the travel-booking-warehouse repo, and the DC events calendar (Nationals games, Capital One Arena) for demand spikes.

## What can go wrong with this data, and how it is handled

| Risk | Handling |
|---|---|
| **bike_id rotates after every trip** (GBFS privacy rule) | Bikes are never tracked over time. Free bikes are only counted per snapshot and per zone; the primary key is `(snapshot, bike_id)` |
| **Polling faster than the feed updates** produces duplicate snapshots | Silver primary key `(station_id, last_updated)` with `INSERT OR IGNORE`; poll at `max(ttl, 5 min)`. DQ check reports how many duplicates were absorbed |
| **Station offline / not reporting** looks like "0 bikes" | `is_station_stale` (last_reported > 60 min) and `is_renting=false` are excluded from empty metrics and reported as `pct_offline` |
| **Double counting** e-bikes that appear in both feeds | Free bikes with a `station_id` are excluded from dockless supply |
| **Bad coordinates** (0,0, swapped lat/lon) or missing IDs | Bounding-box validation; failing rows go to `ops.quarantine` (kept, not dropped), with a quarantine-rate check |
| **API down, slow, rate-limited** | Timeouts and exponential backoff on 429/5xx. Each feed fails independently. Bronze is immutable, so a missed poll is a gap, not corruption. Gap and freshness checks flag it |
| **Collection gaps** bias hourly averages | Every hourly fact carries `n_snapshots`; free-bike supply is NULL (not 0) when that feed wasn't collected |
| **Schema / URL changes, GBFS 3.0** (`free_bike_status` → `vehicle_status`) | Discovery-first: URLs come from `gbfs.json` and are refreshed hourly. Aliases cover the 3.0 rename. Required fields are validated, and a file that fails to parse is logged and retried without stopping the run |
| **Truncated feed** (half the stations missing) | Volume check: latest snapshot vs. median station count. A FAIL blocks gold |
| **Timezones and DST** | Feeds are epoch UTC. Local time is derived with `zoneinfo`; gold keys on the UTC hour so the repeated 1 AM hour in November stays two hours (unit tested) |
| **Weather forecast vs. observed** | Weather upserts by hour, so later loads replace earlier forecasts for the same hour; coverage check. Limitations: one point for all of DC (no per-zone weather), and the forecast API's past hours are modelled, not station observations. For historical analysis at scale, load the Open-Meteo historical archive into a separate table instead of mixing it with forecasts |
| **Capacity changes, new or removed stations** | `station_info_history` keeps every version; orphan-station and capacity-violation checks |
| **AI layer producing wrong numbers** | The LLM never computes numbers; it only narrates a fact pack built with SQL. It doesn't run if DQ fails, and every briefing is stored with its facts and prompt hash |

Quality gate: 10 checks write PASS/WARN/FAIL to `ops.dq_results`. A WARN publishes gold with the caveat shown at the top of the briefing; any FAIL leaves the last good gold in place and skips the AI layer.

| Check | WARN when | FAIL when (blocks publishing) |
|---|---|---|
| Bronze files that failed to parse (retried next run) | > 0 | > 10 |
| Minutes since newest snapshot (live mode) | > 15 | > 90 |
| Largest gap between snapshots | > 15 min | never; gaps show as fewer `n_snapshots` |
| Stations not reporting in the latest snapshot | > 10% | > 50% |
| Status rows for unknown station IDs | > 0 | > 25 stations |
| Bikes + docks over capacity | > 1% of rows | > 10% |
| Records quarantined (bad coordinates, missing IDs) | > 1% | > 10% |
| Latest snapshot station count vs. median (truncated feed) | < 90% | < 50% |
| Observed hours with weather joined | < 90% | < 50% |
| Duplicate polls absorbed by the primary key | never (informational) | never |

## AI layer: where it sits and why

It sits **on top of gold, after the quality gate**, never inside ingestion or transformation. LLM output isn't deterministic, so it shouldn't be part of the system of record. It does two things:

1. **Predictive:** `pred_station_empty_next_hour`. Each station's historical empty rate for the same hour and day type, shrunk toward its zone's rate when there's little history (so one bad hour doesn't read as 100% risk). If rain is forecast, it is scaled by the observed wet-to-dry empty ratio, but only after 24+ wet hours of history and capped to 0.5-2x; with less evidence there is no weather adjustment. It's a heuristic baseline, not a trained model: the benchmark that a gradient-boosted model in MLflow would have to beat once there are weeks of data.
2. **Generative:** `ai_briefing`. The model reads a small JSON fact pack (top risk zones, weather effect, next-hour hotspots, DQ caveats) and writes a short briefing for the product team. Every number comes from SQL. Output is stored with the model name, prompt hash and facts for audit. It uses the Anthropic API when a key is set and falls back to a deterministic template otherwise, so the pipeline never depends on an LLM being available.

A natural next step is natural-language questions over the gold marts (text-to-SQL limited to a read-only gold schema), e.g. Databricks Genie or an MCP tool over the warehouse.
