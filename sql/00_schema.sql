-- Silver + ops tables. Silver is loaded incrementally (only new bronze files)
-- and is idempotent: primary keys + INSERT OR IGNORE mean re-running a load
-- or polling faster than the feed's ttl can't create duplicates.
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Which bronze files have been processed (the incremental watermark).
CREATE TABLE IF NOT EXISTS ops.ingest_log (
    file_path        VARCHAR PRIMARY KEY,
    feed             VARCHAR,
    feed_last_updated BIGINT,
    rows_loaded      INTEGER,
    rows_quarantined INTEGER,
    status           VARCHAR,
    error            VARCHAR,
    processed_at     TIMESTAMP
);

-- Rows that failed validation. Kept (not dropped) so they can be inspected.
CREATE TABLE IF NOT EXISTS ops.quarantine (
    feed             VARCHAR,
    feed_last_updated BIGINT,
    record_key       VARCHAR,
    reason           VARCHAR,
    record_json      VARCHAR,
    quarantined_at   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.dq_results (
    run_id     VARCHAR,
    check_name VARCHAR,
    severity   VARCHAR,       -- PASS / WARN / FAIL
    observed   DOUBLE,
    threshold  DOUBLE,
    detail     VARCHAR,
    run_at     TIMESTAMP
);

-- Reference: station attributes over time (capacity and names do change).
CREATE TABLE IF NOT EXISTS silver.station_info_history (
    station_id         VARCHAR,
    feed_last_updated  BIGINT,
    name               VARCHAR,
    short_name         VARCHAR,
    lat                DOUBLE,
    lon                DOUBLE,
    capacity           INTEGER,
    region_id          VARCHAR,
    PRIMARY KEY (station_id, feed_last_updated)
);

CREATE TABLE IF NOT EXISTS silver.vehicle_types (
    vehicle_type_id   VARCHAR PRIMARY KEY,
    form_factor       VARCHAR,
    propulsion_type   VARCHAR,
    max_range_meters  DOUBLE,
    name              VARCHAR
);

CREATE TABLE IF NOT EXISTS silver.regions (
    region_id   VARCHAR PRIMARY KEY,
    region_name VARCHAR
);

-- Real-time: one row per station per feed snapshot.
CREATE TABLE IF NOT EXISTS silver.station_status (
    station_id              VARCHAR,
    snapshot_ts_utc         TIMESTAMP,     -- feed last_updated
    snapshot_ts_local       TIMESTAMP,     -- America/New_York, DST-aware
    station_last_reported_utc TIMESTAMP,
    num_bikes_available     INTEGER,
    num_ebikes_available    INTEGER,
    num_docks_available     INTEGER,
    num_bikes_disabled      INTEGER,
    num_docks_disabled      INTEGER,
    is_installed            BOOLEAN,
    is_renting              BOOLEAN,
    is_returning            BOOLEAN,
    is_station_stale        BOOLEAN,       -- hasn't reported recently -> offline, not empty
    PRIMARY KEY (station_id, snapshot_ts_utc)
);

-- Real-time: one row per undocked bike per snapshot. bike_id rotates after
-- every trip (GBFS privacy rule), so it is only unique WITHIN a snapshot.
CREATE TABLE IF NOT EXISTS silver.free_bike (
    snapshot_ts_utc       TIMESTAMP,
    snapshot_ts_local     TIMESTAMP,
    bike_id               VARCHAR,
    lat                   DOUBLE,
    lon                   DOUBLE,
    zone_id               VARCHAR,
    vehicle_type_id       VARCHAR,
    is_reserved           BOOLEAN,
    is_disabled           BOOLEAN,
    current_range_meters  DOUBLE,
    station_id            VARCHAR,       -- populated = bike is sitting at a dock -> already in station counts
    PRIMARY KEY (snapshot_ts_utc, bike_id)
);

-- Enrichment: hourly weather (Open-Meteo). Later loads overwrite earlier
-- ones for the same hour, because forecasts get replaced by observations.
CREATE TABLE IF NOT EXISTS silver.weather_hourly (
    hour_ts_utc     TIMESTAMP PRIMARY KEY,
    hour_ts_local   TIMESTAMP,
    temperature_c   DOUBLE,
    precipitation_mm DOUBLE,
    wind_speed_kmh  DOUBLE,
    weather_code    INTEGER,
    loaded_at       TIMESTAMP
);

-- Enrichment: calendar (US federal + DC holidays from the `holidays` package).
CREATE TABLE IF NOT EXISTS silver.calendar_holiday (
    holiday_date DATE PRIMARY KEY,
    holiday_name VARCHAR
);
