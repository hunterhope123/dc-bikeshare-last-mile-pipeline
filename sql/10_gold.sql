-- =====================================================================
-- GOLD: analysis-ready star + marts. Rebuilt in full each run: aggregates
-- over silver are cheap at this size, and full rebuilds make backfills and
-- logic changes trivial. (At scale: incremental by hour partition, i.e. a
-- dbt incremental model / Delta MERGE on hour_utc.)
--
-- Analytical question: for a traveller arriving in a DC neighbourhood at a
-- given hour, how likely are they to find a usable bike (docked or
-- dockless e-bike with enough charge) within a ~5 minute walk, and how do
-- time of day, day type and weather change that?
--
-- Curly-brace tokens are filled from bikeshare/config.py at run time.
-- =====================================================================

-- ---------------------------------------------------------------- dims
CREATE OR REPLACE TABLE gold.dim_station AS
SELECT
    h.station_id, h.name, h.short_name, h.lat, h.lon, h.capacity,
    h.region_id, r.region_name,
    CAST(floor(h.lat / {zone_lat_deg}) AS BIGINT) || '_' || CAST(floor(h.lon / {zone_lon_deg}) AS BIGINT) AS zone_id
FROM silver.station_info_history h
LEFT JOIN silver.regions r USING (region_id)
QUALIFY row_number() OVER (PARTITION BY h.station_id ORDER BY h.feed_last_updated DESC) = 1;

-- Walkable ~550m grid cells. A zone exists if it has a station or a free bike was ever seen there.
CREATE OR REPLACE TABLE gold.dim_zone AS
WITH zones AS (
    SELECT zone_id FROM gold.dim_station
    UNION
    SELECT DISTINCT zone_id FROM silver.free_bike
)
SELECT
    z.zone_id,
    ROUND((CAST(split_part(z.zone_id, '_', 1) AS BIGINT) + 0.5) * {zone_lat_deg}, 5) AS center_lat,
    ROUND((CAST(split_part(z.zone_id, '_', 2) AS BIGINT) + 0.5) * {zone_lon_deg}, 5) AS center_lon,
    COUNT(s.station_id)                                             AS n_stations,
    COALESCE(SUM(s.capacity), 0)                                    AS total_docks,
    array_to_string(list_slice(list(s.name ORDER BY s.capacity DESC, s.name) FILTER (WHERE s.name IS NOT NULL), 1, 2), ' / ') AS zone_label,
    any_value(s.region_name)                                        AS region_name
FROM zones z
LEFT JOIN gold.dim_station s USING (zone_id)
GROUP BY z.zone_id;

-- One row per UTC hour observed. Keyed on UTC so the DST fall-back hour
-- (01:00 local happens twice) stays two distinct hours.
CREATE OR REPLACE TABLE gold.dim_hour AS
WITH hours AS (
    SELECT DISTINCT date_trunc('hour', snapshot_ts_utc) AS hour_utc,
                    date_trunc('hour', snapshot_ts_local) AS hour_local
    FROM silver.station_status
)
SELECT
    h.hour_utc,
    h.hour_local,
    CAST(h.hour_local AS DATE)                    AS local_date,
    hour(h.hour_local)                            AS hour_of_day,
    dayname(h.hour_local)                         AS day_name,
    CASE WHEN isodow(h.hour_local) >= 6 OR hol.holiday_name IS NOT NULL
         THEN 'Weekend/Holiday' ELSE 'Weekday' END AS day_type,
    hol.holiday_name,
    CASE WHEN hour(h.hour_local) < 6  THEN 'Overnight'
         WHEN hour(h.hour_local) < 10 THEN 'AM peak'
         WHEN hour(h.hour_local) < 16 THEN 'Midday'
         WHEN hour(h.hour_local) < 20 THEN 'PM peak'
         ELSE 'Evening' END                       AS day_part,
    w.temperature_c,
    w.precipitation_mm,
    w.wind_speed_kmh,
    w.weather_code,
    CASE WHEN w.hour_ts_utc IS NULL THEN 'Unknown'
         WHEN w.precipitation_mm >= 0.2 OR w.weather_code >= 51 THEN 'Wet'   -- WMO codes 51+ = drizzle/rain/snow
         ELSE 'Dry' END                           AS weather_condition,
    CASE WHEN w.temperature_c IS NULL THEN 'Unknown'
         WHEN w.temperature_c < 5  THEN 'Cold (<5C)'
         WHEN w.temperature_c < 15 THEN 'Cool (5-15C)'
         WHEN w.temperature_c < 25 THEN 'Mild (15-25C)'
         ELSE 'Hot (25C+)' END                    AS temp_band
FROM hours h
LEFT JOIN silver.weather_hourly   w   ON w.hour_ts_utc = h.hour_utc
LEFT JOIN silver.calendar_holiday hol ON hol.holiday_date = CAST(h.hour_local AS DATE);

-- --------------------------------------------------------------- facts
-- Station x hour. "Usable" snapshot = installed, renting, and reported
-- recently. A station that is offline is NOT counted as empty (different
-- problem, different fix), it is tracked separately in pct_offline.
-- churn = sum of |change in bikes| between snapshots: a usage proxy, since
-- GBFS has no trip data.
CREATE OR REPLACE TABLE gold.fct_station_hour AS
WITH s AS (
    SELECT *,
           is_installed AND is_renting AND NOT is_station_stale AS usable,
           abs(num_bikes_available - lag(num_bikes_available)
               OVER (PARTITION BY station_id ORDER BY snapshot_ts_utc)) AS bike_delta
    FROM silver.station_status
)
SELECT
    station_id,
    date_trunc('hour', snapshot_ts_utc)                                        AS hour_utc,
    COUNT(*)                                                                   AS n_snapshots,
    COUNT(*) FILTER (WHERE usable)                                             AS n_usable_snapshots,
    AVG(num_bikes_available)  FILTER (WHERE usable)                            AS avg_bikes,
    AVG(num_ebikes_available) FILTER (WHERE usable)                            AS avg_ebikes,
    AVG(num_docks_available)  FILTER (WHERE usable)                            AS avg_docks,
    MIN(num_bikes_available)  FILTER (WHERE usable)                            AS min_bikes,
    AVG(CASE WHEN num_bikes_available = 0 THEN 1.0 ELSE 0 END) FILTER (WHERE usable) AS pct_empty,
    AVG(CASE WHEN num_docks_available = 0 THEN 1.0 ELSE 0 END) FILTER (WHERE usable) AS pct_full,
    AVG(CASE WHEN usable THEN 0 ELSE 1.0 END)                                  AS pct_offline,
    COALESCE(SUM(bike_delta), 0)                                               AS bike_churn
FROM s
GROUP BY ALL;

-- Dockless supply per zone per hour. Excludes bikes that are reserved,
-- disabled, or sitting at a station (those are already in station counts).
CREATE OR REPLACE TABLE gold.fct_free_bike_zone_hour AS
WITH snaps AS (            -- how many free_bike snapshots landed each hour (denominator)
    SELECT date_trunc('hour', snapshot_ts_utc) AS hour_utc, COUNT(DISTINCT snapshot_ts_utc) AS n_snaps
    FROM silver.free_bike GROUP BY 1
),
per_snap AS (
    SELECT
        f.zone_id,
        f.snapshot_ts_utc,
        COUNT(*) AS n_free,
        COUNT(*) FILTER (WHERE (v.propulsion_type LIKE 'electric%' OR f.current_range_meters IS NOT NULL)
                           AND COALESCE(f.current_range_meters, 0) >= {min_usable_range_m}) AS n_free_ebike_usable,
        AVG(f.current_range_meters) AS avg_range_m
    FROM silver.free_bike f
    LEFT JOIN silver.vehicle_types v USING (vehicle_type_id)
    WHERE NOT f.is_reserved AND NOT f.is_disabled AND f.station_id IS NULL
    GROUP BY 1, 2
)
SELECT
    p.zone_id,
    date_trunc('hour', p.snapshot_ts_utc)               AS hour_utc,
    SUM(p.n_free) * 1.0 / any_value(sn.n_snaps)              AS avg_free_bikes,     -- snapshots with 0 bikes count as 0
    SUM(p.n_free_ebike_usable) * 1.0 / any_value(sn.n_snaps) AS avg_free_ebikes_usable,
    AVG(p.avg_range_m)                                   AS avg_range_m
FROM per_snap p
JOIN snaps sn ON sn.hour_utc = date_trunc('hour', p.snapshot_ts_utc)
GROUP BY 1, 2;

-- Zone x hour: everything a traveller could grab within a short walk.
CREATE OR REPLACE TABLE gold.fct_zone_hour AS
WITH station_part AS (
    SELECT s.zone_id, f.hour_utc,
           SUM(f.avg_bikes)               AS docked_bikes,
           SUM(f.avg_ebikes)              AS docked_ebikes,
           SUM(f.avg_docks)               AS open_docks,
           AVG(f.pct_empty)               AS avg_station_pct_empty,
           COUNT(*) FILTER (WHERE f.n_usable_snapshots > 0) AS stations_reporting,
           SUM(f.bike_churn)              AS bike_churn
    FROM gold.fct_station_hour f
    JOIN gold.dim_station s USING (station_id)
    GROUP BY 1, 2
),
free_hours AS (SELECT DISTINCT date_trunc('hour', snapshot_ts_utc) AS hour_utc FROM silver.free_bike),
spine AS (SELECT z.zone_id, h.hour_utc FROM gold.dim_zone z CROSS JOIN gold.dim_hour h)
SELECT
    sp.zone_id,
    sp.hour_utc,
    COALESCE(st.docked_bikes, 0)                                       AS docked_bikes,
    COALESCE(st.docked_ebikes, 0)                                      AS docked_ebikes,
    COALESCE(st.open_docks, 0)                                         AS open_docks,
    -- NULL (not 0) when the free-bike feed wasn't collected that hour
    CASE WHEN fh.hour_utc IS NULL THEN NULL ELSE COALESCE(fb.avg_free_bikes, 0) END         AS free_bikes,
    CASE WHEN fh.hour_utc IS NULL THEN NULL ELSE COALESCE(fb.avg_free_ebikes_usable, 0) END AS free_ebikes_usable,
    COALESCE(st.docked_bikes, 0) + COALESCE(fb.avg_free_bikes, 0)      AS total_bike_supply,
    st.avg_station_pct_empty,
    COALESCE(st.stations_reporting, 0)                                 AS stations_reporting,
    COALESCE(st.bike_churn, 0)                                         AS bike_churn
FROM spine sp
LEFT JOIN station_part st            ON st.zone_id = sp.zone_id AND st.hour_utc = sp.hour_utc
LEFT JOIN gold.fct_free_bike_zone_hour fb ON fb.zone_id = sp.zone_id AND fb.hour_utc = sp.hour_utc
LEFT JOIN free_hours fh              ON fh.hour_utc = sp.hour_utc;

-- --------------------------------------------------------------- marts
-- THE answer table: by zone, day type and hour of day, how often is supply
-- too thin to count on (< {low_supply_threshold} bikes within the walkable zone)?
CREATE OR REPLACE TABLE gold.mart_last_mile_risk AS
SELECT
    z.zone_id,
    z.zone_label,
    z.region_name,
    z.center_lat,
    z.center_lon,
    h.day_type,
    h.hour_of_day,
    COUNT(*)                                                                 AS observed_hours,
    ROUND(AVG(f.total_bike_supply), 1)                                       AS avg_bike_supply,
    ROUND(AVG(f.free_ebikes_usable), 1)                                      AS avg_free_ebikes_usable,
    ROUND(AVG(CASE WHEN f.total_bike_supply < {low_supply_threshold} THEN 1.0 ELSE 0 END), 3) AS pct_hours_low_supply,
    ROUND(AVG(f.avg_station_pct_empty), 3)                                   AS avg_station_pct_empty,
    ROUND(AVG(f.open_docks), 1)                                              AS avg_open_docks
FROM gold.fct_zone_hour f
JOIN gold.dim_zone z USING (zone_id)
JOIN gold.dim_hour h USING (hour_utc)
WHERE z.n_stations > 0 OR f.free_bikes > 0          -- skip zones with no supply at all that hour
GROUP BY ALL;

-- How weather and time of day shift availability and usage system-wide.
CREATE OR REPLACE TABLE gold.mart_weather_effect AS
WITH sys_hour AS (
    SELECT f.hour_utc,
           AVG(f.pct_empty)   AS station_pct_empty,
           AVG(f.avg_bikes)   AS bikes_per_station,
           AVG(f.bike_churn)  AS churn_per_station
    FROM gold.fct_station_hour f
    WHERE f.n_usable_snapshots > 0
    GROUP BY 1
)
SELECT
    h.weather_condition,
    h.temp_band,
    h.day_part,
    COUNT(*)                              AS observed_hours,
    ROUND(AVG(s.station_pct_empty), 3)    AS avg_station_pct_empty,
    ROUND(AVG(s.bikes_per_station), 2)    AS avg_bikes_per_station,
    ROUND(AVG(s.churn_per_station), 2)    AS avg_churn_per_station_hour
FROM sys_hour s
JOIN gold.dim_hour h USING (hour_utc)
GROUP BY ALL
ORDER BY weather_condition, day_part;
