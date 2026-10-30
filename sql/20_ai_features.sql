-- =====================================================================
-- AI LAYER (predictive part): next-hour "station runs empty" risk.
--
-- A deliberately simple, explainable baseline:
--   1. Historical empty rate for this station at the same hour of day and
--      day type (weekday vs weekend/holiday).
--   2. Shrunk toward the zone's rate (and then the system rate) when the
--      station has few observations: p = (n*p_station + m*p_prior)/(n + m).
--      Stops a station seen empty once in 1 hour from showing 100% risk.
--   3. If the next-hour forecast is wet, scaled by the observed wet-vs-dry
--      empty ratio - but only once there are at least {min_wet_hours} wet hours
--      of history, and capped to [0.5, 2.0]. With less evidence the factor
--      is 1.0 (no weather adjustment), so a handful of rainy hours can't
--      swing the estimate. Its direction comes from the data, not an assumption.
-- This is the benchmark a gradient-boosted model (station, hour, dow,
-- weather, lagged availability) would have to beat in production, tracked
-- in MLflow. With only days of data, a heavier model would overfit.
-- =====================================================================
CREATE OR REPLACE TABLE gold.pred_station_empty_next_hour AS
WITH target AS (
    SELECT MAX(hour_utc) + INTERVAL 1 HOUR AS target_hour_utc FROM gold.fct_station_hour
),
target_ctx AS (
    SELECT t.target_hour_utc,
           w.precipitation_mm, w.weather_code, w.hour_ts_local AS target_hour_local
    FROM target t
    LEFT JOIN silver.weather_hourly w ON w.hour_ts_utc = t.target_hour_utc
),
hist AS (
    SELECT f.station_id, s.zone_id, h.day_type, h.hour_of_day,
           f.pct_empty, f.n_usable_snapshots
    FROM gold.fct_station_hour f
    JOIN gold.dim_station s USING (station_id)
    JOIN gold.dim_hour h USING (hour_utc)
    WHERE f.n_usable_snapshots > 0
),
-- target local hour / day type: use the forecast row if present, otherwise derive from the last hour seen + 1
tgt AS (
    SELECT
        COALESCE(c.target_hour_local, (SELECT MAX(hour_local) FROM gold.dim_hour) + INTERVAL 1 HOUR) AS target_hour_local,
        c.target_hour_utc,
        CASE WHEN c.weather_code IS NULL THEN 'Unknown'
             WHEN c.precipitation_mm >= 0.2 OR c.weather_code >= 51 THEN 'Wet' ELSE 'Dry' END AS forecast_condition
    FROM target_ctx c
),
tgt2 AS (
    SELECT *, hour(target_hour_local) AS hour_of_day,
           CASE WHEN isodow(target_hour_local) >= 6
                  OR CAST(target_hour_local AS DATE) IN (SELECT holiday_date FROM silver.calendar_holiday)
                THEN 'Weekend/Holiday' ELSE 'Weekday' END AS day_type
    FROM tgt
),
station_rate AS (
    SELECT h.station_id, h.zone_id, COUNT(*) AS n_obs, AVG(h.pct_empty) AS p_station
    FROM hist h JOIN tgt2 t ON t.hour_of_day = h.hour_of_day AND t.day_type = h.day_type
    GROUP BY 1, 2
),
zone_rate AS (
    SELECT h.zone_id, AVG(h.pct_empty) AS p_zone
    FROM hist h JOIN tgt2 t ON t.hour_of_day = h.hour_of_day AND t.day_type = h.day_type
    GROUP BY 1
),
system_rate AS (
    SELECT AVG(h.pct_empty) AS p_system
    FROM hist h JOIN tgt2 t ON t.hour_of_day = h.hour_of_day AND t.day_type = h.day_type
),
weather_factor AS (     -- how much more/less often stations are empty when wet vs dry
    SELECT CASE
             WHEN COALESCE(SUM(observed_hours) FILTER (WHERE weather_condition = 'Wet'), 0) < {min_wet_hours}
               THEN 1.0                                   -- not enough wet history to trust a ratio
             ELSE LEAST(2.0, GREATEST(0.5, COALESCE(
                    AVG(avg_station_pct_empty) FILTER (WHERE weather_condition = 'Wet')
                    / NULLIF(AVG(avg_station_pct_empty) FILTER (WHERE weather_condition = 'Dry'), 0), 1.0)))
           END AS wet_to_dry
    FROM gold.mart_weather_effect
)
SELECT
    s.station_id,
    d.name                                   AS station_name,
    s.zone_id,
    d.lat, d.lon,
    t.target_hour_local,
    t.day_type,
    t.forecast_condition,
    s.n_obs                                  AS history_hours,
    ROUND(s.p_station, 3)                    AS p_station_raw,
    ROUND(LEAST(1.0, GREATEST(0.0,
        ((s.n_obs * s.p_station + 3 * COALESCE(z.p_zone, sr.p_system)) / (s.n_obs + 3))
        * CASE WHEN t.forecast_condition = 'Wet' THEN wf.wet_to_dry ELSE 1.0 END
    )), 3)                                   AS p_empty_next_hour,
    ROUND(CASE WHEN t.forecast_condition = 'Wet' THEN wf.wet_to_dry ELSE 1.0 END, 3) AS weather_factor,
    'baseline_v2_shrunk_hourly_rate'         AS model_version,
    now()                                    AS scored_at
FROM station_rate s
JOIN gold.dim_station d USING (station_id)
LEFT JOIN zone_rate z ON z.zone_id = s.zone_id
CROSS JOIN system_rate sr
CROSS JOIN tgt2 t
CROSS JOIN weather_factor wf;

ALTER TABLE gold.pred_station_empty_next_hour ADD COLUMN risk_band VARCHAR;
UPDATE gold.pred_station_empty_next_hour
SET risk_band = CASE WHEN p_empty_next_hour >= 0.5 THEN 'High'
                     WHEN p_empty_next_hour >= 0.2 THEN 'Medium' ELSE 'Low' END;
