USE DATABASE WEATHER_DW;
USE SCHEMA ANALYTICS;

-- 1) CTE with anomaly detection using rolling stats
WITH hourly_base AS (
  SELECT
    h.location_key,
    h.observed_ts,
    h.temperature_c,
    DATE_TRUNC('day', h.observed_ts) AS day_bucket
  FROM WEATHER_DW.CURATED.fact_weather_hourly h
), rolling_stats AS (
  SELECT
    location_key,
    observed_ts,
    temperature_c,
    AVG(temperature_c) OVER (
      PARTITION BY location_key
      ORDER BY observed_ts
      ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
    ) AS avg_prev_24h,
    STDDEV(temperature_c) OVER (
      PARTITION BY location_key
      ORDER BY observed_ts
      ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
    ) AS std_prev_24h
  FROM hourly_base
)
SELECT
  location_key,
  observed_ts,
  temperature_c,
  avg_prev_24h,
  std_prev_24h,
  CASE
    WHEN std_prev_24h > 0
      AND ABS(temperature_c - avg_prev_24h) > 2 * std_prev_24h THEN 1
    ELSE 0
  END AS is_temperature_anomaly
FROM rolling_stats;

-- 2) Event ranking per region
SELECT
  l.country,
  l.state,
  e.event_type,
  COUNT(*) AS event_count,
  DENSE_RANK() OVER (PARTITION BY l.country ORDER BY COUNT(*) DESC) AS event_rank_in_country
FROM WEATHER_DW.CURATED.fact_weather_events e
JOIN WEATHER_DW.CURATED.dim_location l
  ON e.location_key = l.location_key
GROUP BY 1,2,3;

-- 3) Hourly to daily rollup with percentile and lag
SELECT
  h.location_key,
  DATE(h.observed_ts) AS observed_date,
  AVG(h.temperature_c) AS avg_temp,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY h.wind_speed_kph) AS p95_wind,
  SUM(h.precipitation_mm) AS total_precip_mm,
  AVG(h.temperature_c) - LAG(AVG(h.temperature_c)) OVER (
    PARTITION BY h.location_key
    ORDER BY DATE(h.observed_ts)
  ) AS delta_vs_prev_day
FROM WEATHER_DW.CURATED.fact_weather_hourly h
GROUP BY 1,2;

-- 4) Search optimization for selective filters (indexing-like)
ALTER TABLE WEATHER_DW.CURATED.fact_weather_events
ADD SEARCH OPTIMIZATION ON EQUALITY(event_type, severity, location_key);
