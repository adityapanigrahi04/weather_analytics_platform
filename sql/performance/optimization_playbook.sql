USE DATABASE WEATHER_DW;

-- 1) Materialized view for repeated aggregates
CREATE OR REPLACE MATERIALIZED VIEW ANALYTICS.mv_daily_weather_summary AS
SELECT
  location_key,
  DATE(observed_ts) AS observed_date,
  AVG(temperature_c) AS avg_temp,
  MAX(temperature_c) AS max_temp,
  MIN(temperature_c) AS min_temp,
  SUM(precipitation_mm) AS total_precip_mm
FROM CURATED.fact_weather_hourly
GROUP BY 1,2;

-- 2) Query profile checks
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER(USER_NAME => CURRENT_USER(), RESULT_LIMIT => 50));

-- 3) Result cache / warehouse sizing guidelines
-- Use XS/S for dev and medium+ for heavy joins.
-- Keep auto-suspend low and avoid noisy long-running sessions.

-- 4) Clustering health checks
-- SELECT SYSTEM$CLUSTERING_INFORMATION('WEATHER_DW.CURATED.FACT_WEATHER_HOURLY');

-- 5) Search optimization for needle-in-haystack queries
ALTER TABLE CURATED.fact_weather_hourly
ADD SEARCH OPTIMIZATION ON EQUALITY(location_key);
