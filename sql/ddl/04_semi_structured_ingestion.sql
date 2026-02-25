USE DATABASE WEATHER_DW;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE weather_api_raw (
  ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  source STRING,
  payload VARIANT
);

-- Example JSON load
-- COPY INTO weather_api_raw(payload)
-- FROM @my_json_stage
-- FILE_FORMAT = (TYPE = JSON);

-- Flatten nested arrays from payload
CREATE OR REPLACE VIEW weather_events_flattened AS
SELECT
  ingestion_ts,
  source,
  payload:location:id::STRING AS location_id,
  payload:location:name::STRING AS location_name,
  event.value:type::STRING AS event_type,
  event.value:severity::STRING AS severity,
  event.value:start::TIMESTAMP_NTZ AS started_ts,
  event.value:end::TIMESTAMP_NTZ AS ended_ts
FROM weather_api_raw,
LATERAL FLATTEN(input => payload:events) event;

-- Time Travel usage examples
-- SELECT * FROM WEATHER_DW.CURATED.fact_weather_hourly AT (OFFSET => -3600);
-- UNDROP TABLE WEATHER_DW.CURATED.fact_weather_hourly;
