USE DATABASE WEATHER_DW;
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE dim_date (
  date_key NUMBER PRIMARY KEY,
  full_date DATE,
  day_of_week NUMBER,
  day_name STRING,
  month_number NUMBER,
  month_name STRING,
  quarter_number NUMBER,
  year_number NUMBER,
  is_weekend BOOLEAN
);

CREATE OR REPLACE TABLE dim_location (
  location_key NUMBER AUTOINCREMENT PRIMARY KEY,
  location_id STRING,
  city STRING,
  state STRING,
  country STRING,
  country_code STRING,
  latitude FLOAT,
  longitude FLOAT,
  timezone STRING,
  valid_from TIMESTAMP_NTZ,
  valid_to TIMESTAMP_NTZ,
  is_current BOOLEAN
);

CREATE OR REPLACE TABLE dim_station (
  station_key NUMBER AUTOINCREMENT PRIMARY KEY,
  station_id STRING,
  provider STRING,
  elevation_m FLOAT,
  install_date DATE,
  valid_from TIMESTAMP_NTZ,
  valid_to TIMESTAMP_NTZ,
  is_current BOOLEAN
);

CREATE OR REPLACE TABLE dim_source (
  source_key NUMBER AUTOINCREMENT PRIMARY KEY,
  source_name STRING,
  feed_type STRING,
  cadence_minutes NUMBER
);

CREATE OR REPLACE TABLE fact_weather_hourly (
  date_key NUMBER,
  location_key NUMBER,
  station_key NUMBER,
  source_key NUMBER,
  observed_ts TIMESTAMP_NTZ,
  temperature_c FLOAT,
  humidity_pct FLOAT,
  pressure_hpa FLOAT,
  wind_speed_kph FLOAT,
  precipitation_mm FLOAT,
  visibility_km FLOAT,
  aqi NUMBER,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT fk_weather_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
  CONSTRAINT fk_weather_location FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
  CONSTRAINT fk_weather_station FOREIGN KEY (station_key) REFERENCES dim_station(station_key),
  CONSTRAINT fk_weather_source FOREIGN KEY (source_key) REFERENCES dim_source(source_key)
);

CREATE OR REPLACE TABLE fact_weather_events (
  event_id STRING,
  date_key NUMBER,
  location_key NUMBER,
  station_key NUMBER,
  event_type STRING,
  severity STRING,
  started_ts TIMESTAMP_NTZ,
  ended_ts TIMESTAMP_NTZ,
  duration_minutes NUMBER,
  impact_score FLOAT,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE fact_weather_hourly CLUSTER BY (date_key, location_key);
ALTER TABLE fact_weather_events CLUSTER BY (date_key, severity);
