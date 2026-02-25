USE ROLE ACCOUNTADMIN;
USE DATABASE WEATHER_DW;
USE SCHEMA CURATED;

CREATE OR REPLACE TAG data_sensitivity;

ALTER TABLE dim_location
SET TAG data_sensitivity = 'internal';

ALTER TABLE fact_weather_hourly
SET TAG data_sensitivity = 'internal';

-- Optional object tagging for governance catalogs
CREATE OR REPLACE TAG retention_policy;
ALTER TABLE WEATHER_DW.RAW.weather_api_raw
SET TAG retention_policy = '90_days';

-- Data retention example
ALTER TABLE WEATHER_DW.RAW.weather_api_raw
SET DATA_RETENTION_TIME_IN_DAYS = 7;
