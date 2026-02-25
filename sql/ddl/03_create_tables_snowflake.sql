USE DATABASE WEATHER_DW;
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE dim_country (
  country_key NUMBER AUTOINCREMENT PRIMARY KEY,
  country_name STRING,
  country_code STRING
);

CREATE OR REPLACE TABLE dim_state (
  state_key NUMBER AUTOINCREMENT PRIMARY KEY,
  state_name STRING,
  country_key NUMBER
);

CREATE OR REPLACE TABLE dim_city (
  city_key NUMBER AUTOINCREMENT PRIMARY KEY,
  city_name STRING,
  state_key NUMBER,
  latitude FLOAT,
  longitude FLOAT,
  timezone STRING
);

CREATE OR REPLACE TABLE dim_location_snowflake (
  location_key NUMBER AUTOINCREMENT PRIMARY KEY,
  location_id STRING,
  city_key NUMBER,
  valid_from TIMESTAMP_NTZ,
  valid_to TIMESTAMP_NTZ,
  is_current BOOLEAN
);
