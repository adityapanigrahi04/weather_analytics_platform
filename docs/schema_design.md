# Warehouse Schema Design

## Business Goals
- Analyze historical and near-real-time weather trends.
- Track severe events and alerts.
- Measure data quality and ingestion latency.

## Grain
- `fact_weather_hourly`: one row per `location_id` per hour.
- `fact_weather_events`: one row per weather event occurrence.

## Star Schema (Recommended for BI)

### Facts
- `fact_weather_hourly`
  - Keys: `date_key`, `location_key`, `station_key`, `source_key`
  - Metrics: temperature, humidity, pressure, wind_speed, precipitation_mm, visibility_km, aqi, load_ts
- `fact_weather_events`
  - Keys: `event_id`, `date_key`, `location_key`, `station_key`
  - Metrics: severity, duration_minutes, impact_score

### Dimensions
- `dim_date` (calendar attributes)
- `dim_location` (city, state, country, lat, lon, timezone)
- `dim_station` (station id, provider, install date, elevation)
- `dim_source` (API provider, feed type, refresh cadence)
- `dim_event_type` (rain, storm, snow, heatwave, etc.)

## Snowflake Schema (Normalized dimensions)
Use when dimension growth and maintenance complexity increase.

- `dim_location` references:
  - `dim_city`
  - `dim_state`
  - `dim_country`

## SCD Strategy
- `dim_location`: SCD Type 2 for timezone, admin boundary, geo metadata changes.
- `dim_station`: SCD Type 2 for station attributes.

## Surrogate Keys
- Integer surrogate keys on all dimensions for join performance.
- Keep natural keys in dimensions for traceability.

## Partition and Clustering Strategy
- Snowflake cluster keys:
  - `fact_weather_hourly`: `(date_key, location_key)`
  - `fact_weather_events`: `(date_key, severity)`
- Spark write partitioning:
  - partition by `event_date` and `country_code` for large event output tables.
