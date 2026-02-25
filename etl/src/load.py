import json
import snowflake.connector
from utils import configure_logger

logger = configure_logger(__name__)


def get_connection(sf_config: dict):
    return snowflake.connector.connect(
        account=sf_config["account"],
        user=sf_config["user"],
        password=sf_config["password"],
        role=sf_config["role"],
        warehouse=sf_config["warehouse"],
        database=sf_config["database"],
        schema=sf_config["schema_raw"],
    )


def load_raw_payload(conn, payload: dict, source: str = "open-meteo"):
    logger.info("Loading payload into RAW.weather_api_raw")
    sql = """
        INSERT INTO WEATHER_DW.RAW.weather_api_raw (source, payload)
        SELECT %s, PARSE_JSON(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (source, json.dumps(payload)))


def merge_curated_hourly(conn, rows: list[dict], location_key: int = 1, station_key: int = 1, source_key: int = 1):
    logger.info("Merging transformed records into CURATED.fact_weather_hourly")
    merge_sql = """
        MERGE INTO WEATHER_DW.CURATED.fact_weather_hourly tgt
        USING (
          SELECT
            TO_NUMBER(TO_CHAR(%(observed_ts)s::TIMESTAMP_NTZ, 'YYYYMMDD')) AS date_key,
            %(location_key)s::NUMBER AS location_key,
            %(station_key)s::NUMBER AS station_key,
            %(source_key)s::NUMBER AS source_key,
            %(observed_ts)s::TIMESTAMP_NTZ AS observed_ts,
            %(temperature_c)s::FLOAT AS temperature_c,
            %(humidity_pct)s::FLOAT AS humidity_pct,
            %(pressure_hpa)s::FLOAT AS pressure_hpa,
            %(wind_speed_kph)s::FLOAT AS wind_speed_kph,
            %(precipitation_mm)s::FLOAT AS precipitation_mm,
            NULL::FLOAT AS visibility_km,
            NULL::NUMBER AS aqi
        ) src
        ON tgt.location_key = src.location_key
           AND tgt.station_key = src.station_key
           AND tgt.observed_ts = src.observed_ts
        WHEN MATCHED THEN UPDATE SET
            temperature_c = src.temperature_c,
            humidity_pct = src.humidity_pct,
            pressure_hpa = src.pressure_hpa,
            wind_speed_kph = src.wind_speed_kph,
            precipitation_mm = src.precipitation_mm,
            load_ts = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            date_key, location_key, station_key, source_key, observed_ts,
            temperature_c, humidity_pct, pressure_hpa, wind_speed_kph,
            precipitation_mm, visibility_km, aqi
        ) VALUES (
            src.date_key, src.location_key, src.station_key, src.source_key, src.observed_ts,
            src.temperature_c, src.humidity_pct, src.pressure_hpa, src.wind_speed_kph,
            src.precipitation_mm, src.visibility_km, src.aqi
        )
    """

    with conn.cursor() as cur:
        for row in rows:
            params = {
                **row,
                "location_key": location_key,
                "station_key": station_key,
                "source_key": source_key,
            }
            cur.execute(merge_sql, params)
