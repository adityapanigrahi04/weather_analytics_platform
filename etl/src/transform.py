from datetime import datetime
from utils import configure_logger

logger = configure_logger(__name__)


def transform_weather_payload(payload: dict) -> list[dict]:
    logger.info("Transforming weather payload")

    hourly = payload.get("hourly", {})
    timestamps = hourly.get("time", [])
    temperature = hourly.get("temperature_2m", [])
    humidity = hourly.get("relative_humidity_2m", [])
    precipitation = hourly.get("precipitation", [])
    pressure = hourly.get("pressure_msl", [])
    windspeed = hourly.get("windspeed_10m", [])

    rows = []
    for idx, ts in enumerate(timestamps):
        rows.append(
            {
                "observed_ts": datetime.fromisoformat(ts),
                "temperature_c": temperature[idx] if idx < len(temperature) else None,
                "humidity_pct": humidity[idx] if idx < len(humidity) else None,
                "precipitation_mm": precipitation[idx] if idx < len(precipitation) else None,
                "pressure_hpa": pressure[idx] if idx < len(pressure) else None,
                "wind_speed_kph": windspeed[idx] if idx < len(windspeed) else None,
            }
        )
    return rows
