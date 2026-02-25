import requests
from utils import configure_logger

logger = configure_logger(__name__)


def extract_weather(api_config: dict) -> dict:
    params = {
        "latitude": api_config["latitude"],
        "longitude": api_config["longitude"],
        "hourly": api_config["hourly"],
    }
    logger.info("Extracting weather data from API")
    response = requests.get(api_config["weather_url"], params=params, timeout=30)
    response.raise_for_status()
    return response.json()
