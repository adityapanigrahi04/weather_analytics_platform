import time
import yaml
from extract import extract_weather
from transform import transform_weather_payload
from load import get_connection, load_raw_payload, merge_curated_hourly
from utils import configure_logger, resolve_env_template

logger = configure_logger(__name__)


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Resolve ${ENV_VAR} values
    for section, values in config.items():
        if isinstance(values, dict):
            for key, value in values.items():
                config[section][key] = resolve_env_template(value)
    return config


def run_pipeline(config_path: str = "etl/config/settings.yaml"):
    config = load_config(config_path)

    retries = int(config["pipeline"]["max_retries"])
    wait_seconds = int(config["pipeline"]["retry_wait_seconds"])

    for attempt in range(1, retries + 1):
        try:
            payload = extract_weather(config["api"])
            rows = transform_weather_payload(payload)

            conn = get_connection(config["snowflake"])
            try:
                load_raw_payload(conn, payload)
                merge_curated_hourly(conn, rows)
                conn.commit()
            finally:
                conn.close()

            logger.info("Pipeline succeeded with %d rows", len(rows))
            return
        except Exception as exc:
            logger.exception("Attempt %d/%d failed: %s", attempt, retries, exc)
            if attempt == retries:
                raise
            time.sleep(wait_seconds)


if __name__ == "__main__":
    run_pipeline()
