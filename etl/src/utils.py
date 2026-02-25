import logging
import os
from string import Template


def configure_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def resolve_env_template(value: str) -> str:
    if not isinstance(value, str):
        return value
    return Template(value).safe_substitute(os.environ)
