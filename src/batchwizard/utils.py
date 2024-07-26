# utils.py
from typing import Optional

from loguru import logger
from rich.console import Console

from .config import config


def setup_logger(console: Console = None):
    logger.remove()
    if console is None:
        console = Console()
    logger.add(
        console.file,
        format="<red>{time:YYYY-MM-DD HH:mm:ss}</red> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="ERROR",
        backtrace=True,
        diagnose=True,
    )
    return logger


def get_api_key() -> Optional[str]:
    """Get the API key from config or environment variable."""
    return config.get_api_key()


def set_api_key(api_key: str) -> None:
    """Set the API key in the config."""
    config.set_api_key(api_key)


def get_settings():
    """Get the current settings."""
    return config.settings
