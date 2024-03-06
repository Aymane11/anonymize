from .models import load_config, Config, Source, Output
from loguru import logger
import sys


logger.remove()  # All configured handlers are removed
logger.add(
    sys.stdout,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <level>{message}</level>",
)

__all__ = [
    "load_config",
    "Config",
    "Source",
    "Output",
]
