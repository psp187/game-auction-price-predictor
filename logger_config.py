import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logger(logger_name: str, log_file_name: str) -> logging.Logger:
    log_dir = Path(".") / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / log_file_name

    logger = logging.getLogger(logger_name)
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(log_file, maxBytes=5242880, backupCount=5, encoding="utf-8")
    log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(log_format)

    logger.addHandler(handler)

    return logger