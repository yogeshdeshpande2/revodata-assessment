from __future__ import annotations

import logging

class DatabricksLogger:
    """
    A simple logger class for Databricks notebooks that wraps around the standard Python logging module.
    It provides methods for logging messages at different levels (INFO, WARNING, ERROR) and can be easily
    extended to include additional functionality if needed.
    """

    logger = logging.getLogger(__name__)

    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(handler)

    @classmethod
    def _log(cls, level: str, message: str):
        cls.logger.setLevel(level)
        cls.logger.log(level, message)
    
    @staticmethod
    def info(message: str, **kwargs):
        """Log an informational message."""
        if kwargs:
            message = f"{message} | {kwargs}"
        DatabricksLogger._log(logging.INFO, message)

    @staticmethod
    def warning(message: str, **kwargs):
        """Log a warning message."""
        if kwargs:
            message = f"{message} | {kwargs}"
        DatabricksLogger._log(logging.WARNING, message)

    @staticmethod
    def error(message: str, **kwargs):
        """Log an error message."""
        if kwargs:
            message = f"{message} | {kwargs}"
        DatabricksLogger._log(logging.ERROR, message)
        
    @staticmethod
    def debug(message: str, **kwargs):
        """Log a debug message."""
        if kwargs:
            message = f"{message} | {kwargs}"
        DatabricksLogger._log(logging.DEBUG, message)        
