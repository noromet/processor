"""
This file configures the logger for the application.
"""

import logging


class ColoredFormatter(logging.Formatter):
    """
    Custom formatter to add colors to log messages based on their level.
    """

    # Define color codes
    COLORS = {
        "DEBUG": "\033[0;96m",  # Cyan
        "INFO": "",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[1;31m",  # Bold Red
    }
    RESET = "\033[0m"

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.RESET)
        message = super().format(record)
        return f"{log_color}{message}{self.RESET}"


def config_logger(debug: bool = False) -> None:
    """
    Configures the logger to use a custom formatter with colors for different log levels.
    """
    # Get the root logger
    logger = logging.getLogger()

    # Remove any existing handlers to avoid duplication
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Add our custom handler
    handler = logging.StreamHandler()
    formatter = ColoredFormatter("[%(asctime)s] %(levelname)s: %(message)s")
    handler.setFormatter(formatter)

    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.addHandler(handler)
