"""
.. module:: prototools.logger
    :platforms: Unix
    :synopsis: Custom logger for the Protocols with ANSI coloring

.. moduleauthor:: Graham Keenan 2020

"""

# System imports
import time
import logging
from typing import Optional

from .general import colour_item

def make_logger(
    name: str, filename: Optional[str] = '', debug: Optional[bool] = False
) -> logging.Logger:
    """Creates a logger using the custom ProtoFormatter with options for
    file output.

    Args:
        name (str): Name of the logger
        filename (Optional[str], optional): Output log file. Defaults to ''.
        debug (Optional[bool], optional): Debug mode. Defaults to False.

    Returns:
        logging.Logger: Logger
    """

    # Get logger and set level
    logger = logging.getLogger(name)
    level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(level)

    # Custom ANSI colour formatter
    formatter = ProtoFormatter()

    # Using file logging, add FileHandler
    if filename:
        fh = logging.FileHandler(filename=filename)
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # Setup stream handler
    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return logger


class ProtoFormatter(logging.Formatter):
    """Custom Formatter to support ANSI colouring

    Inherits:
        logging.Formatter: Base Formatter
    """

    def __init__(self):
        super().__init__()

    def format(self, record: logging.LogRecord) -> str:
        """Formats the LogRecord with custom formatting

        Args:
            record (logging.LogRecord): Record to format

        Returns:
            str: Formatted Text
        """

        # Get level and level number
        level, levelno, msg = record.levelname, record.levelno, record.msg

        # Colour level name depending on level severity
        if levelno == logging.DEBUG:
            level = colour_item(level, color='cyan')
        elif levelno == logging.INFO:
            level = colour_item(level, color='green')
        elif levelno == logging.WARN:
            level = colour_item(level, color='yellow', bold=True)
            msg = colour_item(msg, color='yellow')
        elif levelno == logging.ERROR:
            level = colour_item(level, color='red', bold=True)
            msg = colour_item(msg, color='red', bold=True)
        elif levelno == logging.CRITICAL:
            level = colour_item(level, color='red', bold=True)
            msg = colour_item(msg, color='red')

        # Log the current time
        timestamp = time.strftime('%d-%m-%Y|%H:%M:%S')

        # Colour the logger name
        name = colour_item(record.name, color='magenta')

        # Formatted message
        return f'[{timestamp}] - {name}::{level} -- {msg}'
