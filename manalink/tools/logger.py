"""
.. module:: prototools.logger
    :platforms: Unix
    :synopsis: Custom logger for the Protocols with ANSI coloring

.. moduleauthor:: Graham Keenan 2020

"""

# System imports
import logging
from typing import Optional

def get_logger(name: str) -> logging.Logger:
    """Gets the logger with the specified name

    Args:
        name (str): Name of the logger

    Returns:
        logging.Logger: Logger
    """

    return logging.getLogger(name)

def make_logger(
    name: str,
    filename: Optional[str] = '',
    debug: Optional[bool] = False,
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

    # Using file logging, add FileHandler
    formatter = logging.Formatter(
        "[%(asctime)s] %(name)s::%(levelname)s -- %(message)s",
        "%d-%m-%Y | %H:%M:%S"
    )

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
