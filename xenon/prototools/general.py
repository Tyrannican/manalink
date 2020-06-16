"""
.. module:: prototools.general
    :platforms: Unix
    :synopsis: General logger mainly used by the Protocols

.. moduleauthor:: Graham Keenan 2020

"""

# System imports
import logging

def make_logger(name: str) -> logging.Logger:
    """Create a basic logger with Stream handling

    Args:
        name (str): Name of the logger

    Returns:
        logging.Logger: Logger
    """

    # Set default level to INFO
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Add basic formatting
    formatter = logging.Formatter(
        '%(name)s::%(levelname)s [%(asctime)s] %(message)s'
    )

    # Add stream handler to display on stdout/stderr
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)

    # Set the formatter
    sh.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(sh)

    return logger
