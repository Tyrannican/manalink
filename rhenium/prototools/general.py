"""
.. module:: prototools.general
    :platforms: Unix
    :synopsis: General functions

.. moduleauthor:: Graham Keenan 2020

"""
from typing import Optional

ANSI_COLORS = {
    'black': '\u001b[30m',
    'red': '\u001b[31m',
    'green': '\u001b[32m',
    'yellow': '\u001b[33m',
    'blue': '\u001b[34m',
    'magenta': '\u001b[35m',
    'cyan': '\u001b[36m',
    'white': '\u001b[37m',
    'bold': '\u001b[1m',
    'reset': '\u001b[0m'
}

def colour_item(
    msg: str, color: Optional[str] = '', bold: Optional[bool] = False
) -> str:
    """Colours a message with an ANSI color and escapes it at the end.
    Options for bold text.

    Args:
        msg (str): Message to colour
        color (str): Colour of the text
        bold (Optional[bool], optional): Bold the message. Defaults to False.

    Returns:
        str: ANSI formatted message
    """

    color = ANSI_COLORS[color] if color in ANSI_COLORS else ''

    return (
        f'{color}{ANSI_COLORS["bold"]}{msg}{ANSI_COLORS["reset"]}' if bold
        else f'{color}{msg}{ANSI_COLORS["reset"]}'
    )
