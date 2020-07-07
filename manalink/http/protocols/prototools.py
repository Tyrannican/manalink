"""
.. module:: http.protocols.prototools
    :platforms: Unix
    :synopsis: Collection of helper tools for use within protocols

.. moduleauthor:: Graham Keenan 2020

"""

# System imports
import json
import asyncio
from enum import Enum
import requests
from typing import Optional, ByteString, Any, Dict, Union, List

# Third-party imports
from jsonrpc.exceptions import JSONRPCInvalidRequestException
from jsonrpc.jsonrpc2 import JSONRPC20Response, JSONRPC20Request, JSONRPCError

class ManaRPCError(Enum):
    """Enum to represent the types of JSON RPC 2.0 errors
    """

    NO_METHOD = -32601  # Method not on server
    CONNECTION = -32000  # Come error in connecting to nodes

class ManaRPCPort(Enum):
    """Enum for representing the ManaLink RPC Protocol ports.
    Any new stable protocols will be added here
    """

    DEFAULT = 4000  # Default port for testing
    DISCOVERY = 4050  # Discovery protocol

class Address:
    """Class to represent a Node address with address and port

    Args:
        host (Optional[str]): Host address

        port (Optional[Union[ManaRPCPort, int]]): Communication port.
        Defaults to ManaRPCPort.DEFAULT (4000)

    """

    def __init__(
        self,
        host: Optional[str] = '',
        port: Optional[Union[ManaRPCPort, int]] = ManaRPCPort.DEFAULT
    ):
        # Host address
        self.host = host

        # Convert to number if already a ManaRPCPort
        self.port = port.value if isinstance(port, ManaRPCPort) else port

    def __repr__(self) -> str:
        """String representation of address

        Returns:
            str: Address as string
        """

        return f'Address(host={self.host}, port={self.port})'

async def ping_node(node: str, port: Optional[int] = -1) -> bool:
    """Ping a node to determine if it is online or not

    Args:
        node (str): Node address
        port (Optional[int], optional): Node port. Defaults to -1.

    Returns:
        bool: Node is online or not
    """

    try:
        # Attempt to open the connection
        _, _ = await asyncio.open_connection(
            host=node, port=port
        )

        # Success
        return True

    # Unsuccessful
    except (ConnectionRefusedError, ConnectionResetError):
        return False

def extract_rpc_call(request: ByteString) -> Optional[JSONRPC20Request]:
    """Extract the RPC call from the request bytes sent by a node.

    Args:
        request (ByteString): Request bytes

    Returns:
        Optional[JSONRPC20Request]: Decoded request if successful, else None.
    """

    try:
        # Split the HTTP request btyes to get the RPC information
        rpc_string = request.decode().split('\r\n\r\n')[1]

        # Load it ad a dictionary
        rpc = json.loads(rpc_string)

        # Build the JSONRPC20Request from the raw dictionary
        return JSONRPC20Request.from_json(rpc)

    # Unable to convert
    except (json.JSONDecodeError, JSONRPCInvalidRequestException):
        return None

def http_response_builder(
    host: Optional[str] = '',
    port: Optional[int] = -1,
    data: Optional[JSONRPC20Response] = None
) -> str:
    """Creates an HTTP response from supplied data
    Response is always OK as this is special case

    Args:
        host (Optional[str], optional): Host address. Defaults to ''.

        port (Optional[int], optional): Host port. Defaults to -1.

        data (Optional[JSONRPC20Response], optional): Data to send in response.
        Defaults to None.

    Returns:
        str: HTTP Response
    """

    return (
        'HTTP/1.1 200 OK\r\n'
        + f'Host: {host}:{port}\r\n'
        + 'Content-Type: application/json\r\n'
        + 'Accept-Encoding: gzip, deflate\r\n'
        + '\r\n'
        + data.json if data is not None else {}
    )

def generate_rpc_error(
    code: Optional[int] = None,
    message: Optional[str] = None,
    data: Optional[Any] = None
) -> Dict:
    """Creates a JSON RPC 2.0 error and converts it to a dictionary

    Args:
        code (Optional[int], optional): JSOn RPC 2.0 Errr code.
        Defaults to None.

        message (Optional[str], optional): Error message to send.
        Defaults to None.

        data (Optional[Any], optional): Any error data to send back.
        Defaults to None.

    Returns:
        Dict: JSON RPC 2.0 Error as dictionary
    """

    error = JSONRPCError(code=code, message=message, data=data)
    return json.loads(error.json)

def parse_response(response: requests.Response) -> JSONRPC20Response:
    """Load the response from returned JSON text

    Args:
        response (requests.Response): Response object

    Returns:
        JSONRPC20Response: RPC Response information
    """

    return JSONRPC20Response.from_json(response.text)

def notify_node(
    node: Address, function: str, args: List[Any] = []
) -> JSONRPC20Response:
    """Creates a JSON RPC request to call the supplied method on the server
    in order to obtain the results.

    Args:
        node (Address): Server address

        function (str): Function/method to call

        args (List[Any], optional): Any args for the function/method call.
        Defaults to [].

    Returns:
        JSONRPC20Response: Response from the server
    """

    try:
        # Build the request and send to the server
        request = JSONRPC20Request(method=function, params=args)
        response = requests.post(
            f'http://{node.host}:{node.port}/',
            json=request.json
        )

        # Return the parsed response
        return parse_response(response)

    # Cannot connect to the node
    except requests.ConnectionError:
        # Return an error Response
        return JSONRPC20Response(
            error=generate_rpc_error(
                code=ManaRPCError.CONNECTION.value,
                message=f'No connection to {node.host}:{node.port}'
            )
        )
