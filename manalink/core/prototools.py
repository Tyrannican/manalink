"""
.. module:: core.prototools
    :synopsis: Helper functions for the Core ManaLink protocol
    :platforms: Unix

.. moduleauthor:: Graham Keenan 2020

"""

# System imports
import json
import asyncio
from dataclasses import dataclass
from typing import ByteString, Optional, Any, Dict, List

# External imports
from jsonrpc.jsonrpc2 import (
    JSONRPCError,
    JSONRPC20Request,
    JSONRPC20Response,
    JSONRPCInvalidRequestException
)

from ..tools import constants as cst

@dataclass
class Address:
    """Simple data class to represent a Network address

    Args:
        host (Optional[str]): Network host address. Default to "".
        port (Optional[int]): Network port. Defaultgs to 4000.
    """

    host: Optional[str] = ""
    port: Optional[int] = 4000

def extract_rpc_request(request: ByteString) -> Optional[JSONRPC20Request]:
    """Extracts a JSON RPC 2.0 request from an incoming bytestring

    Args:
        request (ByteString): Incoming request from a client

    Returns:
        Optional[JSONRPC20Request]: JSONRPCRequest if successful, otherwise None
    """

    try:
        # Decode from bytes to string
        rpc_string = request.decode("utf-8")

        # Return request
        return JSONRPC20Request.from_json(rpc_string)

    # Unable to convert to JSONRPC20Request
    except (json.JSONDecodeError, JSONRPCInvalidRequestException):
        return None

def extract_rpc_response(response: ByteString) -> JSONRPC20Response:
    """Decodes a bytestring into a JSONRPC20Response

    Args:
        response (ByteString): INcoming response from a client

    Returns:
        JSONRPC20Response: Response object
    """

    response = response.decode("utf-8")
    return JSONRPC20Response.from_json(response)

def generate_rpc_error(
    code: int, message: Optional[str] = "", data: Optional[Any] = None
) -> Dict:
    """Creates a JSON RPC-style error conformingn to the specification.

    Args:
        code (int): Error code encountered (See tools.constants)

        message (Optional[str], optional): Error message, if any.
        Defaults to "".

        data (Optional[Any], optional): Any relevant data. Defaults to None.

    Returns:
        Dict: JSON RPC error as a Dict.
    """

    error = JSONRPCError(code=code, message=message, data=data)
    return json.loads(error.json)

def create_json_rpc_response(
    id: int,
    errors: Optional[JSONRPCError] = None,
    result: Optional[Any] = None,
    encode: bool = True
) -> JSONRPC20Response:
    """Creates a JSONRPC20Response object.

    Args:
        id (int): RPC ID

        errors (Optional[JSONRPCError], optional): Errors if encountered.
        Mutually exclusive with `result`.
        Defaults to None.

        result (Optional[Any], optional): Results if present.
        Mutually exclusive with `errors`
        Defaults to None.

    Returns:
        JSONRPC20Response: Response object contianing either results OR errors.
    """

    resp = (
        JSONRPC20Response(id=id, error=errors) if errors is not None
        else JSONRPC20Response(id=id, result=result)
    )

    return resp.json.encode("utf-8") if encode else resp

def create_json_rpc_request(
    method: str, args: List[Any], encode: bool = True
) -> JSONRPC20Request:
    """Creates a JSONRPC20Request object.

    Args:
        method (str): Method to call
        args (List[Any]): Arguments for the method.

    Returns:
        JSONRPC20Request: Request object.
    """

    req = JSONRPC20Request(method=method, params=args)
    return req.json.encode("utf-8") if encode else req

async def ping(host: str, port: int) -> bool:
    """Pings an address to determine if it is listening or not

    Args:
        host (str): Host address
        port (int): Host port

    Returns:
        bool: Node is alive or not
    """

    try:
        _, _ = await asyncio.open_connection(host=host, port=port)
        return True
    except (ConnectionResetError, ConnectionRefusedError):
        return False

async def send_rpc_request(
    host: str,
    port: int,
    method: Optional[str] = "", args: Optional[List[Any]] = [],
    buffer: int = cst.DEFAULT_BUFFER
) -> JSONRPC20Response:
    """Sends a JSON RPC request to an address and returns the response.

    Args:
        host (str): Host address to connect to.

        port (int): Host port to connect to.

        method (Optional[str], optional): RPC method to invoke. Defaults to "".

        args (Optional[List[Any]], optional): Arguments for the RPC method to
        invoke. Defaults to [].

        buffer (int, optional): Read/write buffer size.
        Defaults to cst.DEFAULT_BUFFER.

    Returns:
        JSONRPC20Response: RPC Response from the address
    """

    try:
        # Build request and open connection
        request = create_json_rpc_request(method, args)
        reader, writer = await asyncio.open_connection(
            host=host, port=port
        )

        # Send the request
        writer.write(request)
        await writer.drain()

        # Await the response
        response = await reader.read(buffer)
        response = extract_rpc_response(response)

        # Close writers
        writer.close()
        await writer.wait_closed()

        # Return the Repsonse
        return response

    except (ConnectionRefusedError, ConnectionResetError):
        # Unable to connect to host, return error instead
        return create_json_rpc_response(
            id=cst.JSONRPC_DEFAULT_ID,
            errors=generate_rpc_error(
                code=cst.JSONRPC_SERVER_ERROR,
                message="No connection to host can be made"
            )
        )
