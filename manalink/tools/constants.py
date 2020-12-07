"""
.. module:: prototools.constants
    :platforms: Unix
    :synopsis: General constants to be used with protocols

.. moduleauthor:: Graham Keenan 2020

"""

# Protocol Ports
DEFAULT_PORT = 4000
DISCOVERY = 4050

# Core Protcol constants
BEACON_TIMER = 10
CONSTANT_DISCOVERY_BEACON_TIMER = 10
KEEPALIVE_TIMER = 2.5
DEFAULT_BUFFER = 4096

# JSON RPC constants
JSONRPC_DEFAULT_ID = 1
JSONRPC_PARSE_ERROR = -32700
JSONRPC_INVALID_REQUEST = -32600
JSONRPC_METHOD_NOT_FOUND = -32601
JSONRPC_INVALID_PARAMS = -32602
JSONRPC_INTERNAL_ERROR = -32603
JSONRPC_SERVER_ERROR = -32000
