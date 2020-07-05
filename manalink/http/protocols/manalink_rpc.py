"""
.. module:: http.protocols.manalink_rpc
    :platforms: Unix
    :synopsis: Core of the HTTP protocol using JSON RPC 2.0 as communication
                standard

.. moduleauthor:: Graham Keenan 2020

"""

# System imports
import asyncio
import requests
from copy import copy
from logging import Logger
from typing import (
    List, Dict, Optional, Any, Union, ByteString, Callable
)

# Project imports
from .prototools import (
    ManaRPCError,
    ManaRPCPort,
    Address,
    http_response_builder,
    extract_rpc_call,
    generate_rpc_error,
    ping_node,
    RPC_DEFAULT_ID
)

from ...tools import make_logger, colour_item

# Third-party imports
from jsonrpc.jsonrpc2 import JSONRPC20Request, JSONRPC20Response

class ManaLinkRPCDiscovery:
    """Discovery Protocol responsible for tracking known nodes.

    Args:
        host (str): Address of the host

        port (Optional[Union[ManaRPCPort, int]]): Port to listen on.
        Defaults to ManaRPCPort.DISCOVERY

        nodes (Optional[List[str]]): List of nodes to make initial contact with
        Defaults to [].

    """

    def __init__(
        self,
        host: str,
        port: Optional[Union[ManaRPCPort, int]] = ManaRPCPort.DISCOVERY,
        nodes: Optional[List[str]] = [],
        logger: Optional[Logger] = None,
        _read_buffer: Optional[int] = 4096
    ):
        # Host address
        self.host = host

        # Port number
        self.port = port.value

        # Initial contact nodes
        self.nodes = nodes

        # Primitive lock for updating nodes (Legacy?)
        self._node_lock = asyncio.Lock()

        # Copy of the initial nodes for continuos discovery
        self.initial_nodes = copy(nodes)

        # Logger
        self.logger = logger

        # Read buffer
        self.__buf = _read_buffer

    def register_methods(self, methods: Dict[str, Callable]):
        """Registers external methods to this class

        Args:
            methods (Dict[str, Callable]): Key-value map of method name and
            method callable.

        """

        # Iterate through each method in the dict
        for method_name, method_call in methods.items():
            # Register method to this class
            setattr(self, method_name, method_call)

    async def run(self):
        """Main execution loop for the protocol
        """

        # Get all tasks to execute
        tasks = [
            self._discovery_server(),
            self._discovery_broadcast(),
            self._constant_discovery(),
            self._keepalive(),
        ]

        # Run them and await finish
        await asyncio.gather(*tasks)

    async def _discovery_server(self):
        """Launches the discovery server on the discovery port and listens
        for incoming connections.
        """

        server = await asyncio.start_server(
            self._discovery_handler,
            host=self.host, port=ManaRPCPort.DISCOVERY.value
        )

        await server.serve_forever()

    async def _discovery_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """Callback function for the server.
        On incoming connection, server calls this method to parse the request,
        build the response, and send back the result.

        Args:
            reader (asyncio.StreamReader): Buffer Reader
            writer (asyncio.StreamWriter): Buffer Writer
        """

        # Read in request - Nothing for discovery
        _ = await reader.read(self.__buf)
        sock = writer.get_extra_info('socket')
        self.host = sock.getsockname()[0]

        # Build response which is the current list of nodes for this protocol
        response = JSONRPC20Response(result=[self.nodes], id=RPC_DEFAULT_ID)

        # Convert resposne to HTTP response
        response = http_response_builder(
            host=self.host, port=ManaRPCPort.DISCOVERY.value, data=response
        )

        # Send response and close connection
        writer.write(response.encode())
        writer.close()
        await writer.wait_closed()

    async def _discovery_broadcast(self):
        """Continuously ask other nodes for their list of nodes every X seconds.
        """

        # Loop forever calling the connection method
        while True:
            await self.broadcaster(self._discovery_connection, port=self.port)

            # Log total number of nodes
            self.logger.info(f'Looking for nodes\t\
 {colour_item("Total", "green")}={len(self.nodes)}')

    async def _discovery_connection(self, node: Address):
        """Connect to the address and request their node list.

        Args:
            node (Address): Node to connect to
        """

        # Send request and await result
        response = await self.notify_node(node, function='')

        if response.error:
            self.logger.error(response.error['message'])
            return

        result = response.result

        # Extract nodes and update list
        nodes = result[0]
        for node in nodes:
            await self._update_nodes(node)

    async def _constant_discovery(self, search_time: Optional[int] = 10):
        """
        Continuously search for known nodes when the current node list is empty.
        The intial nodes on startup should be always-connected meaning that if
        the node lost it's current node list, it will always have somewher to
        ping back to.

        Args:
            search_time (Optional[int], optional): Time between beacons.
            Defaults to 10.
        """

        # Loop forever
        while True:
            # We have nodes, jsut pause and loop
            if len(self.nodes) > 0:
                await asyncio.sleep(search_time)
                continue

            # No nodes, set out initial nodes to the current nodes
            for node in self.initial_nodes:
                await self._update_nodes(node)

            # Pause for a while before next beacon
            await asyncio.sleep(search_time)

    async def _keepalive(self):
        """Continuously ping known nodes and remove them if there is no response
        """

        # Loop forever
        while True:
            # Iterate through nodes
            for node in self.nodes:
                # No response, remove from list
                if not await ping_node(node, port=ManaRPCPort.DISCOVERY.value):
                    async with self._node_lock:
                        self.nodes.remove(node)
                    break

            # Time between beacons
            await asyncio.sleep(10)

    async def _update_nodes(self, node: str):
        """Update the current nodes with the given node

        Args:
            node (str): Node address
        """

        # Node is alive, check address
        if await ping_node(node, port=ManaRPCPort.DISCOVERY.value):
            # Address is not the current host,update list
            if node not in self.nodes and node != self.host:
                async with self._node_lock:
                    self.nodes.append(node)

class ManaLinkRPC:
    """HTTP communication protocol for ManaLink using JSON RPC 2.0 with built-in
    peer-to-peer discovery.

    This should for the core of any subsequent protocols built on top of
    ManaLink.

    Args:
        host (str): Host address
        port (Union[ManaRPCPort, int]): Communication port for the protocol
        nodes (Optional[List[str]]): List of initial nodes
        _read_buffer (Optional[str]): Internal buffer size
    """

    def __init__(
        self,
        host: str,
        port: Union[ManaRPCPort, int],
        nodes: Optional[List[str]] = [],
        _read_buffer: Optional[int] = 4096
    ):
        # Current address
        self.host = host

        # Convert port to number if it's a ManaRPCPort
        self.protocol_port = (
            port.value if isinstance(port, ManaRPCPort) else port
        )

        # Initial nodes
        self.nodes = nodes

        # Buffer size for reading from other nodes
        self.__buf = _read_buffer

        # Create the logger
        self.logger = make_logger(self.name)

        # Discovery protocol
        self.discovery = ManaLinkRPCDiscovery(
            self.host, nodes=self.nodes, logger=self.logger
        )

        # Register the broadcaster and notify methods for it
        self.discovery.register_methods({
            'notify_node': self.notify_node,
            'broadcaster': self.broadcaster
        })

        self.logger.info(
            f'Launching {self.name} on {self.host}:{self.protocol_port}'
        )

    @property
    def name(self) -> str:
        """Get the name of the protocol

        Returns:
            str: Protocol name
        """

        return self.__class__.__name__

    async def run(self, *callbacks: Callable):
        """Main exection or the protocol.
        Gathers up all methods to be ran and runs them asyncronously

        Args:
            *callbacks (Callable)
        """

        # Initial task is always the discovery protocol
        tasks = [
            self.discovery.run()
        ]

        # Add on any extra callbacks to run
        tasks.extend([cb for cb in callbacks])

        # Await them finishing
        await asyncio.gather(*tasks)

    async def broadcaster(
        self,
        callback: Callable,
        port: Optional[int] = None,
        broadcast_timer: Optional[int] = 10
    ):
        """Creates a callback for every known node and executes it.

        Args:
            callback (Callable): Method to call

            port (Optional[int], optional): Port to communicate over.
            Defaults to None.

            broadcast_timer (Optional[int], optional): Time between each beacon.
            Defaults to 10.
        """

        # If the port is not set, default it to the Protocol port
        port = port if port is not None else self.protocol_port

        # Create a callback for each known node
        tasks = [
            asyncio.create_task(
                callback(
                    Address(host=node, port=port)
                )
            )
            for node in self.nodes
        ]

        # Execute the callbacks and wait between next beacon
        await asyncio.gather(*tasks)
        await asyncio.sleep(broadcast_timer)

    async def server(self):
        """Launches the main server for the protocol and handles incoming
        connections.
        """

        server = await asyncio.start_server(
            self._handler,
            host=self.host, port=self.protocol_port
        )

        await server.serve_forever()

    async def _handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """Handler function for the main server.
        Each incoming connection triggers this method on the server.
        Processes requests, builds response, and returns to node.

        Args:
            reader (asyncio.StreamReader): Buffered Reader
            writer (asyncio.StreamWriter): Buffered Writer
        """

        sock = writer.get_extra_info('socket')
        self.host = sock.getsockname()[0]

        # Receive the request from the client
        request = await reader.read(self.__buf)

        # Process request and build response
        response = self.parse_request(request)
        response = http_response_builder(
            host=self.host, port=self.protocol_port, data=response
        )

        # Send response and close the conenction
        writer.write(response.encode())
        writer.close()
        await writer.wait_closed()

    async def notify_node(
        self, node: Address, function: str, args: List[Any] = []
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
            return self.parse_response(response)

        # Cannot connect to the node
        except requests.ConnectionError:
            # Return an error Response
            return JSONRPC20Response(
                error=generate_rpc_error(
                    code=ManaRPCError.CONNECTION.value,
                    message=f'No connection to {node.host}:{node.port}'
                )
            )

    def parse_request(self, request: ByteString) -> JSONRPC20Response:
        """Parses the incoming request, calls the requested function/method
        and returns the result/error depending on circumstance.

        Args:
            request (ByteString): Incoming request

        Returns:
            JSONRPC20Response: Server response to client
        """

        # Extract the RPC information
        rpc = extract_rpc_call(request)

        # Get the method and parameters to execute
        method = rpc.method
        params = rpc.params

        # method not supported by the protocol, return error Response
        if not hasattr(self, method):
            return JSONRPC20Response(
                error=generate_rpc_error(
                    code=ManaRPCError.NO_METHOD.value,
                    message=f'No such method: {method}'
                ),
                id=rpc.id
            )

        # Call the method and obtain the result
        method_call = getattr(self, method)
        result = method_call(*params)

        # Return result to client
        return JSONRPC20Response(
            result=result,
            id=rpc.id
        )

    def parse_response(self, response: requests.Response) -> JSONRPC20Response:
        """Load the response from returned JSON text

        Args:
            response (requests.Response): Response object

        Returns:
            JSONRPC20Response: RPC Response information
        """

        return JSONRPC20Response.from_json(response.text)
