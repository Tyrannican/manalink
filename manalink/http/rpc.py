"""
.. module:: http.rpc
    :platforms: Unix
    :synopsis: Core of the HTTP protocol using JSON RPC 2.0 as communication
                standard

.. moduleauthor:: Graham Keenan 2020

"""

# System imports
import asyncio
from copy import copy
from typing import (
    List, Optional, Union, ByteString, Callable
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
    notify_node
)

from ..tools import constants as cst
from ..tools import make_logger, get_logger, colour_item

# Third-party imports
from jsonrpc.jsonrpc2 import JSONRPC20Response

class ManaLinkRPC:
    """HTTP communication protocol for ManaLink using JSON RPC 2.0 with built-in
    peer-to-peer discovery.

    This should for the core of any subsequent protocols built on top of
    ManaLink.

    Args:
        host (str): Host address
        port (Union[ManaRPCPort, int]): Communication port for the protocol
        nodes (Optional[List[str]]): List of initial nodes
        _read_buffer (Optional[str]): Internal buffer size. Defaults to 4096.
    """

    def __init__(
        self,
        host: str,
        port: Union[ManaRPCPort, int],
        nodes: Optional[List[str]] = [],
        _read_buffer: Optional[int] = cst.DEFAULT_BUFFER
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
            self.host, nodes=self.nodes, logger=get_logger(self.name)
        )

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

class ManaLinkRPCDiscovery:
    """Discovery Protocol responsible for tracking known nodes.

    Args:
        host (str): Address of the host

        nodes (Optional[List[str]]): List of nodes to make initial contact with
        Defaults to [].

    """

    def __init__(
        self,
        host: str,
        nodes: Optional[List[str]] = [],
        logger: Optional = None
    ):
        # Host address
        self.host = host

        # Port number
        self.port = ManaRPCPort.DISCOVERY.value

        # Initial contact nodes
        self.nodes = nodes

        # Copy of the initial nodes for continuous discovery
        self.initial_nodes = copy(nodes)

        # Logger
        self.logger = logger

        # Read buffer
        self.__buf = cst.DEFAULT_BUFFER

        # Node lock
        self._node_lock = asyncio.Lock()

    async def run(self):
        """Main run loop for discovery
        """

        tasks = [
            self._discovery_server(),  # Main Discovery server
            self._discovery_broadcast(),  # Beacon sender to other nodes
            self._continuous_discovery(),  # Continuously search for nodes
            self._keepalive()  # Check node alive status
        ]

        await asyncio.gather(*tasks)

    async def _discovery_server(self):
        """Server to process incoming node connections
        """

        server = await asyncio.start_server(
            self._discovery_handler,
            host=self.host, port=self.port
        )

        await server.serve_forever()

    async def _discovery_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """Called on every incoming connection.
        Gives the connecting node the current list of known nodes to the server

        Args:
            reader (asyncio.StreamReader): Buffered Reader
            writer (asyncio.StreamWriter): Buffered Writer
        """

        # Get information from the socket
        sock = writer.get_extra_info('socket')

        # Get own IP address as may not be known from 0.0.0.0
        self.host = sock.getsockname()[0]

        # Get IP of connecting node
        node = sock.getpeername()[0]

        # Ditch request, empty for Discovery, so long as it resolves
        _ = await reader.read(self.__buf)

        # Add node to known node list
        await self._update_nodes(node)
        self.logger.debug(f'Connection from: {node}')

        # Create JSON RPC 2.0 response, with nodes as result
        rpc_response = JSONRPC20Response(
            result=[self.nodes],
            id=cst.RPC_DEFAULT_ID
        )

        # Convert to HTTP response
        response = http_response_builder(
            host=self.host, port=self.port, data=rpc_response
        )

        # Send to node and close connection
        self.logger.debug(f'Sending response to: {node}')
        writer.write(response.encode())
        writer.close()
        await writer.wait_closed()

    async def _discovery_broadcast(self, beacon_timer: int = cst.BEACON_TIMER):
        """Broadcasts a request for nodes to all known nodes.

        Args:
            beacon_timer (int, optional): Time between broadcasts.
               Defaults to cst.BEACON_TIMER.

        """

        # Loop forever
        while True:
            # Create beacon request for each node
            tasks = [
                self._discovery_beacon(Address(host=node, port=self.port))
                for node in self.nodes
            ]

            self.logger.info(
                f'Looking for nodes:\t{colour_item("Total", "green")}\
={len(self.nodes)}')
            self.logger.debug(f'Active nodes: {self.nodes}')

            # Call the tasks and wait
            await asyncio.gather(*tasks)
            await asyncio.sleep(beacon_timer)

    async def _discovery_beacon(self, node: Address):
        """Sends a beacon to a node requesting their node list

        Args:
            node (Address): Node to connect to
        """

        # Send beacon to node and parse response
        response = notify_node(node, function='')
        results, errors = response.result, response.error

        # Errors were encountered, log and return
        if errors:
            self.logger.error(f'Error encountered: {response.error}')
            return

        # Extract the node list and update
        nodes = results[0]
        for node in nodes:
            await self._update_nodes(node)

    async def _continuous_discovery(
        self, beacon_timer: int = cst.CONSTANT_DISCOVERY_BEACON_TIMER
    ):
        """Continually search for previous known nodes if node list is empty.

        Args:
            beacon_timer (int, optional): Time between checks.
            Defaults to cst.CONSTANT_DISCOVERY_BEACON.

        """

        # Loop forever
        while True:
            # Nodes are present, just wait for next iteration
            if len(self.nodes) > 0:
                await asyncio.sleep(beacon_timer)
                continue

            # No nodes, update node list with previous known nodes
            for node in self.initial_nodes:
                await self._update_nodes(node)

            # Still no known nodes, current node may be dead until
            # new connection made
            if len(self.nodes) == 0:
                self.logger.warning('No nodes can be found, dead node!')

            # Wait for next iteration
            await asyncio.sleep(beacon_timer)

    async def _keepalive(self, beacon_timer: int = cst.KEEPALIVE_TIMER):
        """Constantly check if known nodes are still alive.
        Remove dead nodes.

        Args:
            beacon_timer (int, optional): Time between beacons.
            Defaults to cst.KEEPALIVE_TIMER.

        """

        # Loop forever
        while True:
            # Self is in known nodes, remove it
            if self.host in self.nodes:
                async with self._node_lock:
                    self.nodes.remove(self.host)

            # Iterate through known nodes
            for node in self.nodes:
                # Node is alive, skip to next node
                if await ping_node(node, port=self.port):
                    continue

                # Node is dead, remove it
                self.logger.debug(f'Removing inactive node: {node}')
                async with self._node_lock:
                    self.nodes.remove(node)

            # Wait for next iteration
            await asyncio.sleep(beacon_timer)

    async def _update_nodes(self, node: str):
        """Updates the node list with a new node

        Args:
            node (str): Node to update
        """

        # Node is self, just return
        if node == self.host:
            return

        # Node is not in known node list AND is alive, add it
        if node not in self.nodes and await ping_node(node, port=self.port):
            self.logger.debug(f'Registering new node: {node}')
            async with self._node_lock:
                self.nodes.append(node)

                # Make the list unique to protect against duplicates
                self.nodes = list(set(self.nodes))
