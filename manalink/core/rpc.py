"""
.. module:: core.rpc
    :synopsis: Core Protocol for communicating with nodes using JSON RPC
    :platforms: Unix

.. moduleauthor:: Graham Keenan 2020

"""

# Package imports
from ..tools.logger import make_logger, get_logger
from .gemlinker import ManaGemLinker
from ..tools import constants as cst
from .prototools import (
    Address,
    generate_rpc_error,
    extract_rpc_request,
    create_json_rpc_response,
)

# System imports
import asyncio
from typing import Optional, List, ByteString, Callable

# External imports
from jsonrpc.jsonrpc2 import (
    JSONRPC20Response
)

class ManaGem:
    """Core of the P2P protocol.
    Exposes methods for facilitating communication between nodes using JSON RPC.
    Nodes can request certian methods of the inheriting protocol be ran and
    have the results sent back when ready.

    Args:
        host (str): Host IP address.
        Defaults to all interfaces "0.0.0.0".

        port: (int): Host port to listen on.
        Defaults to cst.DEFAULT_PORT.

        seed_nodes (Optional[List[str]]): Initial seed nodes to help find other
        nodes on the network.
        Required, else the node will sit isolated. Defaults to [].

        debug_mode (bool): Debug flag. Defaults to False.

        message_buffer (int): Size of the message buffer. Defaults to
        cst.DEFAULT_BUFFER
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = cst.DEFAULT_PORT,
        seed_nodes: Optional[List[str]] = [],
        debug_mode: bool = False,
        message_buffer: int = cst.DEFAULT_BUFFER
    ):
        # Current address and port
        self.address = Address(host=host, port=port)

        # Initial nodes to seed P2P
        self.gems = seed_nodes

        # Input buffer for receiving messages
        self.__buf = message_buffer

        # Initiate logger
        self.logger = make_logger(self.name, debug=debug_mode)

        # List of scheduled tasks to perform
        self.task_schedule = []

        # GemLinker Protocol (Discovery)
        self.gemlinker = ManaGemLinker(
            self.address.host,
            seed_gems=self.gems,
            logger=get_logger(self.name)
        )

        # Schedule these tasks at instantiation
        # Accept incoming connections and run discovery in background
        self.schedule(self._server(), self.gemlinker.link_gems())

    def __del__(self):
        """Cancel each task in the task schedule on object deletion
        """

        # Iterate through all tasks in the schedule
        for task in self.task_schedule:
            # Cancel the task
            try:
                task.cancel()

            # Ignore the fact that the rror is thrown when cancelled
            except asyncio.CancelledError:
                pass

    @property
    def name(self) -> str:
        """Get the name of the protocol

        Returns:
            str: Name of the protocol.
        """

        return self.__class__.__name__

    async def run(self):
        """Main run loop.
        Run for as long as there are incomplete scheduled tasks.
        """

        while len(self.task_schedule) > 0:
            self.task_schedule = [
                task for task in self.task_schedule if task.done() is False
            ]
            await asyncio.sleep(1)

    def schedule(self, *methods: Callable):
        """Creates an `asyncio.Task` for each supplied method.
        Each method is then ran in the background until either completion or
        program termination.

        Args:
            *methods (Callable, variable): Methods to schedule calls for. Calls
            happen immediately and run in the background.

        """

        # Create list of tasks for each method and add to the protocol's
        # task schedule.
        for method in methods:
            self.task_schedule.extend([
                asyncio.create_task(method, name=method.__name__)
            ])

    def cancel_task(self, *names: str):
        """Cancels tasks with the supplied names.

        Args:
            names (str, variable): Task names to cancel
        """

        for task in self.task_schedule:
            if task.name in names:
                try:
                    task.cancel()
                except asyncio.CancelledError:
                    pass

    async def _server(self):
        """Main server loop for receiving connections from other nodes.
        Accepts incoming connections and passes them to the server handler.
        """

        # Loop forever to accept incoming connections
        server = await asyncio.start_server(
            self._server_handler, self.address.host, self.address.port
        )

        self.logger.debug(
            f"Starting {self.name} protocol server for\
 {self.address.host}:{self.address.port}"
        )
        await server.serve_forever()

    async def _server_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """Handler for the server loop. Extracts incoming requests, executes
        them, and returns the result back to the sender node.

        Args:
            reader (asyncio.StreamReader): Stream reader object for
            incoming node.

            writer (asyncio.StreamWriter): Stream writer object for
            incoming node.
        """

        # Replaces the 0.0.0.0 with the host's actual IP
        sock = writer.get_extra_info("socket")
        self.address.host = sock.getsockname()[0]
        self.logger.debug(f"Received connection from {sock.getpeername()[0]}")

        # Read request and parse for a response
        request = await reader.read(self.__buf)
        response = await self.parse_request(request)

        # Return response back to sender
        writer.write(response.encode("utf-8"))
        await writer.drain()
        self.logger.debug(f"Sent response back to {sock.getpeername()[0]}")

        # Close down writer
        writer.close()
        await writer.wait_closed()

    async def parse_request(self, request: ByteString) -> JSONRPC20Response:
        """Parses requests from other nodes and executes their command with
        supplied arguments, if supported.

        Args:
            request (ByteString): Incoming request from node.

        Returns:
            JSONRPC20Response: Response containing results or errors from
            method RPC call.
        """

        # Extract the RPC call from the incoming data
        rpc = extract_rpc_request(request)

        # Something went wrong, assume malformed message
        if not rpc:
            return create_json_rpc_response(
                rpc.id,
                errors=generate_rpc_error(
                    code=cst.JSONRPC_SERVER_ERROR,
                    message="Malformed request."
                )
            )

        # Extract method name and parameters
        method, params = rpc.method, rpc.params
        self.logger.debug(f"Received RPC Method: {method} Params: {params}")

        # Method not supported, return error
        if not hasattr(self, method):
            return create_json_rpc_response(
                id,
                errors=generate_rpc_error(
                    code=cst.JSONRPC_METHOD_NOT_FOUND,
                    message=f"No such method: {method}."
                )
            )

        # TODO::Only allow accepted methods be called.

        # Call the method and wait on the result
        method_call = getattr(self, method)
        result = await method_call(*params)
        self.logger.debug(f"Processed request: {method}. Result: {result}")

        # Return results
        return create_json_rpc_response(rpc.id, result=result)


async def main():
    m = ManaGem()
    await m.finder.run()

if __name__ == "__main__":
    asyncio.run(main())
