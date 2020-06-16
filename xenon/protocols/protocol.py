"""
.. module:: protocols.protocol
    :platforms: Unix
    :synopsis: Core of the Protocols

.. moduleauthor:: Graham Keenan 2020

"""
# System imports
import asyncio
from typing import (
    Any, Dict, Optional, List, ByteString, Callable
)

# Prototool helpers
from ..prototools import (
    ProtoErrorType,
    ProtoMessage,
    NodeAddress,
    ProtoResult,
    ProtoError,
    ProtoPort,
    address_in_use,
    make_logger
)


class CoreProtocol:
    """Core of the Protocol on which each subsequent protocol will build upon

    Args:
        node_address (NodeAddress): Address of the Protocol's node listener
        buffer (int): Default size of the incoming buffer
    """

    def __init__(
        self,
        nodes: List[str] = [],
        buffer: int = 4096,
        protocol_port: ProtoPort = ProtoPort.UNUSED
    ):
        self.host_address = '0.0.0.0'
        self.protocol_port = protocol_port.value
        self.nodes = nodes
        self._buf = buffer
        self._pulse_timer = 10
        self._node_lock = asyncio.Lock()
        self.logger = make_logger(self.name)

        self.logger.info(
            f'Initialising {self.name} for {self.host_address}'
        )

    @property
    def name(self) -> str:
        """Name of the protocol

        Returns:
            str: Protocol name
        """

        return self.__class__.__name__

    async def run(self):
        """Basic run loop, can be overridden
        """

        # Address for this protocol is already running, clash with another
        # protocol or another service on the machine
        if address_in_use(self.host_address, self.protocol_port):
            self.logger.warning(
                f'Address {self.host_address}:{self.protocol_port} is already\
 in use. {self.name} may already be running on this machine.'
            )
            return

        # Execute loop
        await asyncio.gather(
            self.node_listener(),
            self.broadcast(),
            self.pulse_nodes()
        )

    async def broadcast(self, broadcast_timer: int = 10):
        """Broadcast loop,
        should call the `broadcaster` in an infinite loop with extra additions
        if necessary

        Args:
            broadcast_timer (int): Time between node broadcasts

        Raises:
            NotImplementedError: Not implemented
        """

        raise NotImplementedError(
            'Method requires implementation in child protocols!'
        )

    async def broadcaster(self, callback: Callable, broadcast_timer: int = 10):
        """Broadcaster which calls the callback function for each node and waits
        X seconds

        Args:
            callback (Callable): Callback function to execute
            broadcast_timer (int, optional): How long to wait before yielding.
                                            Defaults to 10.
        """

        # Create a list of tasks calling the callback for each node
        tasks = [
            asyncio.create_task(
                callback(
                    NodeAddress(host=node, port=self.protocol_port)
                )
            )
            for node in self.nodes
        ]

        # Execute the tasks and wait
        await asyncio.gather(*tasks)
        await asyncio.sleep(broadcast_timer)

    async def notify_node(
        self,
        node_address: NodeAddress,
        function: str,
        args: List[Any] = []
    ) -> ProtoResult:
        """Creates and sends a ProtoMessage to a node and awaits a response
        from them.

        Args:
            node_address (NodeAddress): Address of the node
            function (str): Protocol function to call
            args (List[Any]): Protocol function arguments

        Returns:
            ProtoResults: Results form the Peer Response
        """

        # Build the message
        request = self.create_message(
            function=function,
            args=args
        )

        # Get response
        response = await self.open_node_connection(
            node_address=node_address, request=request
        )

        # Debug logger
        self.logger.debug(f'Response Protocol: {response.protocol}')
        self.logger.debug(f'Response Timestamp: {response.timestamp}')
        self.logger.debug(f'Response Function: {response.function}')
        self.logger.debug(f'Response Args: {response.args}')
        self.logger.debug(f'Response results: {response.results}')

        return response.results

    async def open_node_connection(
        self, node_address: NodeAddress, request: ByteString
    ) -> ProtoMessage:
        """Attempts to open a connection with a node to send requests and
        receive responses

        Args:
            node_address (NodeAddress): Peer to connect to
            request (ByteString): Request as an encoded string

        Returns:
            ProtoMessage: Response form the node
        """

        # Attempt to open the connection
        try:
            if isinstance(node_address, str):
                node_address = NodeAddress(
                    host=node_address, port=self.protocol_port
                )

            # Open connection
            reader, writer = await asyncio.open_connection(
                host=node_address.host, port=node_address.port
            )

            # Update host address on outgoing connections
            sock = writer.get_extra_info('socket')
            self.host_address = sock.getsockname()[0]

            # Send request
            writer.write(request)
            await writer.drain()

            # Wait for a response and process
            response = await reader.read(self._buf)
            response = self.parse_response(response)

            # Close writer and return response
            writer.close()
            await writer.wait_closed()

            return response

        # Peer is down
        except (ConnectionRefusedError, ConnectionResetError):
            # Return basic response
            result = ProtoResult.error_result(
                err_msg=f'{node_address.host} ({node_address.port}) is down.',
                err_type=ProtoErrorType.CONNECTION
            )

            return ProtoMessage(
                protocol=self.name,
                results=result
            )

    async def pulse_nodes(self):
        """Pulses each node, determining if it is still present
        If not, remove from peer list
        """

        # Loop forever
        while True:
            # If the node response to ping, keep in list
            for node in self.nodes:
                if (
                    not await self.ping(node)
                    and self.protocol_port == ProtoPort.DISCOVERY.value
                ):
                    async with self._node_lock:
                        self.nodes.remove(node)
                    break

            # Wait between broadcasts
            await asyncio.sleep(self._pulse_timer)

    async def ping(self, node: str) -> bool:
        """Ping an address and check for a response

        Args:
            node (str): Peer to ping

        Returns:
            bool: If a connection is established
        """

        try:
            # Attempt connection
            await asyncio.open_connection(
                host=node, port=self.protocol_port
            )

            # Connection successful
            return True

        except (ConnectionRefusedError, ConnectionResetError):
            # Peer is offline, unsuccessful
            return False

    async def node_listener(self):
        """Peer Listener loop.
        Listens for nodes and processes their Protocol requests
        """

        server = await asyncio.start_server(
            self._listener_handler,
            host=self.host_address, port=self.protocol_port
        )

        await server.serve_forever()

    async def _listener_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """Handles incoming requests by performing the request and constructing
        a response based on the response

        Args:
            reader (asyncio.StreamReader): Read from the incoming node
            writer (asyncio.StreamWriter): Write to the node
        """

        # Update node list with newly conencted node
        # Update host_address on incoming connections
        sock = writer.get_extra_info('socket')
        self.host_address = sock.getsockname()[0]
        await self._update_nodes(sock.getpeername()[0])

        # Get the request and process for a response
        request = await reader.read(self._buf)
        response = self.parse_request(request)

        # Send response
        writer.write(response)
        await writer.drain()

        # Close writer
        writer.close()
        await writer.wait_closed()

    async def _update_nodes(self, host: str):
        """Update the node list with a new connection if not present

        Args:
            host (str): Incoming node connection
        """

        # Not is not present and isn't the server
        if host not in self.nodes and host != self.host_address:
            async with self._node_lock:
                self.nodes.append(host)

    def create_message(
        self,
        function: Optional[str] = None,
        args: List[Any] = [],
        results: Optional[ProtoResult] = None,
    ) -> ByteString:
        """Creates a ProtoMessage and returns it as an encoded string

        Args:
            function (Optional[str], optional): Name of the protocol function
                                                to call. Defaults to None.

            args (List[Any], optional): List of arguments for the Protocol
                                        function to call. Defaults to [].

            result (bool, optional): Result of the function call.
                                    Defaults to False.

            errors (Optional[Dict], optional): Any errors that were encountered.
                                                Defaults to None.

        Returns:
            ByteString: ProtoMessage as an encoded string
        """

        return ProtoMessage(
            protocol=self.name,
            function=function,
            args=args,
            results=results,
        ).as_encoded_string

    def _valid_msg(self, msg: ProtoMessage) -> bool:
        """Checks if a message is valid by comparing fields with a basic message

        Args:
            msg (ProtoMessage): Message to check

        Returns:
            bool: Message contains valid fields
        """

        # Create a dummy message and convert it to Dict
        dummy = ProtoMessage().as_dict
        msg_dict = msg.as_dict

        # Check if all fields in the msg are valid i.e in the dummy message
        return dummy.keys() == msg_dict.keys()

    def _check_msg(self, msg: Dict, original: Any) -> Dict:
        """Checks a message for inconsistencies and returns an error status

        Args:
            msg (Dict): Message to check
            original (Any): Original ByteString for the message

        Returns:
            Dict: Error struct with errors listed
        """

        # Malformed request -- Not parsable
        if msg is None:
            return ProtoError(
                message=f'Malformed message: {original}',
                error_type=ProtoErrorType.MESSAGING
            )

        # Message is missing fields
        if not self._valid_msg(msg):
            return ProtoError(
                message=f'Invalid message fields: {original}',
                error_type=ProtoErrorType.MESSAGING
            )

        # No errors, everything is fine
        return ProtoError(
            message='', error_type=ProtoErrorType.NONE
        )

    def parse_request(self, req: Any) -> ByteString:
        """Parses an incoming request and processes it to call the
        requested function.

        Args:
            req (Any): Incoming request

        Returns:
            ByteString: Response as an encoded string
        """

        # Parse the request to get the dictionary
        parsed_req = ProtoMessage.from_json(req)

        # Get error struct and corresponding error code
        errors = self._check_msg(parsed_req, req)

        # Error code is not NONE error, return weith errors
        if errors.error_type != ProtoErrorType.NONE:
            return self.create_message(
                results=ProtoResult(errors=errors)
            )

        # Get function name and function arguments
        func_name, args = parsed_req.function, parsed_req.args

        # No function by the given name, return response
        if not hasattr(self, func_name):
            result = ProtoResult.error_result(
                err_msg=f'No such function: {func_name}',
                err_type=ProtoErrorType.EXECUTION
            )

            return self.create_message(
                function=func_name,
                args=args,
                results=result
            )

        # Call the func to get result and errors if present
        func = getattr(self, func_name)
        func_result = func(*args)

        # Return response
        return self.create_message(
            function=func_name,
            results=func_result,
        )

    def parse_response(self, resp: Any) -> ProtoMessage:
        """Parses a response for errors

        Args:
            resp (Any): Response ByteString

        Returns:
            ProtoMessage: Response
        """

        # Create ProtoMessage from response
        parsed_resp = ProtoMessage.from_json(resp)

        # Get errors and check
        errors = self._check_msg(parsed_resp, resp)

        # Errors found, return Message with errors
        if errors.error_type != ProtoErrorType.NONE:
            return ProtoMessage(
                results=ProtoResult(errors=errors)
            )

        # Return the response
        return parsed_resp
