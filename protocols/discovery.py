"""
.. module:: protocols.discovery
    :platforms: Unix
    :synopsis: Discovery Protocol for finding new nodes

.. moduleauthor:: Graham Keenan 2020

"""

# System imports
import asyncio
from typing import List

# Protocol imports
from .protocol import (
    CoreProtocol, ProtoErrorType, ProtoResult, ProtoPort
)


class DiscoveryProtocol(CoreProtocol):
    """Discovery Protocol for discovering new nodes

    Args:
        node_sig (str): Unique signature for the node
        port (int): Port where the Protocol will be active on

    Inherits:
        CoreProtocol: Central pillar of each protocol
    """

    def __init__(
        self,
        nodes: List[str] = [],
        protocol_port: ProtoPort = ProtoPort.DISCOVERY
    ):
        super().__init__(nodes=nodes, protocol_port=protocol_port)

    async def run(self, initial_nodes: List[str] = []):
        """Runs all tasks assigned

        Args:
            initial_nodes (List[str], optional): List of initial nodes to
                                            attempt to contact. Defaults to [].
        """

        await asyncio.gather(
            self.initial_discovery(initial_nodes),
            self.request_for_nodes(),
            self.node_listener()
        )

    async def request_for_nodes(self):
        """Main broadcast loop.
        Communicates with nodes on a set interval.
        Each node response is processed and further action can be taken=
        in the `process_node_response()` implementation

        Args:
            nodes (List[PeerAddress]): List of known node addresses
            broadcast_time (int, optional): Interval between broadcasts.
                                            Defaults to 10.
        """

        while True:
            await self.broadcaster(self.ask_for_nodes)
            self.logger.info(
                f'Looking for nodes\t\u001b[32mTotal\u001b[0m={len(self.nodes)}'
            )

    async def ask_for_nodes(self, node: str):
        """Main function used by the `broadcast()` method.
        This is the entry point for the broadcast loop in which other node
        operations may be performed.

        Args:
            node (PeerAddress): Peer to contact
        """

        # Send request for nodes and await response
        response = await self.notify_node(node, 'known_nodes')
        status, errors = response.status, response.errors

        # Not Ok, log errors
        if not status:
            self.logger.warning(f'{errors.message}')

            # Error was a connectio nissue
            if errors.error_type == ProtoErrorType.CONNECTION:
                # Unregister the node
                self.deregister(node)

            return

        # Get the new nodes from the result
        new_nodes = response.results[0]

        # Iterate through all new nodes
        for new_node in new_nodes:
            # If they're alive, register them
            if await self.ping(new_node):
                self.register(new_node)

    async def initial_discovery(self, initial_nodes: List[str] = []):
        """Contacts a list of initial nodes to seed the discovery of newer nodes

        Args:
            initial_nodes (List[str], optional): Peers to attempt to contact.
                                                Defaults to [].
        """

        # Iterate through each node
        for node in initial_nodes:
            # Send request to register self with them and await response
            response = await self.notify_node(
                node, 'register', args=[self.host_address]
            )
            status, errors = response.status, response.errors

            # Not Ok, log errors
            if not status:
                self.logger.warning(errors.message)
                return

            # Register them as a node
            self.register(node)

    def known_nodes(self) -> ProtoResult:
        """Return all known nodes to the host

        Returns:
            ProtoResult: Known nodes
        """

        return ProtoResult(
            results=[self.nodes]
        )

    def register(self, node: str) -> ProtoResult:
        """Register the node as a known node

        Args:
            node (str): Peer to register

        Returns:
            ProtoResult: Basic Result
        """

        # Check the node is not already registered and not the host
        if node not in self.nodes and node != self.host_address:
            # Add node to list
            self.nodes.append(node)
            self.logger.info(
                f'Registered new node: {node} ({self.protocol_port})'
            )

        return ProtoResult()

    def deregister(self, node: str) -> ProtoResult:
        """Deregister a node from the list of known nodes

        Args:
            node (str): Peer to deregister

        Returns:
            ProtoResult: Basic Result
        """

        # Peer is present in known nodes list
        if node in self.nodes:
            # Remove from node list
            self.nodes = [p for p in self.nodes if p != node]
            self.logger.info(
                f'Unregistered node: {node} ({self.protocol_port}).'
            )

        return ProtoResult()
