"""
.. module:: protocols.discovery
    :platforms: Unix
    :synopsis: Discovery Protocol for finding new nodes

.. moduleauthor:: Graham Keenan 2020

"""

# System imports
import asyncio
from typing import List, Optional

# Protocol imports
from .protocol import CoreProtocol
from ..prototools import (
    ProtoResult,
    ProtoPort,
    ProtoErrorType,
    address_in_use,
    colour_item,
    DISCOVERY_NODES
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
        protocol_port: ProtoPort = ProtoPort.DISCOVERY,
        continuous_discovery: Optional[bool] = True
    ):
        super().__init__(
            nodes=nodes, protocol_port=protocol_port
        )

        # Continually search for new peers from whitelist if none are listed
        self.continuous_discovery = continuous_discovery

    async def run(self):
        # Address for this protocol is already running, clash with another
        # protocol or another service on the machine
        if address_in_use(self.host_address, self.protocol_port):
            self.logger.warning(
                f'Address {self.host_address}:{self.protocol_port} is already\
 in use. {self.name} may already be running on this machine.'
            )
            return

        # Kee ptrack of async Tasks to execute
        tasks = [
            self.node_listener(),
            self.broadcast(),
            self.pulse_nodes()
        ]

        # Execute loop
        if self.continuous_discovery:
            tasks.append(self.node_searcher())

            await asyncio.gather(
                *tasks
            )
        else:
            await asyncio.gather(
                *tasks
            )

    async def broadcast(self):
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
            await self.broadcaster(self.ask_for_nodes, broadcast_timer=5)

            # Non-discovery nodes (Actual nodes on the network)
            nd_nodes = [
                node for node in self.nodes if node not in DISCOVERY_NODES
            ]

            self.logger.info(
                f'Looking for nodes\t{colour_item("Total", color="green")}\
={len(nd_nodes)}')

    async def ask_for_nodes(self, node: str):
        """Main function used by the `broadcast()` method.
        This is the entry point for the broadcast loop in which other node
        operations may be performed.

        Args:
            node (PeerAddress): Peer to contact
        """

        # Send request for nodes and await response
        response = await self.notify_node(node, 'known_nodes')
        status, results, errors = (
            response.status, response.results, response.errors
        )

        # Not Ok, log errors
        if not status:
            if errors.error_type != ProtoErrorType.CONNECTION:
                self.logger.warning(f'{errors.message}')
            return

        # Get the new nodes from the result
        new_nodes = results[0]

        # Iterate through all new nodes
        for new_node in new_nodes:
            await self._update_nodes(new_node)

    async def node_searcher(self, search_time: int = 10):
        """Continually searches for known discovery nodes when there are no
        other nodes connected

        Args:
            search_time (int): Time between each search
        """

        while True:
            # We have actual nodes present, just continue on
            if len(self.nodes) > 0:
                await asyncio.sleep(search_time)
                continue

            # No nodes, check if discovery nodes are open
            for node in DISCOVERY_NODES:
                await self._update_nodes(node)

            # Short pause
            await asyncio.sleep(search_time)

    def known_nodes(self) -> ProtoResult:
        """Return all known nodes to the host

        Returns:
            ProtoResult: Known nodes
        """

        return ProtoResult(
            results=[self.nodes]
        )
