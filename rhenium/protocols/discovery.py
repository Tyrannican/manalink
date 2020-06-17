"""
.. module:: protocols.discovery
    :platforms: Unix
    :synopsis: Discovery Protocol for finding new nodes

.. moduleauthor:: Graham Keenan 2020

"""

# System imports
from typing import List

# Protocol imports
from .protocol import CoreProtocol
from ..prototools import (
    ProtoResult, ProtoPort, ProtoErrorType
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
        super().__init__(
            nodes=nodes, protocol_port=protocol_port
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

    def known_nodes(self) -> ProtoResult:
        """Return all known nodes to the host

        Returns:
            ProtoResult: Known nodes
        """

        return ProtoResult(
            results=[self.nodes]
        )
