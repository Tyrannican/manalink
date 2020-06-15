"""
.. module:: protocols.time
    :platforms: Unix
    :synopsis: Simple Time protocol to be mainly used for testing > 1 Protocol

.. moduleauthor:: Graham Keenan 2020

"""

# System imports
import time
from typing import List, Optional

# Protocol helpers
from .protocol import (
    CoreProtocol, NodeAddress, ProtoPort, ProtoResult, ProtoErrorType
)


class TimeProtocol(CoreProtocol):
    """Simple Time Protocol to inform nodes of the current time

    Args:
        nodes (List[str]): Known nodes

    Inherits:
        CoreProtocol: Pillar of all protocols
    """

    def __init__(
        self, nodes: List[str] = [], host_address: Optional[str] = '0.0.0.0'
    ):
        super().__init__(
            nodes=nodes, protocol_port=ProtoPort.TIME
        )

    async def broadcast(self):
        """Main broadcast loop to ask for the time
        """

        while True:
            await self.broadcaster(self.get_time)

    async def get_time(self, node: NodeAddress):
        """Asks a node for the time and logs it

        Args:
            node (NodeAddress): Address of the node
        """

        # Send request to node and get response
        response = await self.notify_node(node, 'current_time')

        # Split status, errors, and results
        status, results, errors = (
            response.status, response.results, response.errors
        )

        # Not Ok, log errors
        if not status:
            if errors.error_type != ProtoErrorType.CONNECTION:
                self.logger.warning(errors.message)
            return

        # Get the time and log it
        current_time = results[0]
        self.logger.info(f'Current time: {current_time}')

    def current_time(self) -> float:
        """Gets the current time

        Returns:
            float: Current time
        """

        return ProtoResult(
            results=[time.time()]
        )
