"""
.. module:: protocols.time
    :platforms: Unix
    :synopsis: Simple Time protocol to be mainly used for testing > 1 Protocol

.. moduleauthor:: Graham Keenan 2020

"""

# System imports
import time
from typing import List

# Protocol helpers
from .protocol import (
    CoreProtocol, PeerAddress, ProtoPort, ProtoResult
)


class TimeProtocol(CoreProtocol):
    """Simple Time Protocol to inform peers of the current time

    Args:
        peers (List[str]): Known peers

    Inherits:
        CoreProtocol: Pillar of all protocols
    """

    def __init__(self, peers: List[str] = []):
        super().__init__(peers=peers, protocol_port=ProtoPort.TIME)

    async def broadcast(self):
        """Main broadcast loop to ask for the time
        """

        while True:
            await self.broadcaster(self.get_time)

    async def get_time(self, peer: PeerAddress):
        """Asks a peer for the time and logs it

        Args:
            peer (PeerAddress): Address of the peer
        """

        # Send request to peer and get response
        response = await self.notify_peer(peer, 'current_time')

        # Split status, errors, and results
        status, results, errors = (
            response.status, response.results, response.errors
        )

        # Not Ok, log errors
        if not status:
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
