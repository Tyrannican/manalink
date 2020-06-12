"""
.. module:: protocols.discovery
    :platforms: Unix
    :synopsis: Discovery Protocol for finding new peers

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
    """Discovery Protocol for discovering new peers

    Args:
        peer_sig (str): Unique signature for the peer
        port (int): Port where the Protocol will be active on

    Inherits:
        CoreProtocol: Central pillar of each protocol
    """

    def __init__(
        self,
        peers: List[str] = [],
        protocol_port: ProtoPort = ProtoPort.DISCOVERY
    ):
        super().__init__(peers=peers, protocol_port=protocol_port)

    async def run(self, initial_peers: List[str] = []):
        """Runs all tasks assigned

        Args:
            initial_peers (List[str], optional): List of initial peers to
                                            attempt to contact. Defaults to [].
        """

        await asyncio.gather(
            self.initial_discovery(initial_peers),
            self.request_for_peers(),
            self.peer_listener()
        )

    async def request_for_peers(self):
        """Main broadcast loop.
        Communicates with peers on a set interval.
        Each peer response is processed and further action can be taken=
        in the `process_peer_response()` implementation

        Args:
            peers (List[PeerAddress]): List of known peer addresses
            broadcast_time (int, optional): Interval between broadcasts.
                                            Defaults to 10.
        """

        while True:
            await self.broadcaster(self.ask_for_peers)
            self.logger.info(
                f'Looking for peers\t\u001b[32mTotal\u001b[0m={len(self.peers)}'
            )

    async def ask_for_peers(self, peer: str):
        """Main function used by the `broadcast()` method.
        This is the entry point for the broadcast loop in which other peer
        operations may be performed.

        Args:
            peer (PeerAddress): Peer to contact
        """

        # Send request for peers and await response
        response = await self.notify_peer(peer, 'known_peers')
        status, errors = response.status, response.errors

        # Not Ok, log errors
        if not status:
            self.logger.warning(f'{errors.message}')

            # Error was a connectio nissue
            if errors.error_type == ProtoErrorType.CONNECTION:
                # Unregister the peer
                self.deregister(peer)

            return

        # Get the new peers from the result
        new_peers = response.results[0]

        # Iterate through all new peers
        for new_peer in new_peers:
            # If they're alive, register them
            if await self.ping(new_peer):
                self.register(new_peer)

    async def initial_discovery(self, initial_peers: List[str] = []):
        """Contacts a list of initial peers to seed the discovery of newer peers

        Args:
            initial_peers (List[str], optional): Peers to attempt to contact.
                                                Defaults to [].
        """

        # Iterate through each peer
        for peer in initial_peers:
            # Send request to register self with them and await response
            response = await self.notify_peer(
                peer, 'register', args=[self.host_address]
            )
            status, errors = response.status, response.errors

            # Not Ok, log errors
            if not status:
                self.logger.warning(errors.message)
                return

            # Register them as a peer
            self.register(peer)

    def known_peers(self) -> ProtoResult:
        """Return all known peers to the host

        Returns:
            ProtoResult: Known peers
        """

        return ProtoResult(
            results=[self.peers]
        )

    def register(self, peer: str) -> ProtoResult:
        """Register the peer as a known peer

        Args:
            peer (str): Peer to register

        Returns:
            ProtoResult: Basic Result
        """

        # Check the peer is not already registered and not the host
        if peer not in self.peers and peer != self.host_address:
            # Add peer to list
            self.peers.append(peer)
            self.logger.info(
                f'Registered new peer: {peer} ({self.protocol_port})'
            )

        return ProtoResult()

    def deregister(self, peer: str) -> ProtoResult:
        """Deregister a peer from the list of known peers

        Args:
            peer (str): Peer to deregister

        Returns:
            ProtoResult: Basic Result
        """

        # Peer is present in known peers list
        if peer in self.peers:
            # Remove from peer list
            self.peers = [p for p in self.peers if p != peer]
            self.logger.info(
                f'Unregistered peer: {peer} ({self.protocol_port}).'
            )

        return ProtoResult()
