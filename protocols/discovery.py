from typing import Tuple, List
from .protocol import CoreProtocol


class DiscoveryProtocol(CoreProtocol):
    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        self.known_peers = []

    def register(self, addr: List) -> Tuple[bool, str]:
        self.known_peers.append(tuple(addr))
        self.known_peers = list(set(self.known_peers))

        return True, ''

    def register_all(
        self, addresses: List[Tuple[str, int]]
    ) -> Tuple[bool, str]:
        for address in addresses:
            self.register(address)

        return True, ''

    def unregister(self, addr: Tuple[str, int]) -> Tuple[bool, str]:
        self.known_peers = [
            peer for peer in self.known_peers if peer != addr
        ]

        return True, ''

    def unregister_all(self):
        self.known_peers = []
        return True, ''
