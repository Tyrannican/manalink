from typing import Tuple
from .protocol import BaseProtocol


class DiscoveryProtocol(BaseProtocol):
    def __init__(self, peer_id: str):
        self.peer_id = peer_id
        self.known_peers = []

    def register(self, peer_address: str, peer_port: int) -> Tuple[bool, str]:
        addr = (peer_address, peer_port)
        self.known_peers.append(addr)
        self.known_peers = list(set(self.known_peers))

        return True, ''

    def unregister(self, peer_address: str, peer_port: int) -> Tuple[bool, str]:
        addr = (peer_address, peer_port)
        self.known_peers = [
            peer for peer in self.known_peers if peer != addr
        ]

        return True, ''
