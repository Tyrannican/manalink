import asyncio
import uuid
from protocols import DiscoveryProtocol


class DiscoveryPeer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._buf_size = 4096
        self.peer_id = f'DP_{uuid.uuid4().hex}'
        self.proto = DiscoveryProtocol(peer_id=self.peer_id)

    async def listen_for_peers(self):
        server = await asyncio.start_server(
            self.handle_incoming_connection, host=self.host, port=self.port
        )

        await server.serve_forever()

    async def handle_incoming_connection(self, reader, writer):
        req = await reader.read(self._buf_size)
        response = self.proto.parse_request(req)

        writer.write(response)
        writer.close()
        await writer.wait_closed()

    async def broadcast_peers(self):
        pass
