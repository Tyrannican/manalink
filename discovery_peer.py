import uuid
import asyncio
import logging
from protocols import DiscoveryProtocol


def make_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(name)s::%(levelname)s [%(asctime)s] %(message)s'
    )

    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)

    sh.setFormatter(formatter)

    logger.addHandler(sh)

    return logger


class DiscoveryPeer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._buf_size = 4096
        self.peer_id = f'DiscoveryPeer_{uuid.uuid4().hex[:12]}'
        self.logger = make_logger(self.peer_id)
        self.proto = DiscoveryProtocol(peer_id=self.peer_id)

    async def listen_for_peers(self):
        self.logger.info('Started peer listener')
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

    async def broadcast(self, host, port):
        try:
            reader, writer = await asyncio.open_connection(
                host=host, port=port
            )

            request = self.proto.create_message(
                function='register_all',
                args=[self.proto.known_peers]
            )

            writer.write(request)
            await writer.drain()

            response = await reader.read(self._buf_size)
            response = self.proto.parse_response(response)

            if not response.result:
                print(response.errors)

            writer.close()
            await writer.wait_closed()

        except (ConnectionRefusedError, ConnectionResetError):
            self.proto.unregister((host, port))

    async def broadcast_peers(self):
        while True:
            tasks = [
                asyncio.create_task(self.broadcast(*peer))
                for peer in self.proto.known_peers
            ]

            self.logger.info('Broadcasting new peer addresses to known peers')
            self.logger.info(f'Current Peers: {self.proto.known_peers}\n')

            await asyncio.gather(*tasks)
            await asyncio.sleep(5)

    async def run(self):
        await asyncio.gather(
            self.listen_for_peers(),
            self.broadcast_peers()
        )


async def main():
    d = DiscoveryPeer('0.0.0.0', 9000)
    await d.run()


if __name__ == '__main__':
    asyncio.run(main())
