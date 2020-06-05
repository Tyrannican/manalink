import time
from termcolor import cprint
import asyncio
import random
import json
import sys


PROTO = ['request_time', 'request_random_number', 'request_incorrect']

DISCOVERY = [
    8000, 9000, 10000, 11000
]


class SimpleProtocol:
    def __init__(self):
        pass

    def request_time(self):
        return time.time()

    def request_random_number(self, low=0.1, high=50.):
        return random.uniform(low, high)


class Peer:
    def __init__(self, port: int = 9000, buffer: int = 4096):
        self.port = port
        self.known_peers = []
        self.__buf_size = buffer
        self.proto = SimpleProtocol()

    def add_peer(self, peer):
        self.known_peers.append(peer)
        self.known_peers = list(set(self.known_peers))

    def remove_peer(self, peer):
        self.known_peers = [p for p in self.known_peers if p != peer]

    async def handle_incoming_connection(self, reader, writer):
        # Get peer info (Address)
        info = writer.get_extra_info('socket')
        peer, p_port = info.getpeername()
        cprint(f'New connection: {peer} ({p_port})', 'green')

        # Get request string from connection
        req_str = await reader.read(self.__buf_size)
        if not req_str:
            return

        # Get the JSON response
        req = json.loads(req_str.decode())

        # Do something with it (Log for now)
        res = await self.parse_incoming_request(req)

        # If processed OK, write response OK else ERR
        res = json.dumps(res).encode()
        writer.write(res)
        await writer.drain()

        # Close writing and leave
        writer.close()
        await writer.wait_closed()

        cprint(f'Peer {peer} ({p_port}) disconnected', 'yellow')

    async def parse_incoming_request(self, req):
        parsed_request = {
            'request': req['func'],
            'result': None,
            'ok': False
        }

        if hasattr(self.proto, req['func']):
            func, args = getattr(self.proto, req['func']), req['args']
            res = func(*args)
            parsed_request['result'] = res
            parsed_request['ok'] = True

        return parsed_request

    async def connect_to_peer(self, address):
        try:
            reader, writer = await asyncio.open_connection(
                *address
            )

            self.add_peer(address)
            cprint(f'Peers: {self.known_peers}', 'cyan')

            proto_choice = random.choice(PROTO)
            req = {
                'func': proto_choice,
                'args': []
            }

            req = json.dumps(req).encode()

            writer.write(req)
            await writer.drain()

            resp = await reader.read(4096)
            resp = json.loads(resp.decode())

            color = 'green' if resp['ok'] else 'red'
            cprint(f'Peer Response: {resp}', color)

            writer.close()
            await writer.wait_closed()

        except (ConnectionRefusedError, ConnectionResetError):
            # Remove from list of known peers
            cprint(f'Host {address[0]} ({address[1]}) is down.', 'red')
            self.remove_peer(address)

    async def launch_server(self):
        server = await asyncio.start_server(
            self.handle_incoming_connection,
            host='0.0.0.0', port=self.port
        )

        cprint('Starting peer server...', 'green')
        await server.serve_forever()

    async def launch_peer_connections(self):
        blacklist = [self.port]
        while True:
            peer = random.choice(DISCOVERY)
            if peer in blacklist:
                continue

            addr = ('0.0.0.0', peer)
            await self.connect_to_peer(addr)
            await asyncio.sleep(random.uniform(0., 5.))

    async def run(self):
        await asyncio.gather(
            self.launch_server(), self.launch_peer_connections()
        )


async def main():
    port = int(sys.argv[1])
    p = Peer(port=port)
    await p.run()

asyncio.run(main())
