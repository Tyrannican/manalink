import sys
import asyncio

sys.path.append('..')

from protocols import TimeProtocol, DiscoveryProtocol

class TimeNode:
    def __init__(self, initial_nodes=[]):
        self.nodes = [node for node in initial_nodes]
        self.discovery = DiscoveryProtocol(nodes=self.nodes)
        self.time = TimeProtocol(nodes=self.nodes)

    async def run(self):

        await asyncio.gather(
            self.discovery.run(),
            self.time.run()
        )

async def main():
    await TimeNode(initial_nodes=['130.209.221.226']).run()

asyncio.run(main())
