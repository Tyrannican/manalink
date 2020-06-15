import asyncio
import sys
sys.path.append('..')
from protocols import DiscoveryProtocol


class DiscoveryNode:

    def __init__(self):
        self.nodes = []
        self.discovery = DiscoveryProtocol(nodes=self.nodes)

    async def run(self):
        await asyncio.gather(
            self.discovery.run()
        )


async def main():
    await DiscoveryNode().run()

asyncio.run(main())
