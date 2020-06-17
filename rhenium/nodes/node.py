"""
.. module:: nodes.node
    :platforms: Unix
    :synopsis: Core Node for developing own nodes. Comes with Discovery Protocol
                already loaded

.. moduleauthor:: Graham Keenan 2020

"""

# System imports
import asyncio
from typing import List

# Protocol imports
from ..protocols import DiscoveryProtocol, CoreProtocol

class CoreNode:
    """Class representing a Core Node
    Core Node has the Discovery Protocol already build in and is designed
    to be the central pillar for building other nodes upon

    Args:
        nodes (List[str]): List of initial or known nodes on startup
                            (For bootstrapping the discovery process)
    """

    def __init__(self, nodes: List[str] = []):
        # List to hold all protocols for the node
        self.protocols = []

        # Automatically register the Discovery Protocol
        self.register_protocols(DiscoveryProtocol(nodes=nodes))

    def register_protocols(self, *protocols: CoreProtocol):
        """Register any protocols

        Args:
            protocols (CoreProtocol): Protocol to register
        """

        for protocol in protocols:
            self.protocols.append(protocol)

    async def run(self):
        """Run loop for the node
        Executes the `run()` method on each Protocol registered
        """

        # Gather each protocol in a task
        tasks = [
            asyncio.create_task(proto.run())
            for proto in self.protocols
        ]

        # Run all tasks
        await asyncio.gather(*tasks)
