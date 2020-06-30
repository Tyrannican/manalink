"""
.. module:: nodes.node
    :platforms: Unix
    :synopsis: Core Node for developing own nodes. Comes with Discovery Protocol
                already loaded

.. moduleauthor:: Graham Keenan 2020

"""

# System imports
import signal
import asyncio
from typing import List

# Protocol imports
from ..protocols import CoreProtocol

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

        # List of nodes
        self.nodes = nodes

        # Current async loop
        self._async_loop = None

    def _add_signal_handler(self):
        """Adds common signal interrupts to the currently set running loop
        """

        # Hangup, Terminate, Interrupt
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)

        # Add the signal to the loop
        for s in signals:
            self._async_loop.add_signal_handler(
                s, lambda s=s: asyncio.create_task(self._shutdown())
            )

    async def _shutdown(self):
        """Shuts down the node 'gracefully'

        Note:
            This causes a `asyncio.CancelledError` to propagate to the main
            running loop. Need to determine a better strategy to catch this
        """

        # Get all tasks except the current one
        tasks = [
            t for t in asyncio.all_tasks() if t is not asyncio.current_task()
        ]

        # Cancel each task
        for task in tasks:
            task.cancel()

        # Wait for each task to be cancelled
        await asyncio.gather(*tasks, return_exceptions=True)

        # Stop the current running loop
        self._async_loop.stop()

    def register_protocol(self, protocol: CoreProtocol):
        """Register any protocols

        Args:
            protocols (CoreProtocol): Protocol to register
        """

        # Set nodes for the protocol
        proto = protocol(nodes=self.nodes)

        # For every Protocol given, add to list
        self.protocols.append(proto)

    async def run(self):
        """Run loop for the node
        Executes the `run()` method on each Protocol registered
        """

        # Set the node's current running loop
        self._async_loop = asyncio.get_running_loop()

        # Add interrupt hanlder
        self._add_signal_handler()

        # Gather each protocol in a task
        tasks = [
            asyncio.create_task(proto.run())
            for proto in self.protocols
        ]

        # Run all tasks
        await asyncio.gather(*tasks)
