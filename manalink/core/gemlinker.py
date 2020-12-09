"""
.. module:: core.gemlinker
    :synopsis: Protocol for linking nodes (gems) together through continuous
    updates of who is on the network. Similar to a discovery protocol.

    :platforms: Unix

.. moduleauthor:: Graham Keenan 2020

"""
from .prototools import (
    Address,
    send_rpc_request,
    create_json_rpc_response,
    ping
)

from ..tools import constants as cst

import asyncio
from copy import copy
from typing import Optional, List

class ManaGemLinker:
    """Main discovery protocol for the core protocol above. Each protocol that
    inherits from the core `ManaLink` protocol has this built into it. This
    protocol will continuously listen for new incoming nodes on the network and
    update itself and others with what nodes are currently connected.

    Args:
        host (str): Host address.

        seed_gems (Optional[List[str]]): List of initial nodes to seed
        initial discovery. Defautls to [].

        logger (Optional[logging.Logger]): Logger for the system. Defaults
        to None.
    """

    def __init__(
        self,
        host: str,
        seed_gems: Optional[List[str]] = [],
        logger=None
    ):
        # Host address
        self.address = Address(host=host, port=cst.DISCOVERY_PORT)

        # Initial list of nodes
        self.gems = seed_gems

        # Copy of initial nodes to fall back on
        self.seed_gems = copy(seed_gems)

        # Logger for system
        self.logger = logger

        # Default read buffer
        self.__buf = cst.DEFAULT_BUFFER

        # Async lock for updating nodes
        self._update_gem_lock = asyncio.Lock()

    @property
    def name(self) -> str:
        """Returns the name of the protocol

        Returns:
            str: Nmae of the protocol
        """
        return self.__class__.__name__

    async def run(self):
        """Main execution loop for the finder protocol.
        """

        # Gather up tasks together for continuous discovery
        tasks = [
            self._server(),
            self._finder_broadcast(),
            self._keepfinding(),
            self._keepalive(),
        ]

        # Execute all tasks
        self.logger.info(f"Initialised {self.name} protocol.")
        await asyncio.gather(*tasks)

    async def _server(self):
        """Main server loop. Accepts incoming connections and passes them to
        the finder handler.
        """

        server = await asyncio.start_server(
            self._finder_handler,
            self.address.host, self.address.port
        )

        self.logger.debug(f"Starting {self.name} protocol server.")
        await server.serve_forever()

    async def _finder_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """Handler method for incoming connections. Adds connected node to the
        list of known nodes and sends back the current list of known nodes back
        to the sender.

        Args:
            reader (asyncio.StreamReader): Reader object for incoming connection
            writer (asyncio.StreamWriter): Writer object for incoming connection
        """

        # Get the socket information
        sock = writer.get_extra_info("socket")
        self.address.host = sock.getsockname()[0]

        # Get the connecting node's address
        gem = sock.getpeername()[0]

        # Ditch the response
        _ = await reader.read(self.__buf)

        # Update the current list of nodes
        await self._update_gems(gem)

        # Create response containing our current list of nodes
        response = create_json_rpc_response(
            cst.JSONRPC_DEFAULT_ID,
            result=[self.gems]
        )

        # Send response to sender
        writer.write(response)
        await writer.drain()

        # Close writer
        writer.close()
        await writer.wait_closed()

    async def _finder_broadcast(self, timer: int = cst.BEACON_TIMER):
        """Executes a callback for each known node, asking for the latest list
        of known nodes on the network.

        Args:
            timer (int, optional): Timer between broadcasts.
            Defaults to cst.BEACON_TIMER.
        """

        # Run this in the background
        while True:
            # Create a callback for each node we know of
            tasks = [
                self._gem_beacon(Address(host=gem, port=self.address.port))
                for gem in self.gems
            ]

            self.logger.info(f"Total ManaGems: {len(self.gems)}")
            self.logger.debug(f"Known ManaGems: {self.gems}")

            # Execute and wait
            await asyncio.gather(*tasks)
            await asyncio.sleep(timer)

    async def _gem_beacon(self, address: Address):
        """Main callback method for the finder broadcasts. Sends an RPC request
        to the given address and updates the current list of nodes with their
        response.

        Args:
            address (Address): Address to ask for known nodes.
        """

        # Send a request to a node and get the repsonse
        response = await send_rpc_request(address.host, address.port)
        result, errors = response.result, response.error

        # Error occured, just return
        if errors:
            self.logger.error(f"Error encountered: {errors}")
            return

        # Update our list of nodes with what they have
        gems = result[0]
        for gem in gems:
            await self._update_gems(gem)

    async def _keepfinding(self, timer: int = cst.BEACON_TIMER):
        """Backup method that runs in the background to act as fallback when
        the list of known nodes is exhausted. Falls back to using the initial
        list of nodes supplied in the hope that they are still alive.

        Args:
            timer (int, optional): Time between broadcasts.
            Defaults to cst.BEACON_TIMER.
        """

        # Always run in the background
        while True:
            # We already have a list of nodes, do nothing
            if len(self.gems) > 0:
                await asyncio.sleep(timer)
                continue

            # No nodes available, fallback to initial nodes
            for gem in self.seed_gems:
                await self._update_gems(gem)

            self.logger.debug(
                "No ManaGems can be found, falling back to initial gems."
            )

            # Still no nodes, raise a warning
            if len(self.gems) == 0:
                self.logger.warning("Cannot find active ManaGems, drained gem!")

            # Wait for next iteration
            await asyncio.sleep(timer)

    async def _keepalive(self, timer: int = cst.KEEPALIVE_TIMER):
        """Continuously checks if each known node in the network is alive.
        Sends a ping message and if no response is given, they are removed
        from the list of known nodes.

        Args:
            timer (int, optional): Time between broadcasts.
            Defaults to cst.BEACON_TIMER.
        """

        # Run in the background, pinging nodes to see if they are alive
        while True:
            # Remove the host node form the list, only care about others
            if self.address.host in self.gems:
                async with self._update_gem_lock:
                    self.gems.remove(self.address.host)

            # Ping each node we know of
            for gem in self.gems:
                # Node is alive, keep going
                if await ping(gem, port=self.address.port):
                    continue

                # Node is dead, remove it
                async with self._update_gem_lock:
                    self.logger.debug(f"Removing ManaGem: {gem}")
                    self.gems.remove(gem)

            # Wait for next iteration
            await asyncio.sleep(timer)

    async def _update_gems(self, gem: str):
        """Updates the list of known nodes with a new node

        Args:
            gem (str): New node to add to list
        """

        # Don't add host node to list of nodes
        if gem == self.address.host:
            return

        # If node isn't in the list and is alive, add it
        if gem not in self.gems and await ping(gem, port=self.address.port):
            async with self._update_gem_lock:
                self.logger.debug(f"Registed new ManaGem: {gem}")
                self.gems.append(gem)
                self.gems = list(set(self.gems))
