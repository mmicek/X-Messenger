import asyncio
import json
import logging

import requests

from dns.services.central_router_message_service import MessageType
from dns.settings.settings import CHAT_API_INTERNAL_SECRET, CHAT_API_URL

LOGGER = logging.getLogger("Dns.ChatServerClient")


class ServerMode:
    INITIALIZATION = "INITIALIZATION"
    OPERATIONAL = "OPERATIONAL"


class ConnectionsManager:
    CHAT_PERMISSION_HEADER = "X-CHAT-INTERNAL-SECRET"

    def __init__(self, context):
        self.context = context
        self.current_server_mode = ServerMode.INITIALIZATION
        # The dict to manage connections between Central Router and Chat Websocket Servers
        # The key is chat server's ID (a GUID), and a value is a websocket object (a DnsServerProtocol
        # instance). When new server connets it gets added, and if disconnects, it gets removed from the dict.
        self.chat_server_websockets = {}
        # How many chat websocket servers we are expecting to connect.
        self.expected_chat_servers_count = self._fetch_chat_servers_count()
        self.event = asyncio.Event()

    async def process_request(self, identifier, websocket):
        self.chat_server_websockets[identifier] = websocket

        if self.current_server_mode == ServerMode.INITIALIZATION:
            if len(self.chat_server_websockets) == self.expected_chat_servers_count:
                LOGGER.debug(f"All chat websocket servers connected")
                self.event.set()

    async def close(self, identifier):
        if identifier in self.chat_server_websockets:
            websocket = self.chat_server_websockets[identifier]
            del self.chat_server_websockets[identifier]
            await self.context.central_router_message_service.close_chat_websocket_server_connection(
                websocket
            )

    async def wait_for_chat_websocket_servers_to_connect(self):
        timeout = None
        if self.expected_chat_servers_count != 0:
            try:
                timeout = 5 * 60
                LOGGER.info(
                    f"Waiting for {self.expected_chat_servers_count} chat servers to connect in INITIALIZATION state."
                )
                await asyncio.wait_for(self.event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                LOGGER.warning(
                    f"Not all chat servers connected before {timeout} timeout"
                )

        LOGGER.debug(f"Setting server mode to OPERATIONAL")
        self.current_server_mode = ServerMode.OPERATIONAL

        # Inform all chat servers that connected to router in INITIALIZATION mode
        for socket in self.chat_server_websockets.values():
            await self.tell_them_if_i_am_ready(socket)

    async def tell_them_if_i_am_ready(self, websocket):
        """
        Tells the websocket server that Central Router is in Operational mode.
        """
        operational_mode_message = json.dumps(
            {"type": MessageType.SERVER_MODE, "message": ServerMode.OPERATIONAL}
        )
        LOGGER.info("Sending notification OPERATIONAL status")
        if self.current_server_mode == ServerMode.OPERATIONAL:
            await websocket.send(operational_mode_message)

    def _fetch_chat_servers_count(self):
        """
        Makes a call to chat service REST API to learn how many chat servers are expected to connect.
        """
        headers = {self.CHAT_PERMISSION_HEADER: CHAT_API_INTERNAL_SECRET}
        url = self._prepare_url("chat-server/")
        response = requests.get(url, headers=headers)
        if response.status_code == 500:
            raise SystemError(
                f"Chat server returned internal server error 500 response code."
            )
        if response.status_code == 502:
            raise SystemError(
                f"Chat server returned bad gateway server error, 502 response code."
            )
        if response.status_code != 200:
            raise SystemError(
                f"Chat server returned non 200 response code: {response.json()}."
            )

        counter = 0
        for chat_server in response.json():
            counter += chat_server["instances"]
        LOGGER.info(f"I will wait for {counter} chat servers to connect.")
        return counter

    @classmethod
    def _prepare_url(cls, path):
        return f"{CHAT_API_URL}/internal-server-to-server/v1/{path}"
