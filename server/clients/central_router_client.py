import asyncio
import json
import logging

import aiohttp
import websockets
from websockets.exceptions import WebSocketException

from server.settings.settings import (
    CENTRAL_ROUTER_INTERNAL_SECRET,
    CHAT_API_INTERNAL_SECRET,
    CENTRAL_ROUTER_PERMISSION_HEADER,
    WEBSOCKET_SERVER_IDENTIFIER_HEADER,
    CHAT_PERMISSION_HEADER,
)
from server.utils.utils import make_server_call, prepare_server_url

LOGGER = logging.getLogger("Server.DnsClient")


class DnsClient:

    def __init__(self, context):
        self.context = context

        self.identifier_socket_dict = (
            {}
        )  # Identifier of a Central Router -> websocket to that Central Router
        self.identifier_task_dict = (
            {}
        )  # Identifier of a Central Router -> <Task> object holding the msg loop coroutine

        self.router_socket_list = []
        self.operational_router_socket_list = []
        self.round_robin_counter = 0

    async def handle(self):
        while True:
            known_central_routers = await self._fetch_central_routers()
            LOGGER.debug(f"Using list of {len(known_central_routers)} central routers")

            routers_to_connect_to = []
            known_central_routers_identifiers = set()
            current_central_routers_identifiers = self.identifier_socket_dict.keys()

            for central_router in known_central_routers:
                identifier = central_router["identifier"]
                known_central_routers_identifiers.add(identifier)
                if identifier not in current_central_routers_identifiers:
                    routers_to_connect_to.append(central_router)

            if routers_to_connect_to:
                await self._connect_to_routers(routers_to_connect_to)

            routers_to_disconnect_from = (
                current_central_routers_identifiers - known_central_routers_identifiers
            )
            if routers_to_disconnect_from:
                await self._disconnect_from_routers(routers_to_disconnect_from)

            await asyncio.sleep(120)

    async def send_message_to_all_routers(self, message_dict: dict):
        message = json.dumps(message_dict)
        LOGGER.debug(
            f"Sending update message to {len(self.router_socket_list)} central router"
        )
        for socket in self.router_socket_list:
            await socket.send(message)

    async def send_message(self, message_dict: dict):
        socket = self.round_robin_central_router()
        if socket:
            await socket.send(json.dumps(message_dict))

    def round_robin_central_router(self):
        if len(self.operational_router_socket_list) == 0:
            LOGGER.warning("No central routers are connected. Cannot route the message")
            return None

        self.round_robin_counter += 1
        if self.round_robin_counter >= len(self.operational_router_socket_list):
            self.round_robin_counter = 0
        return self.operational_router_socket_list[self.round_robin_counter]

    async def _connect_to_routers(self, routers_to_connect_to: [dict]):
        for central_router in routers_to_connect_to:
            identifier = central_router["identifier"]
            # We do not connect if connecting to the same router is already in progress
            if (
                identifier not in self.identifier_task_dict
                or self.identifier_task_dict[identifier].done()
            ):
                central_router_task = asyncio.create_task(
                    self._central_router_message_loop(
                        central_router["public_ip"], identifier
                    )
                )
                self.identifier_task_dict[identifier] = central_router_task

    async def _central_router_message_loop(self, address, identifier):
        try:
            async with websockets.connect(
                f"ws://{address}",
                extra_headers=[
                    (CENTRAL_ROUTER_PERMISSION_HEADER, CENTRAL_ROUTER_INTERNAL_SECRET),
                    (
                        WEBSOCKET_SERVER_IDENTIFIER_HEADER,
                        self.context.websocket_server_identifier,
                    ),
                ],
            ) as websocket:

                await self._init_central_router_data(websocket, identifier)
                while True:
                    try:

                        message_str = await websocket.recv()
                        message_dict = json.loads(message_str)
                        await self.context.central_router_message_service.handle_message(
                            message_dict, websocket
                        )

                    except WebSocketException as e:
                        await self._close_central_router(websocket.identifier)
                        LOGGER.exception(
                            f"Exception in central_router_message_loop: {str(e)}"
                        )
                        break
                    except Exception as e:
                        LOGGER.exception(
                            f"Exception in central_router_message_loop method: {str(e)}"
                        )

        except Exception as e:
            LOGGER.error(f"Exception when connecting to the central router: {str(e)}")

    async def _init_central_router_data(self, websocket, identifier):
        websocket.identifier = identifier
        self.router_socket_list.append(websocket)
        self.identifier_socket_dict[identifier] = websocket
        await self.context.central_router_message_service.send_full_sync_message(
            websocket
        )

    def set_operational(self, websocket):
        if (
            websocket in self.router_socket_list
            and websocket not in self.operational_router_socket_list
        ):
            LOGGER.info(
                f"Setting OPERATIONAL status for central router {websocket.identifier}"
            )
            self.operational_router_socket_list.append(websocket)

    def is_central_router_available(self):
        return self.operational_router_socket_list != []

    async def _disconnect_from_routers(self, not_active_routers_identifier_list):
        for identifier in not_active_routers_identifier_list:
            LOGGER.debug(
                f"Disconnecting from central router: {identifier} because it is no longer on the known "
                f"central routers list"
            )
            if identifier in self.identifier_socket_dict:
                await self._close_central_router(identifier)

    async def _close_central_router(self, identifier):
        if identifier in self.identifier_socket_dict:
            LOGGER.debug(
                f"Closing connection with central router with identifier: {identifier}"
            )
            websocket = self.identifier_socket_dict[identifier]

            del self.identifier_socket_dict[identifier]
            self.router_socket_list.remove(websocket)
            if websocket in self.operational_router_socket_list:
                self.operational_router_socket_list.remove(websocket)

            self.identifier_task_dict[identifier].cancel()
            del self.identifier_task_dict[identifier]
            await websocket.close()

    @staticmethod
    async def _fetch_central_routers():
        LOGGER.info("Getting list of known central routers from the API")
        headers = {CHAT_PERMISSION_HEADER: CHAT_API_INTERNAL_SECRET}
        async with aiohttp.ClientSession(headers=headers) as session:
            url = prepare_server_url("chat-central-router/")
            return await make_server_call(session, url)
