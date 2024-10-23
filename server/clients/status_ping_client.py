import asyncio
import logging

import aiohttp

from server.settings.settings import (
    CHAT_API_INTERNAL_SECRET,
    CHAT_PERMISSION_HEADER,
)
from server.utils.utils import make_server_call, prepare_server_url

LOGGER = logging.getLogger("Server.StatusPingClient")


class StatusPingClient:

    def __init__(self, context):
        self.context = context

    async def handle(self):
        while True:
            LOGGER.debug("Sending ping message to django api.")
            headers = {CHAT_PERMISSION_HEADER: CHAT_API_INTERNAL_SECRET}
            async with aiohttp.ClientSession(headers=headers) as session:
                url = prepare_server_url("chat-server-status/report-status/")
                data = {
                    "identifier": str(self.context.websocket_server_identifier),
                    "connected_clients_count": len(
                        self.context.application_user_device_dict
                    ),
                    "application_data": self.context.applications_service.get_applications_dict(),
                }
                await make_server_call(session, url, method="post", json=data)
            await asyncio.sleep(5 * 60)
