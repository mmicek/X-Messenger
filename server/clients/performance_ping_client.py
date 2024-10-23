import asyncio
import logging

import aiohttp

from server.settings.settings import (
    CHAT_API_INTERNAL_SECRET,
    CHAT_PERMISSION_HEADER,
)
from server.utils.utils import make_server_call, prepare_server_url


LOGGER = logging.getLogger("Server.StatusPingClient")


class PerformancePingClient:

    def __init__(self, context):
        self.context = context

    async def handle(self):
        while True:
            await asyncio.sleep(5 * 60)
            LOGGER.debug("Sending performance message to django api.")
            headers = {CHAT_PERMISSION_HEADER: CHAT_API_INTERNAL_SECRET}
            async with aiohttp.ClientSession(headers=headers) as session:
                url = prepare_server_url("chat-server-status/report-performance/")
                performance_data = (
                    self.context.dynamodb_performance_service.get_performance_data()
                )
                data = {
                    "identifier": str(self.context.websocket_server_identifier),
                    "timestamp_from": performance_data.from_datetime.strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),
                    "timestamp_to": performance_data.to_datetime.strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),
                    "performance_data": performance_data.data,
                }
                await make_server_call(session, url, method="post", json=data)
