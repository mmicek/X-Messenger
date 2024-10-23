import logging

import aiohttp
import asyncio

from server.settings.settings import (
    CHAT_PERMISSION_HEADER,
    CHAT_API_INTERNAL_SECRET,
)
from server.utils.utils import prepare_server_url, make_server_call

LOGGER = logging.getLogger("Server.ApplicationSettingsClient")


class ApplicationSettingsClient:

    def __init__(self, context):
        self.context = context

    async def handle(self):
        while True:
            LOGGER.debug("Fetching application settings from django api.")
            headers = {CHAT_PERMISSION_HEADER: CHAT_API_INTERNAL_SECRET}
            async with aiohttp.ClientSession(headers=headers) as session:
                url = prepare_server_url("applications/")
                response = await make_server_call(session, url, method="get")

                if response:
                    self.context.applications_service.clear_application_settings()

                    for application_dict in response["results"]:
                        LOGGER.debug(
                            f"Setting application settings for application: {application_dict}"
                        )
                        self.context.applications_service.set_application_settings(
                            application_dict["identifier"], application_dict
                        )

                    self.context.offline_notification_client.firebase_client.reset_application_fcm_dict()
            await asyncio.sleep(15 * 60)
