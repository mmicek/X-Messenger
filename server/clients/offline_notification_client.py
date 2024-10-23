import logging

import asyncio

from server.clients.firebase_client import FirebaseClient
from server.settings.settings import FCM_NOTIFICATION_SEC_INTERVAL

LOGGER = logging.getLogger("Server.OfflineNotificationClient")


class OfflineNotificationClient:

    def __init__(self, context):
        self.context = context
        self.application_identifier_offline_message_dict = (
            {}
        )  # Device identifier -> message dict
        self.firebase_client = FirebaseClient(self.context)

    async def handle(self):
        while True:
            LOGGER.info(
                f"Sending notification to offline users: {self.application_identifier_offline_message_dict}"
            )
            offline_message_dict = (
                self.application_identifier_offline_message_dict.copy()
            )
            self.application_identifier_offline_message_dict.clear()

            device_message_dict = {}
            for application_user_identifier, message in offline_message_dict.items():
                (device_fcm_tokens, application_identifier) = (
                    await self.context.device_identifier_cache_service.get_user_identifier_fcm_tokens(
                        application_user_identifier
                    )
                )
                LOGGER.debug(
                    f"Fetched device fcm tokens for user: {application_user_identifier}: {device_fcm_tokens}"
                )
                for device_fcm_token in device_fcm_tokens:
                    if device_fcm_token:
                        device_message_dict[device_fcm_token] = (
                            message,
                            application_identifier,
                        )

            application_fcm_dict = (
                self.firebase_client.get_copied_application_fcm_dict()
            )
            await asyncio.get_event_loop().run_in_executor(
                None,
                self.execute_send_notification,
                application_fcm_dict,
                device_message_dict,
            )
            await asyncio.sleep(FCM_NOTIFICATION_SEC_INTERVAL)

    def set_offline_application_user(self, application_user_identifier, message):
        self.application_identifier_offline_message_dict[
            application_user_identifier
        ] = message

    @staticmethod
    def execute_send_notification(application_fcm_dict, device_message_dict):
        FirebaseClient.send_fcm_notifications(application_fcm_dict, device_message_dict)
