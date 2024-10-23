import logging

from server.utils.utils import MessageType

LOGGER = logging.getLogger("Server.DynamodbService")


class SystemMessageService:

    def __init__(self, context):
        self.context = context

    async def process_message(self, message_data: dict):
        LOGGER.debug(f"Processing system message: {str(message_data)}")
        chat_room_identifier = message_data["chat_room_identifier"]
        message = message_data["message"]

        application_user_identifiers = (
            await self.context.dynamodb_service.fetch_chat_room(chat_room_identifier)[
                "app_users"
            ]
        )

        if application_user_identifiers:
            message_identifier = (
                await self.context.dynamodb_service.create_system_message(
                    chat_room_identifier, message
                )
            )
            central_router_message = {
                "type": MessageType.SYSTEM_ROUTABLE,
                "chat_room_identifier": chat_room_identifier,
                "application_user_identifiers": application_user_identifiers,
                "message_timestamp_identifier": message_identifier,
                "message": message,
            }
            await self.context.central_router_client.send_message(
                central_router_message
            )
