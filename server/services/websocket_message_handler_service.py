import json
import logging

from server.services.websocket_message_service import WebsocketMessageService
from server.utils.exceptions import (
    ChatRoomIdentifiersListLengthException,
    MissingRequiredFieldException,
)
from server.utils.utils import DecimalEncoder, MessageType, ChatRoomType

LOGGER = logging.getLogger("Server.WebSocketMessageService")


class WebSocketMessageHandlerService:

    def __init__(self, context):
        self.context = context
        self.websocket_message_service = WebsocketMessageService(self.context)
        self.message_type_handlers = {
            MessageType.ROUTABLE: self.chat_message_handler,
            MessageType.GET_HISTORY: self.get_history_message_handler,
            MessageType.SET_LAST_MESSAGE_READ: self.set_last_message_read_handler,
            MessageType.GET_LAST_MESSAGES_READ: self.get_last_messages_read_handler,
            MessageType.GET_LAST_CHAT_ROOM_MESSAGE: self.get_last_chat_room_message_handler,
            MessageType.GET_UNREAD_MESSAGES_COUNT: self.get_unread_messages_count_message_handler,
        }
        self.chat_room_type_handler_permissions = {
            ChatRoomType.REGULAR: [
                MessageType.ROUTABLE,
                MessageType.GET_HISTORY,
                MessageType.SET_LAST_MESSAGE_READ,
                MessageType.GET_LAST_MESSAGES_READ,
                MessageType.GET_LAST_CHAT_ROOM_MESSAGE,
                MessageType.GET_UNREAD_MESSAGES_COUNT,
            ],
            ChatRoomType.MASS_PUBLIC: [MessageType.ROUTABLE, MessageType.GET_HISTORY],
            ChatRoomType.MASS_PRIVATE: [MessageType.ROUTABLE, MessageType.GET_HISTORY],
        }

    async def handle_message(self, message, websocket):
        message_type = message.get("type", None)
        if not message_type:
            raise MissingRequiredFieldException("type")
        return await self.message_type_handlers[message_type](message, websocket)

    async def chat_message_handler(self, message: dict, websocket):
        # Format
        # {
        #     'type': 'ROUTABLE',
        #     'chat_room_identifier': str,
        #     'message': str
        # }
        LOGGER.debug("Received ROUTABLE message from client.")
        message_data = await self.websocket_message_service.manage_websocket_message(
            websocket, message, ["message", "chat_room_identifier"]
        )
        # Persist the message in dynamoDB
        message_identifier = await self.context.dynamodb_service.create_chat_message(
            message_data.chat_room_identifier,
            message_data.application_user_identifier,
            message_data.message,
        )

        # Send message to other users in the chat room, via a central router
        central_router_message = {
            "type": MessageType.ROUTABLE,
            "chat_room_identifier": message_data.chat_room_identifier,
            "app_user_identifier": message_data.application_user_identifier,
            "application_user_identifiers": message_data.application_user_identifiers,
            "message_timestamp_identifier": message_identifier,
            "message": message_data.message,
            "custom_data": await self.context.custom_data_cache_service.get_custom_data(
                message_data.application_user_identifier
            ),
        }
        await self.context.central_router_client.send_message(central_router_message)
        # TODO If chat room type is MASS dont save last message read record
        await self.websocket_message_service.send_set_last_message_read(
            message_data, message_identifier
        )

    async def get_last_messages_read_handler(self, message: dict, websocket):
        #
        # This one is used to get identifiers of last messages read by all users in the chat room. For example
        # if there are 3 users: A, B and C in the chatroom, we return 3 message IDs (for users A, B and C respectively)
        # representing LAST messages marked as read by each of those users.
        #
        # {
        #     'type': str,
        #     'chat_room_identifier': str,
        # }
        LOGGER.debug("Received GET_LAST_MESSAGES_READ message from client.")
        message_data = await self.websocket_message_service.manage_websocket_message(
            websocket, message, ["chat_room_identifier"]
        )

        last_messages_read = (
            await self.context.dynamodb_service.fetch_last_messages_read(
                message_data.chat_room_identifier
            )
        )
        payload = (
            await self.websocket_message_service.prepare_last_messages_read_message(
                last_messages_read
            )
        )
        await websocket.send(payload)

    async def set_last_message_read_handler(self, message: dict, websocket):
        #
        #  Using this to mark a message as LAST message read by the current user.
        #
        # {
        #     'type': str,
        #     'chat_room_identifier': str,
        #     'message_timestamp_identifier': str
        # }
        LOGGER.debug("Received SET_LAST_MESSAGE_READ message from client.")
        message_data = await self.websocket_message_service.manage_websocket_message(
            websocket, message, ["chat_room_identifier", "message_timestamp_identifier"]
        )
        message_identifier = int(message_data.message_timestamp_identifier)
        await self.websocket_message_service.send_set_last_message_read(
            message_data, message_identifier
        )

    async def get_history_message_handler(self, message: dict, websocket):
        #
        # Using this to pull historical messages from given chatroom
        #
        # {
        #     'type': str,
        #     'chat_room_identifier': str,
        #     'from_message_timestamp_identifier': int
        #     'limit': int
        # }
        LOGGER.debug("Received HISTORY message from client.")
        message_data = await self.websocket_message_service.manage_websocket_message(
            websocket,
            message,
            ["chat_room_identifier", "from_message_timestamp_identifier"],
        )
        limit = int(message.get("limit", 20))

        history = await self.context.dynamodb_service.fetch_chat_room_messages(
            message_data.chat_room_identifier,
            int(message_data.from_message_timestamp_identifier),
            limit,
        )
        payload = await self.websocket_message_service.prepare_history_message(
            history, message_data.chat_room_identifier
        )
        await websocket.send(payload)

    async def get_last_chat_room_message_handler(self, message: dict, websocket):
        #
        # Using this to get last chat room message
        #
        # {
        #     'type': str,
        #     'chat_room_identifiers': [str],
        # }
        result = []
        message_data = await self.websocket_message_service.manage_websocket_message(
            websocket, message, ["chat_room_identifiers"], validate_user=False
        )

        for chat_room_identifier in message_data.chat_room_identifiers:
            await self.websocket_message_service.validate_users_in_chat_room(
                message_data.application_user_identifier,
                chat_room_identifier,
                MessageType.GET_LAST_CHAT_ROOM_MESSAGE,
            )
            last_message_info = (
                await self.websocket_message_service.get_chat_room_last_message_info(
                    chat_room_identifier, message_data
                )
            )
            result.append(last_message_info)

        result_message = {
            "type": MessageType.GET_LAST_CHAT_ROOM_MESSAGE,
            "payload": result,
        }
        payload = json.dumps(result_message, cls=DecimalEncoder)
        await websocket.send(payload)

    async def get_unread_messages_count_message_handler(self, message: dict, websocket):
        #
        # Using this to get count of unread messages in chat rooms
        #
        # {
        #     'type': str,
        #     'chat_room_identifiers': [str],
        # }
        result = []
        message_data = await self.websocket_message_service.manage_websocket_message(
            websocket, message, ["chat_room_identifiers"], validate_user=False
        )
        if len(message_data.chat_room_identifiers) > 10:
            raise ChatRoomIdentifiersListLengthException()

        for chat_room_identifier in message_data.chat_room_identifiers:
            await self.websocket_message_service.validate_users_in_chat_room(
                message_data.application_user_identifier,
                chat_room_identifier,
                MessageType.GET_UNREAD_MESSAGES_COUNT,
            )
            unread_message_info = (
                await self.websocket_message_service.get_chat_room_unread_message_info(
                    message_data.application_user_identifier, chat_room_identifier
                )
            )
            result.append(unread_message_info)

        result_message = {
            "type": MessageType.GET_UNREAD_MESSAGES_COUNT,
            "payload": result,
        }
        payload = json.dumps(result_message, cls=DecimalEncoder)
        await websocket.send(payload)
