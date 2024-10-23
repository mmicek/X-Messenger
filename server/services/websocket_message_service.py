import json
import time
from decimal import Decimal

from server.utils.exceptions import (
    UserNotInChatRoomException,
    MissingRequiredFieldException,
    InvalidChatRoomMessageTypeException,
    ChatRoomDoesNotExistsException,
)
from server.utils.utils import MessageType, DecimalEncoder, ChatRoomType


class WebsocketMessage:

    type = None
    message = None
    chat_room_identifier = None
    chat_room_identifiers = None
    message_timestamp_identifier = None
    from_message_timestamp_identifier = None

    # Dynamodb objects
    application_user_identifiers = None
    application_user_identifier = None
    device_identifier = None


class WebsocketMessageService:

    def __init__(self, context):
        self.context = context
        self.last_message_read_limit = 100

    async def manage_websocket_message(
        self, websocket, message_dict, fields, validate_user=True
    ) -> WebsocketMessage:
        """
        Converts websocket message into data class, additionally adding custom fields. If validate_user flag
        also validates if user belong to provided chat room.
        """
        result = WebsocketMessage()
        result.application_user_identifier = websocket.application_user_identifier
        result.device_identifier = websocket.device_identifier

        if validate_user:
            chat_room_identifier = message_dict.get("chat_room_identifier", None)
            if not chat_room_identifier:
                raise MissingRequiredFieldException("chat_room_identifier")
            application_user_identifiers = await self.validate_users_in_chat_room(
                result.application_user_identifier,
                chat_room_identifier,
                method_type=message_dict["type"],
            )
            result.application_user_identifiers = application_user_identifiers

        for field in fields:
            value = message_dict.get(field, None)
            if not value:
                raise MissingRequiredFieldException(field)
            setattr(result, field, value)
        return result

    async def validate_users_in_chat_room(
        self, application_user_identifier, chat_room_identifier, method_type
    ):
        """
        Find other users in the chat room. It also validates if current user is in the chat room
        """
        chat_room = await self.context.dynamodb_service.fetch_chat_room(
            chat_room_identifier
        )
        if not chat_room:
            raise ChatRoomDoesNotExistsException()
        chat_room_type = await self.validate_chat_room_type_handler_permission(
            chat_room, method_type
        )
        if chat_room_type != ChatRoomType.MASS_PUBLIC:
            if application_user_identifier not in chat_room["app_users"]:
                raise UserNotInChatRoomException(
                    chat_room_identifier, application_user_identifier
                )
        return chat_room["app_users"]

    async def validate_chat_room_type_handler_permission(self, chat_room, method_type):
        chat_room_type = int(chat_room.get("type", ChatRoomType.REGULAR))
        if (
            method_type
            not in self.context.websocket_message_service.chat_room_type_handler_permissions[
                chat_room_type
            ]
        ):
            raise InvalidChatRoomMessageTypeException(chat_room_type, method_type)
        return chat_room_type

    async def send_set_last_message_read(
        self, message_data: WebsocketMessage, message_identifier
    ):
        # Persist in the dynamoDB
        await self.context.dynamodb_service.update_last_message_read(
            message_data.chat_room_identifier,
            message_data.application_user_identifier,
            message_identifier,
        )

        # Send the message to all users in the chat room (via the central router)
        central_router_message = {
            "type": MessageType.SET_LAST_MESSAGE_READ,
            "chat_room_identifier": message_data.chat_room_identifier,
            "app_user_identifier": message_data.application_user_identifier,
            "application_user_identifiers": message_data.application_user_identifiers,
            "message_timestamp_identifier": message_identifier,
            "custom_data": await self.context.custom_data_cache_service.get_custom_data(
                message_data.application_user_identifier
            ),
        }
        await self.context.central_router_client.send_message(central_router_message)

    async def prepare_last_messages_read_message(self, last_messages_read):
        for last_message in last_messages_read:
            identifier = last_message.get("app_user_identifier", None)
            if identifier:
                last_message["custom_data"] = (
                    await self.context.custom_data_cache_service.get_custom_data(
                        identifier
                    )
                )
            del last_message["identifier"]

        result = {
            "type": MessageType.GET_LAST_MESSAGES_READ,
            "payload": last_messages_read,
        }
        return json.dumps(result, cls=DecimalEncoder)

    async def prepare_history_message(self, history, chat_room_identifier):
        for message in history:
            identifier = message.get("app_user_identifier", None)
            if identifier:
                message["custom_data"] = (
                    await self.context.custom_data_cache_service.get_custom_data(
                        identifier
                    )
                )

        result = {
            "type": MessageType.GET_HISTORY,
            "chat_room_identifier": chat_room_identifier,
            "payload": history,
        }
        return json.dumps(result, cls=DecimalEncoder)

    async def get_chat_room_last_message_info(
        self, chat_room_identifier, message_data: WebsocketMessage
    ):
        now_message_identifier = time.time_ns()
        chat_room_message = (
            await self.context.dynamodb_service.fetch_chat_room_messages(
                chat_room_identifier, now_message_identifier, 1
            )
        )

        has_unread_messages = False
        last_message_text = None
        message_timestamp_identifier = None
        if chat_room_message:
            last_message_text = chat_room_message[0]["message"]
            message_timestamp_identifier = int(
                chat_room_message[0]["message_timestamp_identifier"]
            )
            has_unread_messages = await self.get_has_unread_messages(
                chat_room_identifier,
                message_timestamp_identifier,
                message_data.application_user_identifier,
            )

        return {
            "chat_room_identifier": chat_room_identifier,
            "has_unread_messages": has_unread_messages,
            "last_message_text": last_message_text,
            "message_timestamp_identifier": message_timestamp_identifier,
        }

    async def get_chat_room_unread_message_info(
        self, application_user_identifier, chat_room_identifier
    ):
        now_message_identifier = time.time_ns()
        last_messages_read = (
            await self.context.dynamodb_service.fetch_last_messages_read(
                chat_room_identifier
            )
        )

        last_message_read_timestamp_identifier = Decimal(0)
        for last_message_read in last_messages_read:
            if last_message_read["app_user_identifier"] == application_user_identifier:
                last_message_read_timestamp_identifier = last_message_read[
                    "message_timestamp_identifier"
                ]

        chat_room_last_messages = (
            await self.context.dynamodb_service.fetch_chat_room_messages(
                chat_room_identifier,
                now_message_identifier,
                self.last_message_read_limit,
            )
        )

        unread_messages_count = 0
        if chat_room_last_messages:
            for chat_room_last_message in chat_room_last_messages:
                if (
                    chat_room_last_message["message_timestamp_identifier"]
                    > last_message_read_timestamp_identifier
                ):
                    unread_messages_count += 1
                else:
                    break
        return {
            "chat_room_identifier": chat_room_identifier,
            "unread_messages_count": unread_messages_count,
        }

    async def get_has_unread_messages(
        self, chat_room_identifier, last_message_in_chat_room, app_user_identifier
    ):
        has_unread_messages = True
        users = await self.context.dynamodb_service.fetch_read_message_users(
            chat_room_identifier, last_message_in_chat_room
        )
        for user in users:
            if user["app_user_identifier"] == app_user_identifier:
                has_unread_messages = False
        return has_unread_messages
