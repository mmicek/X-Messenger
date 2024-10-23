import json
import logging

from dns.services.message_type import MessageType

LOGGER = logging.getLogger("Dns.DnsMessageService")


class DnsMessage:

    type = None
    message = None
    chat_room_identifier = None
    application_user_identifier = None
    application_user_identifiers = None

    # Additional objects
    message_str = None


class DnsMessageManager:

    def __init__(self, context):
        self.context = context

    @staticmethod
    async def manage_central_router_message(
        message_dict, message_str, fields
    ) -> DnsMessage:
        """
        Converts websocket message into data class, additionally adding custom fields. If validate_user flag
        also validates if user belong to provided chat room.
        """
        result = DnsMessage()
        result.message_str = message_str

        for field in fields:
            setattr(result, field, message_dict[field])
        return result

    async def notify_server_sockets(
        self, message_data, websocket, websockets_by_app_user_id
    ):
        chat_server_sockets_to_send = set()
        offline_user_identifiers = set()

        for user_identifier in message_data.application_user_identifiers:
            # If user is NOT in the dict, we're assuming that user went offline and we ignore them (ie. we do not
            # route anything). The chat service paradigm is that all users are online all the time! If someone
            # goes offline, and then back online, they shall sync with the chat server, i.e. pull all unread messages
            # in all chat rooms they're in.
            if user_identifier in websockets_by_app_user_id:
                for chat_server_socket in websockets_by_app_user_id[user_identifier]:
                    if chat_server_socket not in chat_server_sockets_to_send:
                        chat_server_sockets_to_send.add(chat_server_socket)
            else:
                if message_data.type == MessageType.ROUTABLE:
                    offline_user_identifiers.add(user_identifier)

        LOGGER.debug(f"Sending to {len(chat_server_sockets_to_send)} clients")
        for socket in chat_server_sockets_to_send:
            await socket.send(message_data.message_str)

        if offline_user_identifiers:
            LOGGER.debug(
                f"Sending message with offline users: {offline_user_identifiers}"
            )
            await self.notify_offline_users(
                offline_user_identifiers, message_data, websocket
            )

    @staticmethod
    async def notify_offline_users(offline_user_identifiers, message_data, socket):
        message = json.dumps(
            {
                "type": MessageType.OFFLINE_NOTIFICATION,
                "application_user_identifiers": list(offline_user_identifiers),
                "chat_room_identifier": message_data.chat_room_identifier,
                "application_user_identifier": message_data.application_user_identifier,
                "message": message_data.message,
            }
        )
        await socket.send(message)
