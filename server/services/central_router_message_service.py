import json
import logging

from websockets.exceptions import WebSocketException

from server.utils.utils import MessageType

LOGGER = logging.getLogger("Server.DnsMessageService")


class DnsMessageService:

    def __init__(self, context):
        self.context = context
        self.message_type_handlers = {
            MessageType.ROUTABLE: self.routable_message_handler,
            MessageType.SERVER_MODE: self.sever_mode_handler,
            MessageType.SET_LAST_MESSAGE_READ: self.routable_message_handler,
            MessageType.OFFLINE_NOTIFICATION: self.offline_notification_handler,
            MessageType.SYSTEM_ROUTABLE: self.routable_message_handler,
        }

    async def handle_message(self, message: dict, websocket):
        return await self.message_type_handlers[message["type"]](message, websocket)

    async def routable_message_handler(self, message: dict, websocket):
        # Format
        # {
        #     'application_user_identifiers': str,
        #     'message': str
        # }
        LOGGER.debug("Received message from central router and sending to users")
        application_user_identifiers = message["application_user_identifiers"]
        del message["application_user_identifiers"]
        message_dict = json.dumps(message)

        for user_identifier in application_user_identifiers:
            if user_identifier in self.context.application_user_device_dict:
                devices = self.context.application_user_device_dict[user_identifier]
                for device_identifier, socket in devices.items():
                    LOGGER.debug(
                        f"Sending message to device {device_identifier} owned by user {user_identifier}"
                    )
                    try:
                        # Ignore web socket exception, because one of the users could have disconnected already
                        await socket.send(message_dict)
                    except WebSocketException:
                        pass

    async def offline_notification_handler(self, message: dict, websocket):
        # Format
        # {
        #     'application_user_identifiers': str,
        #     'message': str,
        #     'chat_room_identifier': str
        # }
        application_user_identifiers = message["application_user_identifiers"]
        chat_room_identifier = message["chat_room_identifier"]
        payload = message["message"]
        app_user_identifier = message["application_user_identifier"]

        notification_message = {
            "chat_room_identifier": chat_room_identifier,
            "message": payload,
            "app_user_identifier": app_user_identifier,
            "click_action": "CHAT_NOTIFICATION",
        }

        LOGGER.debug(
            f"Received offline notification message and adding to fcm queue: {application_user_identifiers}"
        )
        for application_user_identifier in application_user_identifiers:
            # Don t want to send notification to user that has sent the message
            if application_user_identifier != app_user_identifier:
                self.context.offline_notification_client.set_offline_application_user(
                    application_user_identifier, notification_message
                )

    async def sever_mode_handler(self, message: dict, websocket):
        self.context.central_router_client.set_operational(websocket)

    async def send_add_app_user_websocket_message(self, application_user_identifier):
        LOGGER.debug(
            f"Sending add update message to central router for user: {application_user_identifier}"
        )

        message = {
            "type": MessageType.ADD_APP_USER_WEBSOCKET,
            "application_user_identifier": application_user_identifier,
        }
        await self.context.central_router_client.send_message_to_all_routers(message)

    async def send_remove_app_user_websocket_message(self, application_user_identifier):
        LOGGER.debug(
            f"Sending remove update message to central router for user: {application_user_identifier}"
        )

        message = {
            "type": MessageType.REMOVE_APP_USER_WEBSOCKET,
            "application_user_identifier": application_user_identifier,
        }
        await self.context.central_router_client.send_message_to_all_routers(message)

    async def send_full_sync_message(self, websocket):
        users_count = len(self.context.application_user_device_dict.keys())
        LOGGER.info(f"Sending init message to central router with {users_count} users")
        message = {
            "type": MessageType.FULL_SYNC,
            "application_user_identifiers": list(
                self.context.application_user_device_dict.keys()
            ),
        }
        await websocket.send(json.dumps(message))
