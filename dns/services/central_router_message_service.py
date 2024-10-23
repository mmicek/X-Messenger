import logging

from dns.services.central_router_message_manager import (
    DnsMessageManager,
    DnsMessage,
)
from dns.services.message_type import MessageType

LOGGER = logging.getLogger("Dns.DnsService")


class DnsMessageService:
    """
    Business logic class for maintaining the state of app user ID dict and handling messages between
    the Central Router and Chat Websocket Servers
    """

    def __init__(self, context):
        # The main map in form of Application_user_identifier -> {websocket, websocket, ....}
        self.websockets_by_app_user_id = {}
        # Messages handlers registry
        self.message_type_handlers = {
            MessageType.ADD_APP_USER_WEBSOCKET: self.add_app_user_websocket_message_handler,
            MessageType.REMOVE_APP_USER_WEBSOCKET: self.remove_app_user_websocket_message_handler,
            MessageType.ROUTABLE: self.routable_message_handler,
            MessageType.SYSTEM_ROUTABLE: self.routable_message_handler,
            MessageType.FULL_SYNC: self.full_sync_message_handler,
            MessageType.SET_LAST_MESSAGE_READ: self.routable_message_handler,
        }
        self.context = context
        self.central_router_message_manager = DnsMessageManager(self.context)

    async def handle_message(self, message: dict, message_str: str, websocket):
        # Note: the "websocket" param is an instance of  DnsServerProtocol
        return await self.message_type_handlers[message["type"]](
            message, message_str, websocket
        )

    async def add_app_user_websocket_message_handler(
        self, message, message_str, websocket
    ):
        # Format
        # {
        #     'type': str,
        #     'application_user_identifier': str,
        # }
        message_data = (
            await self.central_router_message_manager.manage_central_router_message(
                message, message_str, ["application_user_identifier"]
            )
        )
        LOGGER.debug(
            f"Updating add APPLICATION_USER_DICT with user: {message_data.application_user_identifier}"
        )

        if (
            message_data.application_user_identifier
            not in self.websockets_by_app_user_id
        ):
            self.websockets_by_app_user_id[message_data.application_user_identifier] = (
                set()
            )

        self.websockets_by_app_user_id[message_data.application_user_identifier].add(
            websocket
        )
        LOGGER.debug(
            f"APPLICATION_USER_DICT size is: {len(self.websockets_by_app_user_id)}"
        )

    async def remove_app_user_websocket_message_handler(
        self, message, message_str, websocket
    ):
        # Format
        # {
        #     'type': str,
        #     'application_user_identifier': str,
        # }
        message_data = (
            await self.central_router_message_manager.manage_central_router_message(
                message, message_str, ["application_user_identifier"]
            )
        )
        LOGGER.debug(
            f"Updating remove APPLICATION_USER_DICT with user: {message_data.application_user_identifier}"
        )

        if message_data.application_user_identifier in self.websockets_by_app_user_id:
            self.websockets_by_app_user_id[
                message_data.application_user_identifier
            ].remove(websocket)
        if not self.websockets_by_app_user_id[message_data.application_user_identifier]:
            del self.websockets_by_app_user_id[message_data.application_user_identifier]
        LOGGER.debug(
            f"APPLICATION_USER_DICT size is: {len(self.websockets_by_app_user_id)}"
        )

    async def routable_message_handler(self, message, message_str, websocket):
        # Format
        # {
        #     'type': str,
        #     'application_user_identifiers': [str],
        #     'message': str
        # }
        message_data = DnsMessage()
        message_data.type = message["type"]
        message_data.application_user_identifiers = message[
            "application_user_identifiers"
        ]
        message_data.message_str = message_str

        if message_data.type == MessageType.ROUTABLE:
            message_data.application_user_identifier = message["app_user_identifier"]
            message_data.chat_room_identifier = message["chat_room_identifier"]
            message_data.message = message["message"]

        LOGGER.debug(
            f"Received routable message of type {message_data.type} from chat websocket server"
        )
        await self.central_router_message_manager.notify_server_sockets(
            message_data, websocket, self.websockets_by_app_user_id
        )

    async def full_sync_message_handler(self, message, message_str, websocket):
        # Format
        # {
        #     'type': str,
        #     'application_user_identifiers': [str],
        # }
        message_data = (
            await self.central_router_message_manager.manage_central_router_message(
                message, message_str, ["application_user_identifiers"]
            )
        )
        LOGGER.debug(
            f"Initializing APPLICATION_USER_DICT with {len(message_data.application_user_identifiers)} users"
        )

        for application_user_identifier in message_data.application_user_identifiers:
            if application_user_identifier not in self.websockets_by_app_user_id:
                self.websockets_by_app_user_id[application_user_identifier] = set()
            self.websockets_by_app_user_id[application_user_identifier].add(websocket)

        # If central router is in operational mode, inform the websocket server about it
        await self.context.connections_manager.tell_them_if_i_am_ready(websocket)

    async def close_chat_websocket_server_connection(self, websocket):
        removed_user_counter = 0
        for (
            application_user_identifier,
            websockets,
        ) in self.websockets_by_app_user_id.items():
            if websocket in websockets:
                self.websockets_by_app_user_id[application_user_identifier].remove(
                    websocket
                )
                removed_user_counter += 1

        # Keep only app user Id keys where value is a non-empty websocket set
        self.websockets_by_app_user_id = {
            k: v for k, v in self.websockets_by_app_user_id.items() if v
        }
        LOGGER.info(
            f"Dropped connection with websocket server. {removed_user_counter} users were removed"
        )
