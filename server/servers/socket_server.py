import asyncio
import logging
import traceback
import uuid
from http import HTTPStatus
import json
from json import JSONDecodeError
from websockets.exceptions import WebSocketException
from websockets.legacy.server import ServerProtocol, serve

from server.clients.application_settings_client import (
    ApplicationSettingsClient,
)
from server.clients.central_router_client import DnsClient
from server.clients.offline_notification_client import (
    OfflineNotificationClient,
)
from server.clients.performance_ping_client import PerformancePingClient
from server.clients.status_ping_client import StatusPingClient
from server.services.anti_spam_service import AntiSpamMixin
from server.services.applications_service import ApplicationService
from server.services.cache_service import (
    CustomDataCacheService,
    DeviceFcmTokenCacheService,
)
from server.services.central_router_message_service import (
    DnsMessageService,
)
from server.services.dynamodb_performance_service import (
    DynamodbPerformanceService,
)
from server.services.dynamodb_service import DynamodbService
from server.services.email_exception_service import EmailExceptionService
from server.services.manager_message_handler_service import (
    ManagerMessageHandlerService,
)
from server.services.socket_service import SocketService
from server.services.websocket_message_handler_service import (
    WebSocketMessageHandlerService,
)
from server.settings.settings import (
    DYNAMO_SESSION_TABLE_NAME,
    DYNAMO_CHAT_ROOM_TABLE_NAME,
    DYNAMO_CHAT_MESSAGE_TABLE_NAME,
    DYNAMO_LAST_MESSAGE_READ_TABLE_NAME,
    DYNAMO_USER_IDENTIFIER_CUSTOM_DATA_TABLE_NAME,
)
from server.utils.exceptions import (
    CustomException,
    InvalidMessageFormat,
    ChatServerException,
    DnsConnectionsException,
    MessageSpamException,
)
from server.utils.utils import send_error

LOGGER = logging.getLogger("Server.SocketServer")


class Context:

    def __init__(self):

        # Application User Identifier -> { Device Identifier -> websocket}
        # A two-level dict, mapping app user identifier to a dict, where keys are Device Identifiers and values are
        # websockets. Allows to find a websocket for given user identifier and device identifier.
        self.application_user_device_dict = {}
        self.websocket_server_identifier = uuid.uuid4()
        self.email_exception_service = EmailExceptionService(self)

        # Clients
        self.status_ping_client = StatusPingClient(self)
        self.offline_notification_client = OfflineNotificationClient(self)
        self.central_router_client = DnsClient(self)
        self.application_settings_client = ApplicationSettingsClient(self)
        self.performance_ping_client = PerformancePingClient(self)

        # Services
        self.central_router_message_service = DnsMessageService(self)
        self.websocket_message_service = WebSocketMessageHandlerService(self)
        self.custom_data_cache_service = CustomDataCacheService(self)
        self.device_identifier_cache_service = DeviceFcmTokenCacheService(self)
        self.socket_service = SocketService(self)
        self.applications_service = ApplicationService(self)

        self.manager_message_service = ManagerMessageHandlerService(self)

        # Dynamodb
        self.dynamodb_service = DynamodbService(
            self,
            DYNAMO_SESSION_TABLE_NAME,
            DYNAMO_CHAT_ROOM_TABLE_NAME,
            DYNAMO_CHAT_MESSAGE_TABLE_NAME,
            DYNAMO_LAST_MESSAGE_READ_TABLE_NAME,
            DYNAMO_USER_IDENTIFIER_CUSTOM_DATA_TABLE_NAME,
        )
        self.dynamodb_performance_service = DynamodbPerformanceService(self)


class ChatServer:

    def __init__(self):
        self.context = Context()

    async def serve(self, host, port):
        await self.context.dynamodb_service.connect()
        central_router_task = asyncio.create_task(
            self.context.central_router_client.handle()
        )
        status_ping_task = asyncio.create_task(self.context.status_ping_client.handle())
        offline_notification_task = asyncio.create_task(
            self.context.offline_notification_client.handle()
        )
        application_settings_task = asyncio.create_task(
            self.context.application_settings_client.handle()
        )
        performance_ping_task = asyncio.create_task(
            self.context.performance_ping_client.handle()
        )

        socket_server_task = asyncio.create_task(self.server_task(host, port))
        await asyncio.gather(
            central_router_task,
            socket_server_task,
            status_ping_task,
            offline_notification_task,
            application_settings_task,
            performance_ping_task,
        )

    async def server_task(self, host, port):
        try:
            async with serve(
                self.messages_loop,
                host,
                port,
                create_protocol=ChatServerProtocol,
            ) as ws_server:
                self.server_socket = ws_server
                ws_server.context = self.context
                await asyncio.get_event_loop().create_future()
        except Exception as e:
            LOGGER.error(f"Error in server task: {str(e)}")
        finally:
            LOGGER.info("Finished server task")

    async def messages_loop(self, websocket: "ChatServerProtocol", path):
        message = None
        while True:
            try:

                message = await websocket.recv()
                websocket.check_anti_spam()

                if not websocket.is_manager:
                    if (
                        not self.context.central_router_client.is_central_router_available()
                    ):
                        exception = DnsConnectionsException()
                        await self.context.email_exception_service.notify_admin(
                            exception, None
                        )
                        raise exception
                    message_dict = json.loads(message)
                    await self.context.websocket_message_service.handle_message(
                        message_dict, websocket
                    )
                else:
                    message_dict = json.loads(message)
                    await self.context.manager_message_service.handle_message(
                        message_dict, websocket
                    )

            except MessageSpamException as e:
                await send_error(e, websocket)
                return
            except CustomException as e:
                LOGGER.warning(f"Custom exception: {e.get_message()}")
                await send_error(e, websocket)
            except JSONDecodeError:
                LOGGER.exception(
                    f"Invalid message format: Must be a dictionary with proper fields: {message}."
                )
                await send_error(InvalidMessageFormat(), websocket)
            except WebSocketException as e:
                LOGGER.warning(
                    f"WebSocketException exception in messages_loop method: {str(e)}."
                )
                # We want to ignore this error due to close method in server protocol
                return
            except Exception as e:
                LOGGER.exception(
                    f"{e.__class__.__name__} exception in messages_loop method: {str(e)}."
                )
                stack_trace = traceback.format_exc()
                await self.context.email_exception_service.notify_admin(
                    e, stack_trace, message
                )
                await send_error(
                    ChatServerException(e.__class__.__name__, e), websocket
                )
                return
            # IMPORTANT. This method cannot raise an exception, cause on_close method on protocol will not be called.


class ChatServerProtocol(ServerProtocol, AntiSpamMixin):

    async def process_request(self, path, request_headers):
        LOGGER.info(f"Received message from path: {path}")
        self.connection_closed = False

        token, error_response = self.context.socket_service.validate_token(
            path, request_headers
        )
        if error_response and not token:
            return error_response

        session, error_response = await self.context.socket_service.validate_session(
            token
        )
        if error_response and not session:
            return error_response
        self.application_identifier = token.split(":")[-1]

        self.is_manager = await self.context.socket_service.validate_manager_header(
            request_headers
        )
        self.init_counter()

        can_be_connected = self.context.applications_service.increase_user_count(
            self.application_identifier
        )
        if not can_be_connected:
            message = "Connection refused: exceeded max concurrent online users limit for the application."
            return HTTPStatus.BAD_REQUEST, [], message

        if not self.is_manager:
            (self.application_user_identifier, self.device_identifier) = (
                await self.context.socket_service.manage_user_websocket_connection(
                    session, self
                )
            )

    async def wait_closed(self):
        await self.close_websocket_connection()
        await super().wait_closed()

    async def close(self, code: int = 1000, reason: str = ""):
        await self.close_websocket_connection()
        await super().close(code, reason)

    async def close_websocket_connection(self):
        if not self.connection_closed:
            LOGGER.info(f"Closing session for device {self.device_identifier}")
            self.context.applications_service.reduce_user_count(
                self.application_identifier
            )
            await self.context.socket_service.close_user_websocket_connection(self)
            self.connection_closed = True

    @property
    def context(self):
        return self.ws_server.context
