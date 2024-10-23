import asyncio
import json
import logging
import traceback
from http import HTTPStatus
from json import JSONDecodeError

from websockets.exceptions import (
    WebSocketException,
    ConnectionClosedError,
    ConnectionClosedOK,
)
from websockets.legacy.server import ServerProtocol, serve

from dns.services.central_router_message_service import (
    DnsMessageService,
)
from dns.managers.connections_manager import ConnectionsManager
from dns.services.central_router_message_manager import (
    DnsMessageManager,
)
from dns.services.email_exception_service import EmailExceptionService
from dns.settings.settings import CENTRAL_ROUTER_INTERNAL_SECRET

LOGGER = logging.getLogger("Dns.SocketServer")


class Context:
    """
    We use this class as a registry of 'global' variables. The context object is instantiated just once.
    """

    def __init__(self):
        self.connections_manager = ConnectionsManager(self)
        self.central_router_message_service = DnsMessageService(self)
        self.email_exception_service = EmailExceptionService(self)


class DnsServer:

    def __init__(self):
        self.context = Context()  # The only place where we instantiate the context

    async def serve(self, host, port):
        wait_for_websocket_servers_task = asyncio.create_task(
            self.context.connections_manager.wait_for_chat_websocket_servers_to_connect()
        )

        socket_server_task = asyncio.create_task(self.server_task(host, port))
        await asyncio.gather(wait_for_websocket_servers_task, socket_server_task)

    async def server_task(self, host, port):
        try:
            async with serve(
                self.messages_loop,
                host,
                port,
                create_protocol=DnsServerProtocol,
                ping_timeout=None,
            ) as ws_server:
                # Using this trick to pass the context object to the DnsServerProtocol instance
                self.server_socket = ws_server
                ws_server.context = self.context
                await asyncio.get_event_loop().create_future()
        except Exception as e:
            LOGGER.error(f"Error in server task: {str(e)}")
        finally:
            LOGGER.info("Finished server task")

    async def messages_loop(self, websocket, path):
        # Note: the "websocket" param is an instance of  DnsServerProtocol
        message_str = None
        while True:
            try:
                message_str = await websocket.recv()
                message_dict = json.loads(message_str)
                await self.context.central_router_message_service.handle_message(
                    message_dict, message_str, websocket
                )
            except JSONDecodeError as e:
                LOGGER.error(
                    f"Invalid message format: Must be a dictionary with proper fields: {message_str}."
                )
                stack_trace = traceback.format_exc()
                await self.context.email_exception_service.notify_admin(e, stack_trace)
                return
            except ConnectionClosedOK:
                # This exception is raise when django-api will close connection after sending system message.
                return
            except WebSocketException as e:
                LOGGER.error(f"WebSocketException in messages_loop method: {str(e)}")
                stack_trace = traceback.format_exc()
                await self.context.email_exception_service.notify_admin(e, stack_trace)
                # We want to ignore this error due to close method in server protocol
                return
            except Exception as e:
                stack_trace = traceback.format_exc()
                await self.context.email_exception_service.notify_admin(e, stack_trace)
                LOGGER.exception(
                    f"{e.__class__.__name__} exception in messages_loop method: {str(e)}."
                )
                return
            # IMPORTANT. This method cannot raise an exception, cause on_close method on protocol will not be called.


class DnsServerProtocol(ServerProtocol):
    """
    Websocket server will instantiate this class automatically on each connection.
    """

    CENTRAL_ROUTER_PERMISSION_HEADER = "X-ROUTER-INTERNAL-SECRET"
    WEBSOCKET_SERVER_IDENTIFIER_HEADER = "X-WEBSOCKET-SERVER-IDENTIFIER"
    SYSTEM_MESSAGE_SOCKET_HEADER = "X-IS-SYSTEM-MESSAGE-SOCKET"

    async def process_request(self, path, request_headers):
        # This function is called on opening new websocket connection (do not confuse this with functionality of
        # the messages_loop, where socket messages are being processed). It's HTTP request/response handler
        LOGGER.info("Processing connection request.")
        self.connection_closed = False

        # We check whether to accept the connection or not
        validation_result = self._validate_permission_headers(request_headers)
        if validation_result is not None:
            return validation_result

        self.is_system_message = self._is_system_message_socket(request_headers)
        if not self.is_system_message:
            validation_result = self._validate_websocket_server_header(request_headers)
            if validation_result is not None:
                return validation_result

            self.identifier = request_headers[self.WEBSOCKET_SERVER_IDENTIFIER_HEADER]
            LOGGER.info(
                f"New connection from chat websocket server with identifier: {self.identifier}"
            )
            await self.context.connections_manager.process_request(
                self.identifier, self
            )
        else:
            LOGGER.info(f"New connection from system message socket")

    async def wait_closed(self):
        await self.close_websocket_connection()
        await super().wait_closed()

    async def close(self, code: int = 1000, reason: str = ""):
        await self.close_websocket_connection()
        await super().close(code, reason)

    async def close_websocket_connection(self):
        if not self.connection_closed:
            if not self.is_system_message:
                LOGGER.info(
                    f"Closing connection with chat websocket server with identifier: {self.identifier}"
                )
                await self.context.connections_manager.close(self.identifier)
            else:
                LOGGER.info("Closing connection with system message socket")
            self.connection_closed = True

    def _validate_permission_headers(self, request_headers):
        if self.CENTRAL_ROUTER_PERMISSION_HEADER not in request_headers:
            return self._response(
                HTTPStatus.FORBIDDEN,
                f"{self.CENTRAL_ROUTER_PERMISSION_HEADER} header is not present",
            )
        if (
            request_headers[self.CENTRAL_ROUTER_PERMISSION_HEADER]
            != CENTRAL_ROUTER_INTERNAL_SECRET
        ):
            return self._response(
                HTTPStatus.FORBIDDEN,
                f"{self.CENTRAL_ROUTER_PERMISSION_HEADER} header is invalid: "
                f"{request_headers[self.CENTRAL_ROUTER_PERMISSION_HEADER]}",
            )
        return None

    def _validate_websocket_server_header(self, request_headers):
        if self.WEBSOCKET_SERVER_IDENTIFIER_HEADER not in request_headers:
            return self._response(
                HTTPStatus.NOT_FOUND,
                f"{self.WEBSOCKET_SERVER_IDENTIFIER_HEADER} header is not present",
            )
        return None

    def _is_system_message_socket(self, request_headers):
        if (
            self.SYSTEM_MESSAGE_SOCKET_HEADER in request_headers
            and request_headers.get(self.SYSTEM_MESSAGE_SOCKET_HEADER)
        ):
            return True
        return False

    @staticmethod
    def _response(status, message):
        LOGGER.info(message)
        return status, [], message

    @property
    def context(self):
        return self.ws_server.context
