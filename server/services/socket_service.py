import logging
from http import HTTPStatus
from typing import Optional
from urllib.parse import urlparse, parse_qs

from server.settings.settings import MANAGER_SECRET

LOGGER = logging.getLogger("Server.SocketService")


class SocketService:
    TOKEN_HEADER = "X-TOKEN"
    TOKEN_PARAMETER = "token"
    MANAGER_HEADER = "X-MANAGER-SECRET"
    TOKEN_ERROR_MESSAGE = "X-TOKEN is invalid or expired. Get a new token."

    def __init__(self, context):
        self.context = context

    def validate_token(
        self, path, request_headers
    ) -> (Optional[object], Optional[tuple]):
        if "/socket" not in path:
            return None, (
                HTTPStatus.NOT_FOUND,
                [],
                "Socket server is listening under /socket path.",
            )
        return self.validate_required_parameter(
            request_headers, path, self.TOKEN_HEADER, self.TOKEN_PARAMETER
        )

    def validate_required_parameter(self, request_headers, path, header, parameter):
        if header not in request_headers:
            token = self.get_from_parameter(path, parameter)
            if not token:
                error_message = (
                    f"{header} header or parameter: {parameter} is not present."
                )
                LOGGER.debug(f"Process_request exception {error_message}")
                return None, (HTTPStatus.NOT_FOUND, [], error_message)
            else:
                return token, None
        else:
            return request_headers[header], None

    async def validate_session(self, token) -> (Optional[object], Optional[tuple]):
        if ":" not in token:
            return None, (HTTPStatus.FORBIDDEN, [], self.TOKEN_ERROR_MESSAGE)
        session = await self.context.dynamodb_service.fetch_session(token)
        if session is None:
            LOGGER.debug(f"Process_request exception {self.TOKEN_ERROR_MESSAGE}")
            return None, (HTTPStatus.FORBIDDEN, [], self.TOKEN_ERROR_MESSAGE)
        else:
            return session, None

    async def manage_user_websocket_connection(self, session, websocket) -> (str, str):
        application_user_identifier = session.application_user_identifier
        device_identifier = session.device_identifier
        LOGGER.debug(
            f"New client connection with identifier {application_user_identifier}"
        )

        if application_user_identifier not in self.context.application_user_device_dict:
            self.context.application_user_device_dict[application_user_identifier] = {}
        self.context.application_user_device_dict[application_user_identifier][
            device_identifier
        ] = websocket

        await self.context.central_router_message_service.send_add_app_user_websocket_message(
            application_user_identifier
        )
        return application_user_identifier, device_identifier

    async def close_user_websocket_connection(self, websocket):
        if (
            websocket.application_user_identifier
            in self.context.application_user_device_dict
        ):
            del self.context.application_user_device_dict[
                websocket.application_user_identifier
            ][websocket.device_identifier]
            if not self.context.application_user_device_dict[
                websocket.application_user_identifier
            ]:
                await self.context.central_router_message_service.send_remove_app_user_websocket_message(
                    websocket.application_user_identifier
                )
                del self.context.application_user_device_dict[
                    websocket.application_user_identifier
                ]

    async def validate_manager_header(self, request_headers):
        if self.MANAGER_HEADER not in request_headers:
            return False
        if request_headers[self.MANAGER_HEADER] == MANAGER_SECRET:
            return True
        return False

    @staticmethod
    def get_from_parameter(path, header):
        parameters = parse_qs(urlparse(path).query)
        if header not in parameters:
            return None
        return parameters[header][0]
