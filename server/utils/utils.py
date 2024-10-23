import decimal
import json
import logging

from aiohttp import ClientConnectionError

from server.settings.settings import CHAT_API_URL
from server.utils.exceptions import CustomException

LOGGER = logging.getLogger("Server.Utils")


class MessageType:
    ROUTABLE = "ROUTABLE"
    ERROR = "ERROR"
    GET_HISTORY = "GET_HISTORY"
    SET_LAST_MESSAGE_READ = "SET_LAST_MESSAGE_READ"
    GET_LAST_MESSAGES_READ = "GET_LAST_MESSAGES_READ"
    ADD_APP_USER_WEBSOCKET = "ADD_APP_USER_WEBSOCKET"
    REMOVE_APP_USER_WEBSOCKET = "REMOVE_APP_USER_WEBSOCKET"
    FULL_SYNC = "FULL_SYNC"
    SERVER_MODE = "SERVER_MODE"
    OFFLINE_NOTIFICATION = "OFFLINE_NOTIFICATION"
    GET_LAST_CHAT_ROOM_MESSAGE = "GET_LAST_CHAT_ROOM_MESSAGE"
    GET_UNREAD_MESSAGES_COUNT = "GET_UNREAD_MESSAGES_COUNT"
    SYSTEM_ROUTABLE = "SYSTEM_ROUTABLE"


class ChatRoomType:
    REGULAR = 1
    MASS_PUBLIC = 2
    MASS_PRIVATE = 3


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


async def send_error(custom_exception: CustomException, websocket):
    payload = {"type": MessageType.ERROR, "exception": custom_exception.get_message()}
    await websocket.send(json.dumps(payload))


async def make_server_call(session, url, method="get", **kwargs):
    try:
        async with getattr(session, method)(url, **kwargs) as resp:
            if resp.status == 500 or resp.status == 502:
                LOGGER.error(
                    f"Internal server error (HTTP {resp.status}) in the Chat API service {url}"
                )
                return []

            response = await resp.json()
            if resp.status != 200:
                LOGGER.warning(
                    f"Error (HTTP {resp.status}) in the Chat API service {url}: {response}"
                )
                return []
            return response
    except ClientConnectionError:
        LOGGER.warning(f"Cannot connect to the Chat API service at url: {url}")
        return []
    except Exception as e:
        LOGGER.exception(
            f"Unknown exception in the Chat API service at url: {url}, message: {str(e)}"
        )
        return []


def prepare_server_url(path):
    return f"{CHAT_API_URL}/internal-server-to-server/v1/{path}"
