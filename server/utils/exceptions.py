import json
from decimal import Decimal


class CustomException(Exception):
    message = None
    error_code = None

    def get_message(self):
        assert self.message
        assert self.error_code
        return {
            "message": self.message,
            "error_code": self.error_code,
            "extra": self.get_extra(),
        }

    @staticmethod
    def get_extra():
        return None

    def __str__(self):
        return json.dumps(self.get_message())


class ChatServerException(CustomException):
    message = "Exception in chat websocket server."
    error_code = 10000

    def __init__(self, class_name, exception):
        self.class_name = class_name
        self.exception = exception

    def get_extra(self):
        return {"class_name": self.class_name, "exception": str(self.exception)}


class UserNotInChatRoomException(CustomException):
    message = "User does not belong to this chat room."
    error_code = 10001

    def __init__(self, chat_room_identifier, application_user_identifier):
        self.chat_room_identifier = chat_room_identifier
        self.application_user_identifier = application_user_identifier

    def get_extra(self):
        return {
            "chat_room_identifier": self.chat_room_identifier,
            "application_user_identifier": self.application_user_identifier,
        }


class WrongMessageTypeException(CustomException):
    message = "Message must be string type."
    error_code = 10002


class ChatRoomIdentifiersListLengthException(CustomException):
    message = "Length of chat_room_identifiers list cant be grater than 10."
    error_code = 10003


class InvalidMessageFormat(CustomException):
    message = "Invalid message format: Must be a dictionary with proper fields."
    error_code = 10004


class MissingRequiredFieldException(CustomException):
    message = "Missing required field."
    error_code = 10005

    def __init__(self, field_name):
        self.field_name = field_name

    def get_extra(self):
        return {"field_name": self.field_name}


class DnsConnectionsException(CustomException):
    message = "Central router is not connected. Ignoring message."
    error_code = 10006


class MessageSpamException(CustomException):
    message = "Message spam detected: the rate exceeded 300 messages per minute. Server will close the socket."
    error_code = 10007


class InvalidChatRoomMessageTypeException(CustomException):
    message = "Invalid message type for chat room. See details."
    error_code = 10008

    def __init__(self, chat_room_type: Decimal, method_type):
        self.chat_room_type = int(chat_room_type)
        self.method_type = method_type

    def get_extra(self):
        return {"chat_room_type": self.chat_room_type, "method_type": self.method_type}


class ChatRoomDoesNotExistsException(CustomException):
    message = "Chat room does not exists."
    error_code = 10009
