import json

from server.utils.exceptions import MissingRequiredFieldException


class ManagerMessageType:
    CONNECTED_USERS_INFO = "CONNECTED_USERS_INFO"


class ManagerMessageHandlerService:

    def __init__(self, context):
        self.context = context
        self.message_type_handlers = {
            ManagerMessageType.CONNECTED_USERS_INFO: self.connected_users_info_message_handler
        }

    async def handle_message(self, message, websocket):
        message_type = message.get("type", None)
        if not message_type:
            raise MissingRequiredFieldException("type")
        return await self.message_type_handlers[message_type](message, websocket)

    async def connected_users_info_message_handler(self, message, websocket):
        data = {
            "counter": len(self.context.application_user_device_dict),
            "identifier": str(self.context.websocket_server_identifier),
            "data": {},
        }
        for (
            application_user,
            devices,
        ) in self.context.application_user_device_dict.items():
            data["data"][application_user] = {
                "devices": [],
                "custom_data": await self.context.custom_data_cache_service.get_custom_data(
                    application_user
                ),
            }
            for device, _ in devices.items():
                data["data"][application_user]["devices"].append(device)
        await websocket.send(json.dumps(data))
