import logging
import time
import uuid
import aioboto3
from aiohttp import ClientError
from boto3.dynamodb.conditions import Key

from server.settings.settings import (
    AWS_DEFAULT_REGION,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    MAX_DYNAMO_MESSAGE_LIMIT,
)

LOGGER = logging.getLogger("Server.DynamodbService")


class DynamodbOperationType:
    READ = "READ"
    WRITE = "WRITE"


class DynamodbSession:

    def __init__(self, app_user_identifier, device_identifier):
        self.application_user_identifier = app_user_identifier
        self.device_identifier = device_identifier


class DynamodbService:

    def __init__(
        self,
        context,
        session_table_name,
        chat_room_table_name,
        chat_message_table_name,
        last_message_read_table_name,
        user_identifier_custom_data_table_name,
    ):
        self.context = context
        self.session_table_name = session_table_name
        self.chat_room_table_name = chat_room_table_name
        self.chat_message_table_name = chat_message_table_name
        self.last_message_read_table_name = last_message_read_table_name
        self.user_identifier_custom_data_table_name = (
            user_identifier_custom_data_table_name
        )

        self.retry_connection = True

    async def connect(self):
        if self.retry_connection:
            self.retry_connection = False
            resource = aioboto3.Session().resource(
                "dynamodb",
                region_name=AWS_DEFAULT_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            )
            # Need to call __aenter__ method as we do not want to close client connection in __aexit__ method
            # But we need to go out from 'with' block
            client = await resource.__aenter__()

            self.session_table = await client.Table(self.session_table_name)
            self.chat_room_table = await client.Table(self.chat_room_table_name)
            self.chat_message_table = await client.Table(self.chat_message_table_name)
            self.last_message_read_table = await client.Table(
                self.last_message_read_table_name
            )
            self.user_identifier_custom_data = await client.Table(
                self.user_identifier_custom_data_table_name
            )

            self.client = client
            LOGGER.debug("Successfully connected with dynamodb database.")

    async def fetch_session(self, token):
        session = await self._query(
            self.session_table,
            key_condition_expression=Key("token").eq(token),
            index="token-index",
        )
        if not session:
            return None
        return DynamodbSession(
            session[0]["app_user_identifier"], session[0]["device_identifier"]
        )

    async def fetch_custom_data(self, application_user_identifier):
        custom_data = await self._query(
            self.user_identifier_custom_data,
            key_condition_expression=Key("app_user_identifier").eq(
                application_user_identifier
            ),
            index="app_user_identifier-index",
        )
        if not custom_data:
            return None
        return custom_data[0]["custom_data"]

    async def fetch_device_fcm_tokens(self, application_user_identifier):
        device_data = await self._query(
            self.session_table,
            key_condition_expression=Key("app_user_identifier").eq(
                application_user_identifier
            ),
            index="app_user_identifier-index",
        )
        if not device_data:
            return []
        return [e["fcm_token"] for e in device_data]

    async def fetch_chat_room(self, chat_room_identifier):
        chat_room = await self._query(
            self.chat_room_table,
            key_condition_expression=Key("identifier").eq(chat_room_identifier),
            index="identifier-index",
        )
        if not chat_room:
            return []
        return chat_room[0]

    async def fetch_chat_room_messages(
        self, chat_room_identifier, from_message_identifier, limit
    ):
        return await self._query(
            self.chat_message_table,
            key_condition_expression=(
                Key("chat_room_identifier").eq(chat_room_identifier)
                & Key("message_timestamp_identifier").lt(from_message_identifier)
            ),
            index="chat_room_identifier-message_timestamp_identifier-index",
            limit=limit,
            scan_index_forward=False,
        )

    async def fetch_read_message_users(
        self, chat_room_identifier, message_timestamp_identifier
    ):
        return await self._query(
            self.last_message_read_table,
            key_condition_expression=(
                Key("chat_room_identifier").eq(chat_room_identifier)
                & Key("message_timestamp_identifier").eq(message_timestamp_identifier)
            ),
            index="chat_room_identifier-message_timestamp_identifier-index",
        )

    async def update_last_message_read(
        self, chat_room_identifier, application_user_identifier, message_identifier
    ):
        last_messages_read = await self.fetch_last_messages_read(chat_room_identifier)
        for message in last_messages_read:
            if message.get("app_user_identifier", None) == application_user_identifier:
                await self._delete_item(
                    self.last_message_read_table, {"identifier": message["identifier"]}
                )
        await self.create_last_message_read(
            chat_room_identifier, application_user_identifier, message_identifier
        )

    async def fetch_last_messages_read(self, chat_room_identifier):
        return await self._query(
            self.last_message_read_table,
            key_condition_expression=(
                Key("chat_room_identifier").eq(chat_room_identifier)
            ),
            index="chat_room_identifier-index",
        )

    async def create_chat_message(
        self, chat_room_identifier, application_user_identifier, message
    ):
        message_timestamp_identifier = time.time_ns()  # Unix time in nanoseconds (!)
        data = {
            "chat_room_identifier": chat_room_identifier,
            "message": message,
            "message_timestamp_identifier": message_timestamp_identifier,
            "app_user_identifier": application_user_identifier,
            "is_system_message": False,
        }
        await self._put_item(self.chat_message_table, data)
        return message_timestamp_identifier

    async def create_system_message(self, chat_room_identifier, message):
        message_timestamp_identifier = time.time_ns()  # Unix time in nanoseconds (!)
        data = {
            "chat_room_identifier": chat_room_identifier,
            "message": message,
            "message_timestamp_identifier": message_timestamp_identifier,
            "is_system_message": True,
        }
        await self._put_item(self.chat_message_table, data)
        return message_timestamp_identifier

    async def create_last_message_read(
        self, chat_room_identifier, application_user_identifier, message_identifier
    ):
        data = {
            "identifier": str(uuid.uuid4()),
            "message_timestamp_identifier": message_identifier,
            "chat_room_identifier": chat_room_identifier,
            "app_user_identifier": application_user_identifier,
        }
        await self._put_item(self.last_message_read_table, data)

    async def _query(
        self,
        table,
        key_condition_expression,
        index,
        limit=None,
        scan_index_forward=True,
    ):
        if not key_condition_expression:
            return []
        await self.connect()
        if limit is not None and limit > MAX_DYNAMO_MESSAGE_LIMIT:
            limit = MAX_DYNAMO_MESSAGE_LIMIT

        try:
            if limit:
                items = (
                    await table.query(
                        KeyConditionExpression=key_condition_expression,
                        IndexName=index,
                        Limit=limit,
                        ScanIndexForward=scan_index_forward,
                    )
                )["Items"]
            else:
                items = (
                    await table.query(
                        KeyConditionExpression=key_condition_expression, IndexName=index
                    )
                )["Items"]
            self.context.dynamodb_performance_service.update_counter(
                table.table_name, DynamodbOperationType.READ, index=index
            )
            if items:
                return items
            return []
        except ClientError as e:
            self.context.dynamodb_performance_service.update_counter(
                table.table_name, DynamodbOperationType.READ, is_error=True, index=index
            )
            self.retry_connection = True
            LOGGER.error(f"Lost connection with dynamodb: {str(e)}")

    async def _put_item(self, table, item):
        await self.connect()
        try:
            item = await table.put_item(Item=item)
            self.context.dynamodb_performance_service.update_counter(
                table.table_name, DynamodbOperationType.WRITE
            )
            return item
        except ClientError as e:
            self.retry_connection = True
            self.context.dynamodb_performance_service.update_counter(
                table.table_name, DynamodbOperationType.WRITE, is_error=True
            )
            LOGGER.error(f"Lost connection with dynamodb: {str(e)}")

    async def _delete_item(self, table, key):
        await self.connect()
        try:
            item = await table.delete_item(Key=key)
            self.context.dynamodb_performance_service.update_counter(
                table.table_name, DynamodbOperationType.READ
            )
            return item
        except ClientError as e:
            self.context.dynamodb_performance_service.update_counter(
                table.table_name, DynamodbOperationType.WRITE, is_error=True
            )
            self.retry_connection = True
            LOGGER.error(f"Lost connection with dynamodb: {str(e)}")
