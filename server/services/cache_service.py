from datetime import datetime, timedelta


class CustomDataCacheService:

    def __init__(self, context):
        self.context = context
        self.identifier_custom_data_dict = (
            {}
        )  # Identifier of a app user -> { 'expiry_date', 'custom_data' }

    async def get_custom_data(self, app_user_identifier, cache_time_sec=60 * 60):
        if (
            app_user_identifier not in self.identifier_custom_data_dict
            or self._cache_expired(app_user_identifier)
        ):
            custom_data = await self.context.dynamodb_service.fetch_custom_data(
                app_user_identifier
            )
            self.identifier_custom_data_dict[app_user_identifier] = {
                "custom_data": custom_data,
                "expiry_datetime": datetime.utcnow()
                + timedelta(seconds=cache_time_sec),
            }
        return self.identifier_custom_data_dict[app_user_identifier]["custom_data"]

    def _cache_expired(self, app_user_identifier):
        expiry_datetime = self.identifier_custom_data_dict[app_user_identifier][
            "expiry_datetime"
        ]
        return datetime.utcnow() > expiry_datetime


class DeviceFcmTokenCacheService:

    def __init__(self, context):
        self.context = context
        self.user_identifier_fcm_tokens_dict = {}

    async def get_user_identifier_fcm_tokens(
        self, app_user_identifier, cache_time_sec=60 * 60 * 12
    ):
        if (
            app_user_identifier not in self.user_identifier_fcm_tokens_dict
            or self._cache_expired(app_user_identifier)
        ):
            fcm_tokens = await self.context.dynamodb_service.fetch_device_fcm_tokens(
                app_user_identifier
            )
            application_identifier = app_user_identifier.split(":")[1]

            self.user_identifier_fcm_tokens_dict[app_user_identifier] = {
                "fcm_tokens": fcm_tokens,
                "application_identifier": application_identifier,
                "expiry_datetime": datetime.utcnow()
                + timedelta(seconds=cache_time_sec),
            }
        return (
            self.user_identifier_fcm_tokens_dict[app_user_identifier]["fcm_tokens"],
            self.user_identifier_fcm_tokens_dict[app_user_identifier][
                "application_identifier"
            ],
        )

    def _cache_expired(self, app_user_identifier):
        expiry_datetime = self.user_identifier_fcm_tokens_dict[app_user_identifier][
            "expiry_datetime"
        ]
        return datetime.utcnow() > expiry_datetime
