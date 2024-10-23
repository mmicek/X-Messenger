import json
import logging
from pyfcm import FCMNotification

LOGGER = logging.getLogger("Server.FirebaseClient")


class FirebaseClient:

    def __init__(self, context):
        self.context = context
        self.application_fcm_dict = (
            {}
        )  # Application identifier -> FCMNotification (object)

    @staticmethod
    def send_fcm_notifications(application_fcm_dict, device_message_dict):
        LOGGER.info("Execution fcm notifications sync task")
        for device_fcm_token, (
            message,
            application_identifier,
        ) in device_message_dict.items():
            try:
                fcm = application_fcm_dict.get(application_identifier, None)
                if not fcm:
                    LOGGER.debug(
                        f"Missing fcm token for application {application_identifier}"
                    )
                    continue
                result = fcm.notify_single_device(
                    registration_id=device_fcm_token,
                    message_title="New message from in chat room",
                    message_body=f"{message['message']}",
                    data_message=message,
                )

                LOGGER.debug(
                    f"Sending message to device with fcm_token: {device_fcm_token}"
                )
                if not result["success"] and result["failure"]:
                    # TODO What if fcm notification fail? Do we want to remove session from db?
                    error_message = json.dumps(result["results"])
                    LOGGER.error(f"Sending fcm notification failure: {error_message}")

            except Exception as e:
                LOGGER.error(f"Exception while sending fcm notification: {str(e)}")

    def reset_application_fcm_dict(self):
        application_fcm_dict = {}
        for (
            application,
            settings,
        ) in self.context.applications_service.get_applications_settings().items():
            firebase_server_key = settings.get("firebase_server_key", None)
            if firebase_server_key:
                application_fcm_dict[application] = FCMNotification(
                    api_key=firebase_server_key
                )

        self.application_fcm_dict.clear()
        self.application_fcm_dict = application_fcm_dict

    def get_copied_application_fcm_dict(self):
        return self.application_fcm_dict.copy()
