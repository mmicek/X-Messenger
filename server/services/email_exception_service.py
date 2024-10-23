import asyncio
import json
import logging
import smtplib
from datetime import datetime, timedelta

from server.settings.settings import (
    ADMINS,
    EMAIL_HOST,
    EMAIL_HOST_PASSWORD,
    EMAIL_HOST_USER,
    EMAIL_PORT,
)

LOGGER = logging.getLogger("Server.EmailService")


class EmailExceptionService:
    EMAIL_EXCEPTION_TIMEOUT_SEC = 60 * 60

    def __init__(self, context):
        self.context = context
        self.exception_dict = {}

    async def notify_admin(
        self, exception: Exception, stack_trace, additional_data=None
    ):
        if ADMINS:
            exception_class = type(exception).__name__
            if (
                exception_class not in self.exception_dict
                or datetime.utcnow() > self.exception_dict[exception_class]
            ):
                await asyncio.get_event_loop().run_in_executor(
                    None, self.send_email, exception, stack_trace, additional_data
                )
                self.exception_dict[exception_class] = datetime.utcnow() + timedelta(
                    seconds=self.EMAIL_EXCEPTION_TIMEOUT_SEC
                )

    @staticmethod
    def send_email(exception, stack_trace, additional_data):
        message_data = {"exception": str(exception), "additional_data": additional_data}
        exception_class = type(exception).__name__
        subject = f"[{exception_class}] error in Websocket Server"

        connection = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)

        message_text = "Subject: {}\n\n{}\n\n{}".format(
            subject, json.dumps(message_data), stack_trace
        )

        connection.starttls(keyfile=None, certfile=None)
        connection.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
        connection.sendmail(EMAIL_HOST_USER, ADMINS, message_text)

        LOGGER.info(f"Sending email to admins: {ADMINS} with {exception_class}")