from datetime import timedelta, datetime
from server.utils.exceptions import MessageSpamException


class AntiSpamMixin:

    MESSAGE_LIMIT_PER_MINUTE = 300

    def init_counter(self):
        self.counter = 0
        self.reset_counter_datetime = datetime.utcnow() + timedelta(minutes=1)

    def check_anti_spam(self):
        datetime_now = datetime.utcnow()
        if datetime_now > self.reset_counter_datetime:
            self.init_counter()

        if (
            self.counter >= self.MESSAGE_LIMIT_PER_MINUTE
            and datetime_now < self.reset_counter_datetime
        ):
            raise MessageSpamException()
        self.counter = self.counter + 1
