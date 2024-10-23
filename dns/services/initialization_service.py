import os
import logging
import sys

from dns.settings import settings


class InitializationService:

    @classmethod
    def init(cls):
        cls.set_up_logger()

    @classmethod
    def set_up_logger(cls):
        root = logging.getLogger("Dns")
        if settings.DEBUG:
            root.setLevel(logging.DEBUG)
        else:
            root.setLevel(logging.INFO)

        if not settings.LOG_FILE_DIRECTORY:
            handler = logging.StreamHandler(sys.stdout)
        else:
            handler = logging.FileHandler(
                f"{os.path.join(settings.LOG_FILE_DIRECTORY, settings.LOG_FILE_NAME)}.log"
            )
        if settings.DEBUG:
            handler.setLevel(logging.DEBUG)
        else:
            handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        root.addHandler(handler)
