import logging
import os
import sys
from datetime import datetime

from server.settings import settings


class InitializationService:

    @classmethod
    def init(cls):
        cls.set_up_logger()

    @classmethod
    def set_up_logger(cls):
        root = logging.getLogger("Server")
        if settings.DEBUG:
            root.setLevel(logging.DEBUG)
        else:
            root.setLevel(logging.INFO)

        if not settings.LOG_FILE_DIRECTORY:
            handler = logging.StreamHandler(sys.stdout)
        else:
            cls.clear_log_directory()
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

    @classmethod
    def clear_log_directory(cls):
        for filename in os.listdir(settings.LOG_FILE_DIRECTORY):
            if ".log" in filename:
                file_path = os.path.join(settings.LOG_FILE_DIRECTORY, filename)
                if (
                    datetime.fromtimestamp(os.path.getctime(file_path)).date()
                    != datetime.now().date()
                ):
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
