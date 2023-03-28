import logging


class Logger:
    def __init__(self, log_file_path):
        self.logger = logging.getLogger("WeatherLogger")
        self.logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def log_info(self, message):
        self.logger.info(message)

    def log_exception(self, exception, message=None):
        if message is not None:
            self.logger.error(message)
        self.logger.exception(exception)
