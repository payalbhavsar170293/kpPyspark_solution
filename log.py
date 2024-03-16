import logging
from logging.handlers import RotatingFileHandler


def configure_logger(log_file):
    """
    configure logger to write log messages to a file with a rotating policy
    :param log_file: path of the log file
    :return: None
    """
    # Configure logging to file with rotating policy
    log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    log_handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024, backupCount=3)
    log_handler.setFormatter(log_formatter)
    logger = logging.getLogger()
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)
