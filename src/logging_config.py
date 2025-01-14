import logging
from logging.handlers import RotatingFileHandler
import os
import datetime
import sys
def setup_logging(script_name, level=logging.DEBUG):
    # Create log directory if it doesn't exist
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Set up log file path
    log_file = os.path.join(log_dir, f'{script_name}.log')

    # Create a logger with the desired settings
    logger = logging.getLogger(script_name)
    logger.propagate = False  # Prevent duplicate logs by disabling propagation
    logger.setLevel(logging.INFO)


    # Ensure no duplicate handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Define a common log format without milliseconds
    log_format = '%(asctime)s %(levelname)s %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # Create a file handler to save logs
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter(log_format, datefmt=date_format)
    )

    # Create a stream handler to log to stdout (for real-time streaming)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(
        logging.Formatter(log_format, datefmt=date_format)
    )

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger