import logging
from logging.handlers import RotatingFileHandler
import os
import datetime
import sys
def setup_logging(script_name, level=logging.DEBUG):

    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logtime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # log_file = os.path.join(log_dir, f'{script_name}{logtime}.log')
    log_file = os.path.join(log_dir, f'{script_name}.log')
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)  # Explicitly log to stdout
        ]
    )

    logger = logging.getLogger(script_name)
    #
    # log_dir = 'logs'
    # if not os.path.exists(log_dir):
    #     os.makedirs(log_dir)
    # log_file = os.path.join(log_dir, f'{script_name}.log')
    #
    # # Create a new logger for each script
    # logger = logging.getLogger(script_name)
    #
    # # Prevent adding handlers multiple times
    # if not logger.hasHandlers():
    #     # Set level
    #     logger.setLevel(level)
    #
    #     # Create handlers
    #     file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    #     stream_handler = logging.StreamHandler()
    #
    #     # Create formatter and add it to handlers
    #     formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    #     file_handler.setFormatter(formatter)
    #     stream_handler.setFormatter(formatter)
    #
    #     # Add handlers to the logger
    #     logger.addHandler(file_handler)
    #     logger.addHandler(stream_handler)
    return logger