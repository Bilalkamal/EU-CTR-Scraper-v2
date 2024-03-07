# app/utils/logger.py

import logging
import os
from datetime import datetime
from pathlib import Path


def setup_logging():
    """
    Sets up logging for the application.

    This function creates a log directory if it doesn't exist and sets up a log file with the current date as the filename.
    The log file will contain log messages with the format: "<timestamp> <log_level>: <message>".

    Args:
        None

    Returns:
        None
    """

    log_directory = Path(__file__).resolve().parent.parent.parent / 'logs'

    os.makedirs(log_directory, exist_ok=True)
    log_filename = os.path.join(
        log_directory, datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + "-run.log")

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=log_filename,
                        filemode='w')

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s\t->\t %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)



