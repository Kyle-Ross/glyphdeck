import logging
import os


def logger_setup():
    """Initialises, configures and returns the core logger object for the project"""
    # Setting the path of the log file relative to this script location
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = 'logs'
    log_file = 'core.log'
    log_file_path = os.path.join(parent_dir, log_dir, log_file)
    # Set levels - 1# DEBUG | 2# INFO | 3# WARNING | #4 ERROR | #5 CRITICAL
    file_log_level = logging.INFO
    console_log_level = logging.INFO

    # Initialising the logger
    logger = logging.getLogger()

    # Set logger level to the lowest level between file and console
    # This is because handlers can only access levels at or above the level of the logger
    logger.setLevel(min(file_log_level, console_log_level))

    # Shared format of all logs
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')

    # File log handler
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(file_log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console log handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Return the configured logger object
    return logger
