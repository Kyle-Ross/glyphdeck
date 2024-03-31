import logging
import os


def check_logger_exists(logger_name):
    """Checks if a logger with the provided name exists"""
    existing_loggers = logging.Logger.manager.loggerDict.keys()
    return logger_name in existing_loggers


def core_logger_setup():
    """Initialises, configures and returns the core logger object for the project. To be imported in other files."""
    logger_name = 'core'

    # Check if the logger already exists, if it does, return it and skip the rest of the function
    if check_logger_exists(logger_name):
        return logging.getLogger(logger_name)

    # Otherwise, set up the logger then return it

    # Setting the path of the log file relative to this script location
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = 'logs'
    log_file = 'core.log'
    log_file_path = os.path.join(parent_dir, log_dir, log_file)

    # Set levels - 1# DEBUG | 2# INFO | 3# WARNING | #4 ERROR | #5 CRITICAL
    file_log_level = logging.INFO
    console_log_level = logging.INFO

    # Initialising the logger and naming it
    logger = logging.getLogger(logger_name)

    # Set logger level to the lowest level between file and console
    # This is because handlers can only access levels at or above the level of the logger
    logger.setLevel(min(file_log_level, console_log_level))

    # Shared format of all logs
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(module)s | %(levelname)s | %(message)s')

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


def assert_and_log_errors(logger, level, condition: bool, message: str):
    """Asserts a condition but logs the error. Raises the AssertionError if the level is 'error'"""
    levels = ['error', 'warning']
    level_clean = level.lower()
    assert level_clean in levels, f"Provided level '{level_clean}' is not in list of allowed arguments {levels}"
    try:
        # Adds module name here since it isn't recorded correctly at the logger level on import
        assert condition, message
    except AssertionError as e:
        error = str(e)
        if level == 'error':
            logger.error(error)
            raise
        if level == 'warning':
            logger.warning(error)
            raise
