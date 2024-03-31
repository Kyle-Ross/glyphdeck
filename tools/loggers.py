from tools.time import time_since_start
from typing import Type, NoReturn
from constants import OUTPUT_LOGS_DIR
import traceback
import logging
import os


def log_and_raise_error(logger: logging.Logger,
                        level: str,
                        error_type: Type[BaseException],
                        message: str,
                        traceback_message: bool = False) -> NoReturn:
    """Logs and raises an error with the same message string. Wraps in custom error to indicate this is an error that
    was handled. Later this will prevent it being re-raised as an unhandled error"""

    class HandledError(error_type):
        pass

    # Check the provided level arguments
    allowed_levels = ('warning', 'error', 'critical')
    try:
        assert level.lower() in allowed_levels, f"AssertionError - Level argument {level} " \
                                                f"is not one of the allowed levels {allowed_levels}"
    except AssertionError as error:
        logger.error(error)
        raise HandledError(error)

    # Build the log / error message
    error_message = f"{error_type.__name__} - {message}"
    if traceback_message:  # Include detailed traceback information in the log if specified
        error_message = f"{error_message}\n{traceback.format_exc()}#ENDOFLOG#"

    # Log the message at the specified level and re-raise the error
    if level == 'warning':
        logger.warning(error_message)
        raise HandledError(error_message)
    if level == 'error':
        logger.error(error_message)
        raise HandledError(error_message)
    if level == 'critical':
        logger.critical(error_message)
        raise HandledError(error_message)


def assert_and_log_error(logger: logging.Logger,
                         level: str,
                         condition: bool,
                         message: str,
                         traceback_message: bool = False) -> NoReturn:
    """Asserts a condition and logs the specified error"""
    if not condition:
        log_and_raise_error(logger, level, AssertionError, message, traceback_message)


def check_logger_exists(existing_logger):
    """Checks if a logger with the provided name exists"""
    existing_loggers = logging.Logger.manager.loggerDict.keys()
    return existing_logger in existing_loggers  # To be evaluated as True if it exists at all


def logger_setup(name: str,
                 format_string: str,
                 file_log_level: int,
                 console_log_level: int,
                 log_file_path: str) -> logging.Logger:
    """Initialises, configures and returns the logger object if it doesn't exist yet"""
    # Check if the logger already exists, if it does, return it and skip the rest of the function
    if check_logger_exists(name):
        return logging.getLogger(name)

    # Otherwise, set up the logger then return it

    # Initialising the logger and naming it
    logger = logging.getLogger(name)

    # Set logger level to the lowest level between file and console
    # This is because handlers can only access levels at or above the level of the logger
    logger.setLevel(min(file_log_level, console_log_level))

    # Get the formatter for this logger
    formatter = logging.Formatter(format_string)

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


class BaseLogger:
    def __init__(self,
                 name: str,
                 file_log_level: int,
                 console_log_level: int,
                 log_file_name: str = 'base.log'):
        self.name: str = name
        self.file_log_level: int = file_log_level
        self.console_log_level: int = console_log_level
        self.log_file_name: str = log_file_name
        self.log_file_path = os.path.join(OUTPUT_LOGS_DIR, self.log_file_name)
        self.format_string: str = f'%(asctime)s | {time_since_start()} | %(levelname)s | %(name)s |  %(message)s'

        # Check if the log directory exists and create it if it doesn't
        if not os.path.exists(OUTPUT_LOGS_DIR):
            os.makedirs(OUTPUT_LOGS_DIR)  # Create the directory
            log_dir_exists = False
            log_message = f"'{OUTPUT_LOGS_DIR}' - Logs folder created"
        else:
            log_dir_exists = True
            log_message = f"'{OUTPUT_LOGS_DIR}' - Logs folder exists"

        # Create logger for just for logging inside the logger
        logging_logger = logger_setup('logging_logger',
                                      self.format_string,
                                      self.file_log_level,
                                      self.console_log_level,
                                      self.log_file_path)

        # Log the results of the log directory check using the logging_logger
        if log_dir_exists:
            logging_logger.debug(log_message)
        if not log_dir_exists:
            logging_logger.info(log_message)

    def setup(self) -> logging.Logger:
        """Sets up the logger"""
        logger = logger_setup(self.name,
                              self.format_string,
                              self.file_log_level,
                              self.console_log_level,
                              self.log_file_path)
        return logger


# Shared logging levels - edit in each class for more granular control
# This sets the minimum level of logging each logger will save to the file or print to the console
# Levels - 0 NOTSET | 10 DEBUG | 20 INFO | 30 WARNING | 40 ERROR | 50 CRITICAL - enter like logging.INFO
shared_file_log_level = logging.INFO
shared_console_log_level = logging.INFO


# Loggers inheriting from the base logger with their own name and level controls
class DataTypesLogger(BaseLogger):
    def __init__(self):
        super().__init__(name='data_types',
                         file_log_level=shared_file_log_level,
                         console_log_level=shared_console_log_level)


class PrepperLogger(BaseLogger):
    def __init__(self):
        super().__init__(name='Prepper',
                         file_log_level=shared_file_log_level,
                         console_log_level=shared_console_log_level)


class ChainLogger(BaseLogger):
    def __init__(self):
        super().__init__(name='Chain',
                         file_log_level=shared_file_log_level,
                         console_log_level=shared_console_log_level)


class SanitiserLogger(BaseLogger):
    def __init__(self):
        super().__init__(name='Sanitiser',
                         file_log_level=shared_file_log_level,
                         console_log_level=shared_console_log_level)


class ValidatorsLogger(BaseLogger):
    def __init__(self):
        super().__init__(name='validators_models',
                         file_log_level=shared_file_log_level,
                         console_log_level=shared_console_log_level)


class LLMHandlerLogger(BaseLogger):
    def __init__(self):
        super().__init__(name='LLMHandler',
                         file_log_level=shared_file_log_level,
                         console_log_level=shared_console_log_level)


class CacheLogger(BaseLogger):
    def __init__(self):
        super().__init__(name='cache',
                         file_log_level=shared_file_log_level,
                         console_log_level=shared_console_log_level)


class BaseScriptLogger(BaseLogger):
    def __init__(self):
        super().__init__(name='base_script',
                         file_log_level=shared_file_log_level,
                         console_log_level=shared_console_log_level)


class UnhandledErrorsLogger(BaseLogger):
    def __init__(self):
        super().__init__(name='unhandled_errors',
                         file_log_level=shared_file_log_level,
                         console_log_level=shared_console_log_level)
