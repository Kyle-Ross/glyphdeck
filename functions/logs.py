from functions.time import time_since_start
from typing import Type, NoReturn
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


def check_logger_exists(logger_name):
    """Checks if a logger with the provided name exists"""
    existing_loggers = logging.Logger.manager.loggerDict.keys()
    return logger_name in existing_loggers


class BaseLogger:
    def __init__(self,
                 name: str,
                 file_log_level: int,
                 console_log_level: int,
                 log_file: str = 'base.log'):
        self.name: str = name
        self.file_log_level: int = file_log_level
        self.console_log_level: int = console_log_level
        self.log_file: str = log_file
        self.format_string: str = f'%(asctime)s | {time_since_start()} | %(levelname)s | %(name)s |  %(message)s'

    def check_logger_exists(self):
        """Checks if a logger with the provided name exists"""
        existing_loggers = logging.Logger.manager.loggerDict.keys()
        return self.name in existing_loggers

    def setup(self):
        """Initialises, configures and returns the logger object."""
        # Check if the logger already exists, if it does, return it and skip the rest of the function
        if self.check_logger_exists():
            return logging.getLogger(self.name)

        # Otherwise, set up the logger then return it

        # Setting the path of the log file relative to this script location
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_dir = 'logs'
        log_file_path = os.path.join(parent_dir, log_dir, self.log_file)

        # Initialising the logger and naming it
        logger = logging.getLogger(self.name)

        # Set logger level to the lowest level between file and console
        # This is because handlers can only access levels at or above the level of the logger
        logger.setLevel(min(self.file_log_level, self.console_log_level))

        # Get the formatter for this logger
        formatter = logging.Formatter(self.format_string)

        # File log handler
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(self.file_log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console log handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.console_log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Return the configured logger object
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


class TypeModelsLogger(BaseLogger):
    def __init__(self):
        super().__init__(name='type_models',
                         file_log_level=shared_file_log_level,
                         console_log_level=shared_console_log_level)


class HandlerLogger(BaseLogger):
    def __init__(self):
        super().__init__(name='Handler',
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
