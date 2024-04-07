import traceback
import logging
import os
from typing import Type, NoReturn

from CategoriGen.tools.directory_creators import check_logs_directory


def log_and_raise_error(logger: logging.Logger,
                        level: str,
                        error_type: Type[BaseException],
                        message: str,
                        include_traceback: bool = False) -> NoReturn:
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
    if include_traceback:  # Include detailed traceback information in the log if specified
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


class LogBlock:
    """Context manager for 'with' blocks, logging the start and end with a specified process name"""
    def __init__(self, process_name: str, logger: logging.Logger):
        self.process_name: str = process_name
        self.logger: logging.Logger = logger

    def __enter__(self):  # What happens at the start of the 'with' block
        self.logger.info(f"Started: {self.process_name}")

    def __exit__(self, exc_type, exc_value, exc_tb):  # What happens at the end
        self.logger.info(f"Finished: {self.process_name}")


def check_logger_exists(existing_logger):
    """Checks if a logger with the provided name exists"""
    existing_loggers = logging.Logger.manager.loggerDict.keys()
    return existing_logger in existing_loggers  # To be evaluated as True if it exists at all


def exception_logger(logger, include_traceback=False):  # At this level the function is a "decorator factory"
    """Decorator function to automatically log any errors that are not explicitly handled elsewhere"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)  # Try the function that was passed in
            except Exception as error:
                # Handled exceptions should have the name 'HandledError' (see log_and_raise_error())
                # So if the exception has this name, just re-raise it - it will already have logging
                if type(error).__name__ == 'HandledError':
                    raise
                # Otherwise, log the unhandled error as critical and then re_raise
                else:
                    # Conditionally log a more detailed message with the error traceback appended
                    if include_traceback:
                        error_message = f"{type(error).__name__}\n{traceback.format_exc()}#ENDOFLOG#"
                    else:
                        error_message = str(error)
                    # Log the message as CRITICAL and re-raise
                    logger.critical(error_message)
                    raise
        return wrapper
    return decorator


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
        self.format_string: str = f'%(asctime)s | %(levelname)s | %(name)s |  %(message)s'

        # Check if the log directory exists (returns Tuple[bool, str, str])
        log_dir_exists, log_message, log_directory = check_logs_directory()

        # Define the full path of the log file
        self.log_file_path = os.path.join(log_directory, self.log_file_name)

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


class BaseWorkflowLogger(BaseLogger):
    def __init__(self):
        super().__init__(name='base_workflow',
                         file_log_level=shared_file_log_level,
                         console_log_level=shared_console_log_level)


class UnhandledErrorsLogger(BaseLogger):
    def __init__(self):
        super().__init__(name='unhandled_errors',
                         file_log_level=shared_file_log_level,
                         console_log_level=shared_console_log_level)
