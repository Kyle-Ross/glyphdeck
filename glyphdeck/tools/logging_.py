"""Comprehensive logging setup for various components of the application.

Defines classes and functions to create, configure, and manage loggers,
ensuring consistent error handling across different parts of the system.

Classes
-------
**BaseLogger**
    Base class for setting up loggers.

**DataTypesLogger**
    Logger for handling data type validations.

**PrepperLogger**
    Logger for handling prepper tools.

**CascadeLogger**
    Logger for handling processor cascade.

**SanitiserLogger**
    Logger for handling data sanitisation.

**ValidatorsLogger**
    Logger for handling validation models.

**LLMHandlerLogger**
    Logger for handling LLM processing.

**BaseWorkflowLogger**
    Logger for handling workflows.

**CacheLogger**
    Logger for handling caching activities.

**StringsToolsLogger**
    Logger for handling string transforming function actions.

**TimeToolsLogger**
    Logger for handling time related functions.

**FileImportersToolsLogger**
    Logger for handling file importation.


Functions
---------
**log_and_raise_error**
    Logs and raises an error with the same message string.

**assert_and_log_error**
    Asserts a condition and logs the specified error.

**log_decorator**
    Function decorator to log the start and end of a function.

**logger_setup**
    Initializes, configures, and returns a logger instance.

**global_exception_logger**
    Global exception handler that logs uncaught exceptions.

"""

import traceback
import logging
import os
import sys
from typing import Type, Callable, Optional

from glyphdeck.tools.directory_creators import check_logs_directory
import glyphdeck.config.logger_levels as logger_levels


def log_and_raise_error(
    logger_arg: logging.Logger,
    level: str,
    error_type: Type[BaseException],
    message: str,
    include_traceback: bool = False,
):
    """Log and raise an error with the same message string.

    Args:
        logger_arg (logging.Logger): Logger instance to log the error.
        level (str): Level of the log, must be one of 'warning', 'error', or 'critical'.
        error_type (Type[BaseException]): The type of exception to raise.
        message (str): The error message to log and raise.
        include_traceback (bool, optional): Whether to include the traceback in the log. Defaults to False.

    Raises:
        HandledError: The logged error, wrapped in a custom HandledError to indicate it was handled.

    """
    # Later this will prevent it being re-raised as an log level CRITICAL unhandled error

    # This class name is what is check to see if an error is handled
    class HandledError(error_type):
        """Custom exception to indicate the error has been handled.

        Args:
            error_type (Type[BaseException]): The base exception type.

        """

        pass

    # Check the provided level arguments
    allowed_levels = ("warning", "error", "critical")
    try:
        assert level.lower() in allowed_levels, (
            f"AssertionError - Level argument {level} "
            f"is not one of the allowed levels {allowed_levels}"
        )
    except AssertionError as error:
        logger_arg.error(error)
        raise HandledError(error)

    # Build the log / error message
    error_message = f" | Function | log_and_raise_error() | Exit | {message} | {error_type.__name__}"
    if (
        include_traceback
    ):  # Include detailed traceback information in the log if specified
        error_message = (
            f"{error_message} | \\n{traceback.format_exc().replace('\n', '\\n')}"
        )

    # Log the message at the specified level and re-raise the error
    if level == "warning":
        logger_arg.warning(error_message)
        raise HandledError(error_message)
    if level == "error":
        logger_arg.error(error_message)
        raise HandledError(error_message)
    if level == "critical":
        logger_arg.critical(error_message)
        raise HandledError(error_message)


def assert_and_log_error(
    logger_arg: logging.Logger,
    level: str,
    condition: bool,
    message: str,
    include_traceback: bool = False,
):
    """Assert a condition and log the specified error.

    Args:
        logger_arg (logging.Logger): Logger instance to log the error.
        level (str): Level of the log, must be one of 'warning', 'error', or 'critical'.
        condition (bool): Condition to be asserted.
        message (str): The error message to log if the assertion fails.
        include_traceback (bool, optional): Whether to include the traceback in the log. Defaults to False.

    """
    if not condition:
        log_and_raise_error(
            logger_arg, level, AssertionError, message, include_traceback
        )


def _check_logger_exists(logger_name: str) -> bool:
    """Check if a logger with the provided name exists.

    Args:
        logger_name (str): The name of the logger to check.

    Returns:
        bool: True if the logger exists, False otherwise.

    """
    # Checks if a logger_arg with the provided logger_name exists, and returns the name
    existing_loggers = logging.Logger.manager.loggerDict.keys()
    # To be evaluated as True if it exists at all
    return logger_name in existing_loggers


nesting_level = 0


def log_decorator(
    logger_arg: logging.Logger,
    level: str = "debug",
    start: str = "Start",
    finish: str = "Finish",
    suffix_message: Optional[str] = None,
    is_static_method: bool = False,
    is_property: bool = False,
    show_nesting: bool = True,  # Include or exclude the nesting prefix
) -> Callable:
    """Decorate a function to log the start and end of its execution.

    Args:
        logger_arg (logging.Logger): Logger instance to log messages.
        level (str, optional): Log level for both start and finish messages. Defaults to "debug".
        start (str, optional): Start message text. Defaults to "Start".
        finish (str, optional): Finish message text. Defaults to "Finish".
        suffix_message (str, optional): Additional suffix message. Defaults to None.
        is_static_method (bool, optional): Indicates if the decorated function is a static method. Defaults to False.
        is_property (bool, optional): Indicates if the decorated function is a property. Defaults to False.
        show_nesting (bool, optional): Indicates if nesting information should be included. Defaults to True.

    Raises:
        AssertionError: If the provided level is not one of the allowed levels.

    Returns:
        Callable: The decorated function.

    """
    # Set the prefix text
    if is_static_method:
        prefix = "Static method"
    elif is_property:
        prefix = "Property"
    else:
        prefix = "Function"

    levels = ["debug", "info", "warning", "error", "critical"]

    def outer_wrapper(func: Callable) -> Callable:
        # Build the messages
        func_name = func.__name__ + "()"

        start_message_list = [prefix, func_name, start]
        start_message_list = (
            start_message_list + [suffix_message]
            if suffix_message is not None
            else start_message_list
        )
        start_message = " | ".join(start_message_list)

        finish_message_list = [prefix, func_name, finish]
        finish_message_list = (
            finish_message_list + [suffix_message]
            if suffix_message is not None
            else finish_message_list
        )
        finish_message = " | ".join(finish_message_list)

        def inner_wrapper(*args, **kwargs) -> Callable:
            def conditional_log(message: str):
                """Record a log at a level specified by the provided decorator argument.

                Args:
                    message (str): Message to include in the log.

                Raises:
                    AssertionError: If the provided level argument is not in the available levels list.

                """
                # Make a different type of log depending on the provided arguments
                if level == "off":
                    pass
                elif level == "debug":
                    logger_arg.debug(message)
                elif level == "info":
                    logger_arg.info(message)
                elif level == "warning":
                    logger_arg.warning(message)
                elif level == "error":
                    logger_arg.error(message)
                elif level == "critical":
                    logger_arg.critical(message)
                else:
                    error_message = f"Level argument '{level}' is not in available levels list: {levels}"
                    raise AssertionError(error_message)

            # Log before and after the function
            global nesting_level
            nesting_level += 1
            nesting_prefix = f"Nest {nesting_level} | " if show_nesting else " | "
            conditional_log(nesting_prefix + start_message)
            result = func(*args, **kwargs)
            conditional_log(nesting_prefix + finish_message)
            nesting_level -= 1

            return result

        return inner_wrapper

    return outer_wrapper


def logger_setup(
    logger_name: str,
    format_string: str,
    file_log_level: int,
    console_log_level: int,
    log_file_path: str,
) -> logging.Logger:
    """Initialize, configure, and return a logger instance.

    Args:
        logger_name (str): The name of the logger.
        format_string (str): The format string for the log messages.
        file_log_level (int): The log level for the file handler.
        console_log_level (int): The log level for the console handler.
        log_file_path (str): The path to the log file.

    Returns:
        logging.Logger: The configured logger instance.

    """
    # Initialises, configures and returns the logger_arg object if it doesn't exist yet
    # Check if the logger_arg already exists, if it does, return it and skip the rest of the function
    if _check_logger_exists(logger_name):
        return logging.getLogger(logger_name)

    # Otherwise, set up the logger_arg then return it

    # Initialising the logger_arg and naming it
    logger = logging.getLogger(logger_name)

    # Set logger_arg level to the lowest possible level, to allow the handlers to decide the output without interference
    # This is because handlers can only access levels at or above the level of the logger_arg
    # We set it to 1 not 0, since setting it to 0 was resolving to NOTSET, and seemingly setting it to the level of the root logger (30)
    logger.setLevel(1)

    # Get the formatter for this logger_arg
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

    # Return the configured logger_arg object
    return logger


# Define the global exception handler
def global_exception_logger(exctype, value, tb):
    """Global exception handler that logs uncaught exceptions.

    This function is set to `sys.excepthook` to handle uncaught exceptions globally. This is set on any import of `_logging` or `loggers`.
    It skips logging for exceptions of type `HandledError`.

    Args:
        exctype (type): The exception type.
        value (BaseException): The exception instance raised.
        tb (traceback): Traceback object representing the point at which
            the exception was raised.

    """
    # Handled exceptions should have the name 'HandledError'
    if exctype.__name__ == "HandledError":
        return

    # Check if logger is already set up, if not, set it up
    try:
        if not _check_logger_exists("unhandled_errors_logger"):
            logger = logger_setup(
                "unhandled_errors_logger",
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                logging.ERROR,
                logging.ERROR,
                os.path.join(check_logs_directory()[2], "base.log"),
            )

        # Build the log/error message
        formatted_exception = "".join(traceback.format_exception(exctype, value, tb))
        error_message = f" | Function | global_exception_logger() | Exit | | {exctype.__name__} | \\n{formatted_exception.replace('\n', '\\n')}"
        # Log the critical error
        logger.critical(error_message)
    except Exception as handler_error:
        # In case of any issues with logging, print the exception details to stderr as a fallback
        print(f"Error in global_exception_logger: {handler_error}", file=sys.stderr)
        print("Original exception was:", file=sys.stderr)
        traceback.print_exception(exctype, value, tb)

    # Re-raise the error afterwards
    raise value


# Set the global exception handler to sys.excepthook when ever this module is imported
sys.excepthook = global_exception_logger


class BaseLogger:
    """Base class for setting up loggers.

    Attributes:
        logger_name (str): The name of the logger.
        file_log_level (int): Log level for the file handler.
        console_log_level (int): Log level for the console handler.
        log_file_name (str): Name of the log file.
        format_string (str): Format string for log messages.
        log_file_path (str): Full path to the log file.

    """

    def __init__(
        self,
        logger_name: str,
        file_log_level: int,
        console_log_level: int,
        log_file_name: str = "base.log",
    ):
        """Initialize the BaseLogger instance.

        Args:
            logger_name (str): The name of the logger.
            file_log_level (int): The log level for the file handler.
            console_log_level (int): The log level for the console handler.
            log_file_name (str, optional): The name of the log file. Defaults to "base.log".

        """
        self.logger_name: str = logger_name
        self.file_log_level: int = file_log_level
        self.console_log_level: int = console_log_level
        self.log_file_name: str = log_file_name
        self.format_string: str = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

        # Check if the log directory exists (returns Tuple[bool, str, str])
        log_dir_exists, log_message, log_directory = check_logs_directory()

        # Define the full path of the log file
        self.log_file_path = os.path.join(log_directory, self.log_file_name)

        # Create logger_arg for just for logging inside the logger_arg
        logging_logger = logger_setup(
            "logging_logger",
            self.format_string,
            self.file_log_level,
            self.console_log_level,
            self.log_file_path,
        )

        # Log the results of the log directory check using the logging_logger
        if log_dir_exists:
            logging_logger.debug(log_message)
        if not log_dir_exists:
            logging_logger.info(log_message)

    def setup(self) -> logging.Logger:
        """Set up the logger.

        Returns:
            logging.Logger: The configured logger instance.

        """
        # Sets up the logger_arg
        logger = logger_setup(
            self.logger_name,
            self.format_string,
            self.file_log_level,
            self.console_log_level,
            self.log_file_path,
        )
        return logger


# Logging levels - use the constant or edit in each class for more granular control
# This sets the minimum level of logging each logger_arg will save to the file or print to the console
# Levels - 0 NOTSET | 10 DEBUG | 20 INFO | 30 WARNING | 40 ERROR | 50 CRITICAL -  or enter like logging.INFO


# Loggers inheriting from the base logger_arg with their own logger_name and level controls
class DataTypesLogger(BaseLogger):
    """Logger for handling data type validations.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initialize the DataTypesLogger instance."""
        super().__init__(
            logger_name="validation.data_types",
            file_log_level=logger_levels.data_types_file_log_level,
            console_log_level=logger_levels.data_types_console_log_level,
        )


class PrepperLogger(BaseLogger):
    """Logger for handling prepper tools.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initialize the PrepperLogger instance."""
        super().__init__(
            logger_name="tools.prepper",
            file_log_level=logger_levels.prepper_file_log_level,
            console_log_level=logger_levels.prepper_console_log_level,
        )


class CascadeLogger(BaseLogger):
    """Logger for handling processor cascade.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initialize the CascadeLogger instance."""
        super().__init__(
            logger_name="processors.cascade",
            file_log_level=logger_levels.cascade_file_log_level,
            console_log_level=logger_levels.cascade_console_log_level,
        )


class SanitiserLogger(BaseLogger):
    """Logger for handling data sanitisation.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initialize the SanitiserLogger instance."""
        super().__init__(
            logger_name="processors.sanitiser",
            file_log_level=logger_levels.sanitiser_file_log_level,
            console_log_level=logger_levels.sanitiser_console_log_level,
        )


class ValidatorsLogger(BaseLogger):
    """Logger for handling validation models.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initialize the ValidatorsLogger instance."""
        super().__init__(
            logger_name="validation.validators_models",
            file_log_level=logger_levels.validators_file_log_level,
            console_log_level=logger_levels.validators_console_log_level,
        )


class LLMHandlerLogger(BaseLogger):
    """Logger for handling LLM processing.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initialize the LLMHandlerLogger instance."""
        super().__init__(
            logger_name="processors.LLMHandler",
            file_log_level=logger_levels.llmhandler_file_log_level,
            console_log_level=logger_levels.llmhandler_console_log_level,
        )


class BaseWorkflowLogger(BaseLogger):
    """Logger for handling workflows.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initialize the BaseWorkflowLogger instance."""
        super().__init__(
            logger_name="base_workflow",
            file_log_level=logger_levels.workflow_file_log_level,
            console_log_level=logger_levels.workflow_console_log_level,
        )


class CacheLogger(BaseLogger):
    """Logger for handling caching activites.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initialize the CacheLogger instance."""
        super().__init__(
            logger_name="processors.LLMHandler <---> tools.caching",
            file_log_level=logger_levels.cache_file_log_level,
            console_log_level=logger_levels.cache_console_log_level,
        )


class StringsToolsLogger(BaseLogger):
    """Logger for handling string transforming function actions.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initialize the StringsToolsLogger instance."""
        super().__init__(
            logger_name="tools.strings",
            file_log_level=logger_levels.strings_file_log_level,
            console_log_level=logger_levels.strings_console_log_level,
        )


class TimeToolsLogger(BaseLogger):
    """Logger for handling time related functions.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initialize the TimeToolsLogger instance."""
        super().__init__(
            logger_name="tools.time",
            file_log_level=logger_levels.time_file_log_level,
            console_log_level=logger_levels.time_console_log_level,
        )


class FileImportersToolsLogger(BaseLogger):
    """Logger for handling file importation.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initialize the FileImportersToolsLogger instance."""
        super().__init__(
            logger_name="tools.file_importers",
            file_log_level=logger_levels.file_importers_file_log_level,
            console_log_level=logger_levels.file_importers_console_log_level,
        )
