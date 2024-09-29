import traceback
import logging
import os
from typing import Type, Callable, Optional

from glyphdeck.tools.directory_creators import check_logs_directory
import glyphdeck.logger_constants as logger_constants


def log_and_raise_error(
    logger_arg: logging.Logger,
    level: str,
    error_type: Type[BaseException],
    message: str,
    include_traceback: bool = False,
):
    """Logs and raises an error with the same message string.

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
    """Asserts a condition and logs the specified error.

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


def check_logger_exists(logger_name: str) -> bool:
    """Checks if a logger with the provided name exists.

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
    """Function decorator to log the start and end of a function.

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
            def conditional_log(message):
                """_summary_

                Args:
                    message (_type_): _description_

                Raises:
                    AssertionError: _description_
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


def exception_logger(
    logger_arg: logging.Logger, include_traceback: bool = True
) -> Callable:
    """Decorator to automatically log any unhandled exceptions as critical.

    Args:
        logger_arg (logging.Logger): Logger instance to log the exception.
        include_traceback (bool, optional): Whether to include the traceback in the log. Defaults to True.

    Returns:
        Callable: The decorated function.
    """

    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Callable:
            try:
                # Try the function that was passed in
                return func(*args, **kwargs)
            except Exception as error:
                # Handled exceptions should have the name 'HandledError' (see log_and_raise_error())
                # So if the exception has this name, just re-raise it - it will already have logging
                error_name = type(error).__name__
                if error_name == "HandledError":
                    raise
                # Otherwise, log the unhandled error as critical and then re-raise
                else:
                    # Build the log / error message
                    error_message = (
                        f" | Function | exception_logger() | Exit | | {error_name}"
                    )
                    if (
                        include_traceback
                    ):  # Include detailed traceback information in the log if specified
                        error_message = f"{error_message} | \\n{traceback.format_exc().replace('\n', '\\n')}"
                logger_arg.critical(error_message)
                raise

        return wrapper

    return decorator


def logger_setup(
    logger_name: str,
    format_string: str,
    file_log_level: int,
    console_log_level: int,
    log_file_path: str,
) -> logging.Logger:
    """Initializes, configures, and returns a logger instance.

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
    if check_logger_exists(logger_name):
        return logging.getLogger(logger_name)

    # Otherwise, set up the logger_arg then return it

    # Initialising the logger_arg and naming it
    logger = logging.getLogger(logger_name)

    # Set logger_arg level to the lowest level between file and console
    # This is because handlers can only access levels at or above the level of the logger_arg
    logger.setLevel(min(file_log_level, console_log_level))

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
        """Initializes the BaseLogger instance.

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
        """Sets up the logger.

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
        """Initializes the DataTypesLogger instance."""
        super().__init__(
            logger_name="validation.data_types",
            file_log_level=logger_constants.data_types_file_log_level,
            console_log_level=logger_constants.data_types_console_log_level,
        )


class PrepperLogger(BaseLogger):
    """Logger for handling prepper tools.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initializes the PrepperLogger instance."""
        super().__init__(
            logger_name="tools.prepper",
            file_log_level=logger_constants.prepper_file_log_level,
            console_log_level=logger_constants.prepper_console_log_level,
        )


class CascadeLogger(BaseLogger):
    """Logger for handling processor cascade.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initializes the CascadeLogger instance."""
        super().__init__(
            logger_name="processors.cascade",
            file_log_level=logger_constants.cascade_file_log_level,
            console_log_level=logger_constants.cascade_console_log_level,
        )


class SanitiserLogger(BaseLogger):
    """Logger for handling data sanitisation.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initializes the SanitiserLogger instance."""
        super().__init__(
            logger_name="processors.sanitiser",
            file_log_level=logger_constants.sanitiser_file_log_level,
            console_log_level=logger_constants.sanitiser_console_log_level,
        )


class ValidatorsLogger(BaseLogger):
    """Logger for handling validation models.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initializes the ValidatorsLogger instance."""
        super().__init__(
            logger_name="validation.validators_models",
            file_log_level=logger_constants.validators_file_log_level,
            console_log_level=logger_constants.console_log_level_default,
        )


class LLMHandlerLogger(BaseLogger):
    """Logger for handling LLM processing.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initializes the LLMHandlerLogger instance."""
        super().__init__(
            logger_name="processors.LLMHandler",
            file_log_level=logger_constants.llmhandler_file_log_level,
            console_log_level=logger_constants.llmhandler_console_log_level,
        )


class BaseWorkflowLogger(BaseLogger):
    """Logger for handling workflows.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initializes the BaseWorkflowLogger instance."""
        super().__init__(
            logger_name="base_workflow",
            file_log_level=logger_constants.base_workflow_file_log_level,
            console_log_level=logger_constants.base_workflow_console_log_level,
        )


class UnhandledErrorsLogger(BaseLogger):
    """Logger for handling otherwise Unhandled Errors.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initializes the UnhandledErrorsLogger instance."""
        super().__init__(
            logger_name="unhandled_errors",
            file_log_level=logger_constants.unhandled_errors_file_log_level,
            console_log_level=logger_constants.unhandled_errors_console_log_level,
        )


class CacheLogger(BaseLogger):
    """Logger for handling caching activites.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initializes the CacheLogger instance."""
        super().__init__(
            logger_name="processors.LLMHandler <---> tools.caching",
            file_log_level=logger_constants.cache_file_log_level,
            console_log_level=logger_constants.cache_console_log_level,
        )


class StringsToolsLogger(BaseLogger):
    """Logger for handling string transforming function actions.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initializes the StringsToolsLogger instance."""
        super().__init__(
            logger_name="tools.strings",
            file_log_level=logger_constants.tools_strings_file_log_level,
            console_log_level=logger_constants.tools_strings_console_log_level,
        )


class TimeToolsLogger(BaseLogger):
    """Logger for handling time related functions.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initializes the TimeToolsLogger instance."""
        super().__init__(
            logger_name="tools.time",
            file_log_level=logger_constants.tools_time_file_log_level,
            console_log_level=logger_constants.tools_time_console_log_level,
        )


class FileImportersToolsLogger(BaseLogger):
    """Logger for handling file importation.

    Inherits from BaseLogger.
    """

    def __init__(self):
        """Initializes the FileImportersToolsLogger instance."""
        super().__init__(
            logger_name="tools.file_importers",
            file_log_level=logger_constants.tools_file_importers_file_log_level,
            console_log_level=logger_constants.tools_file_importers_console_log_level,
        )
