import traceback
import logging
import os
from typing import Type, Optional

from CategoriGen.tools.directory_creators import check_logs_directory
import CategoriGen.logger_constants as logger_constants


def log_and_raise_error(
    logger_arg: logging.Logger,
    level: str,
    error_type: Type[BaseException],
    message: str,
    include_traceback: bool = False,
) -> None:
    """Logs and raises an error with the same message string. Wraps in custom error to indicate this is an error that
    was handled. Later this will prevent it being re-raised as an log level CRITICAL unhandled error"""

    # This class name is what is check to see if an error is handled
    class HandledError(error_type):
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
    error_message = f"{error_type.__name__} - {message}"
    if (
        include_traceback
    ):  # Include detailed traceback information in the log if specified
        error_message = (
            f"{error_message}\\n{traceback.format_exc().replace('\n', '\\n')}"
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
    traceback_message: bool = False,
) -> None:
    """Asserts a condition and logs the specified error"""
    if not condition:
        log_and_raise_error(
            logger_arg, level, AssertionError, message, traceback_message
        )


class LogBlock:
    """Context manager for 'with' blocks, logging the start and end with a specified process logger_name"""

    def __init__(self, process_name: str, logger_arg: logging.Logger):
        self.process_name: str = process_name
        self.logger_arg: logging.Logger = logger_arg

    def __enter__(self):  # What happens at the start of the 'with' block
        self.logger_arg.info(f"Started: {self.process_name}")

    def __exit__(self, exc_type, exc_value, exc_tb):  # What happens at the end
        self.logger_arg.info(f"Finished: {self.process_name}")


def check_logger_exists(logger_name: str):
    """Checks if a logger_arg with the provided logger_name exists"""
    existing_loggers = logging.Logger.manager.loggerDict.keys()
    return (
        logger_name in existing_loggers
    )  # To be evaluated as True if it exists at all


nesting_level = 0


def log_decorator(
    logger_arg,
    level: str = "debug",
    start: Optional[str] = None,
    finish: Optional[str] = None,
    is_static_method=False,
    is_property=False,
):
    """Function decorator to log the start and end of a function with an optional suffix message"""
    # Create the suffixes only if they were provided
    start = " - " + start if start is not None else ""
    finish = " - " + finish if finish is not None else ""
    # Set the prefix text
    if is_static_method:
        prefix = "Static method"
    elif is_property:
        prefix = "Property"
    else:
        prefix = "Function"

    levels = ["debug", "info", "warning", "error", "critical"]

    def outer_wrapper(func):
        # Build the message
        start_message = f"{prefix}: '{func.__name__}' - Start{start}"
        finish_message = f"{prefix}: '{func.__name__}' - Finish{finish}"

        def inner_wrapper(*args, **kwargs):
            def conditional_log(message):
                """Make a different type of log depending on the provided arguments"""
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
            nesting_prefix = f"Nest {nesting_level}: "
            conditional_log(nesting_prefix + start_message)
            result = func(*args, **kwargs)
            conditional_log(nesting_prefix + finish_message)
            nesting_level -= 1

            return result

        return inner_wrapper

    return outer_wrapper


def exception_logger(
    logger_arg, include_traceback=True
):  # At this level the function is a "decorator factory"
    """Decorator function to automatically log any errors that are not explicitly handled elsewhere"""

    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)  # Try the function that was passed in
            except Exception as error:
                # Handled exceptions should have the logger_name 'HandledError' (see log_and_raise_error())
                # So if the exception has this logger_name, just re-raise it - it will already have logging
                if type(error).__name__ == "HandledError":
                    raise
                # Otherwise, log the unhandled error as critical and then re-raise
                else:
                    # Conditionally log a more detailed message with the error traceback appended
                    if include_traceback:
                        # Flattens the traceback into a single line by replacing newlines
                        error_message = f"{type(error).__name__}\\n{traceback.format_exc().replace('\n', '\\n')}"
                    else:
                        error_message = type(error).__name__
                    # Log the message as CRITICAL and re-raise
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
    """Initialises, configures and returns the logger_arg object if it doesn't exist yet"""
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
    def __init__(
        self,
        logger_name: str,
        file_log_level: int,
        console_log_level: int,
        log_file_name: str = "base.log",
    ):
        self.logger_name: str = logger_name
        self.file_log_level: int = file_log_level
        self.console_log_level: int = console_log_level
        self.log_file_name: str = log_file_name
        self.format_string: str = (
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

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
        """Sets up the logger_arg"""
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
# Levels - 0 NOTSET | 10 DEBUG | 20 INFO | 30 WARNING | 40 ERROR | 50 CRITICAL -  orenter like logging.INFO


# Loggers inheriting from the base logger_arg with their own logger_name and level controls
class DataTypesLogger(BaseLogger):
    def __init__(self):
        super().__init__(
            logger_name="validation.data_types",
            file_log_level=logger_constants.data_types_file_log_level,
            console_log_level=logger_constants.data_types_console_log_level,
        )


class PrepperLogger(BaseLogger):
    def __init__(self):
        super().__init__(
            logger_name="processors.Prepper",
            file_log_level=logger_constants.prepper_file_log_level,
            console_log_level=logger_constants.prepper_console_log_level,
        )


class ChainLogger(BaseLogger):
    def __init__(self):
        super().__init__(
            logger_name="processors.chain",
            file_log_level=logger_constants.chain_file_log_level,
            console_log_level=logger_constants.chain_console_log_level,
        )


class SanitiserLogger(BaseLogger):
    def __init__(self):
        super().__init__(
            logger_name="processors.sanitiser",
            file_log_level=logger_constants.sanitiser_file_log_level,
            console_log_level=logger_constants.sanitiser_console_log_level,
        )


class ValidatorsLogger(BaseLogger):
    def __init__(self):
        super().__init__(
            logger_name="validation.validators_models",
            file_log_level=logger_constants.validators_file_log_level,
            console_log_level=logger_constants.console_log_level_default,
        )


class LLMHandlerLogger(BaseLogger):
    def __init__(self):
        super().__init__(
            logger_name="processors.LLMHandler",
            file_log_level=logger_constants.llmhandler_file_log_level,
            console_log_level=logger_constants.llmhandler_console_log_level,
        )


class BaseWorkflowLogger(BaseLogger):
    def __init__(self):
        super().__init__(
            logger_name="base_workflow",
            file_log_level=logger_constants.base_workflow_file_log_level,
            console_log_level=logger_constants.base_workflow_console_log_level,
        )


class UnhandledErrorsLogger(BaseLogger):
    def __init__(self):
        super().__init__(
            logger_name="unhandled_errors",
            file_log_level=logger_constants.unhandled_errors_file_log_level,
            console_log_level=logger_constants.unhandled_errors_console_log_level,
        )


class CacheLogger(BaseLogger):
    def __init__(self):
        super().__init__(
            logger_name="processors.llm_handler <---> tools.caching",
            file_log_level=logger_constants.cache_file_log_level,
            console_log_level=logger_constants.cache_console_log_level,
        )


class StringsToolsLogger(BaseLogger):
    def __init__(self):
        super().__init__(
            logger_name="tools.strings",
            file_log_level=logger_constants.tools_strings_file_log_level,
            console_log_level=logger_constants.tools_strings_console_log_level,
        )


class TimeToolsLogger(BaseLogger):
    def __init__(self):
        super().__init__(
            logger_name="tools.time",
            file_log_level=logger_constants.tools_time_file_log_level,
            console_log_level=logger_constants.tools_time_console_log_level,
        )
