from typing import Tuple
import logging
import os

from CategoriGen.path_constants import (
    OUTPUT_DIR,
    OUTPUT_LOGS_DIR,
    OUTPUT_CACHES_DIR,
    OUTPUT_FILES_DIR,
)

# Not implementing function decorator logging here since it is nested within the functions themselves


def create_directory(
    directory: str, directory_name: str, logger_arg: logging.Logger
) -> str:
    """Creates a directory if it doesn't exist and logs the action.

    Args:
        directory: The path of the directory to create.
        directory_name: A descriptive name for the directory being created.
        logger_arg: The logger used to log the outcome of the directory creation.

    Returns:
        The path of the directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger_arg.info(
            f" | Function | create_directory() | Action | {directory}' - {directory_name} directory created"
        )
    else:
        logger_arg.info(
            f" | Function | create_directory() | Check | {directory}' - {directory_name} directory exists"
        )
    return directory


def create_output_directory(logger_arg: logging.Logger) -> str:
    """Checks and creates the output directory and returns the path.

    Args:
        logger_arg: The logger used to log the outcome of the directory creation.

    Returns:
        The path of the output directory.
    """
    return create_directory(OUTPUT_DIR, "Output", logger_arg)


def create_logs_directory(logger_arg: logging.Logger) -> str:
    """Checks and creates the logs directory and returns the path.

    Args:
        logger_arg: The logger used to log the outcome of the directory creation.

    Returns:
        The path of the logs directory.
    """
    return create_directory(OUTPUT_LOGS_DIR, "Logs", logger_arg)


def create_caches_directory(logger_arg: logging.Logger) -> str:
    """Checks and creates the caches directory and returns the path.

    Args:
        logger_arg: The logger used to log the outcome of the directory creation.

    Returns:
        The path of the caches directory.
    """
    return create_directory(OUTPUT_CACHES_DIR, "Caches", logger_arg)


def create_files_directory(logger_arg: logging.Logger) -> str:
    """Checks and creates the files directory and returns the path.

    Args:
        logger_arg: The logger used to log the outcome of the directory creation.

    Returns:
        The path of the files directory.
    """
    return create_directory(OUTPUT_FILES_DIR, "Files", logger_arg)


def check_logs_directory() -> Tuple[bool, str, str]:
    """Checks if the logs directory exists and creates it if not.

    Returns:
        A tuple containing:
            - A boolean indicating whether the logs directory existed.
            - A log message describing the action taken.
            - The path of the logs directory.
    """
    # Checks and creates the logs directory and returns log messages but does not make the actual log
    if not os.path.exists(OUTPUT_LOGS_DIR):
        os.makedirs(OUTPUT_LOGS_DIR)
        log_dir_exists = False
        log_message = f" | Function | check_logs_directory() | Action | '{OUTPUT_LOGS_DIR}' - Logs folder created"
    else:
        log_dir_exists = True
        log_message = f" | Function | check_logs_directory() | Check | '{OUTPUT_LOGS_DIR}' - Logs folder exists"
    return log_dir_exists, log_message, OUTPUT_LOGS_DIR
