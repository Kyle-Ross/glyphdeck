import os

from typing import Tuple

from CategoriGen.path_constants import OUTPUT_DIR, OUTPUT_LOGS_DIR, OUTPUT_CACHES_DIR, OUTPUT_FILES_DIR


def create_directory(directory, directory_name, logger) -> str:
    """Function to check a directory and create it if it doesn't exist, logging the outcome & returning the directory"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"'{directory}' - {directory_name} directory created")
    else:
        logger.debug(f"'{directory}' - {directory_name} directory  exists")
    return directory


def create_output_directory(logger) -> str:
    """Checks and creates the output directory and returns the path"""
    return create_directory(OUTPUT_DIR, 'Output', logger)


def create_logs_directory(logger) -> str:
    """Checks and creates the logs directory and returns the path"""
    return create_directory(OUTPUT_LOGS_DIR, 'Logs', logger)


def create_caches_directory(logger) -> str:
    """Checks and creates the caches directory and returns the path"""
    return create_directory(OUTPUT_CACHES_DIR, 'Caches', logger)


def create_files_directory(logger) -> str:
    """Checks and creates the files directory and returns the path"""
    return create_directory(OUTPUT_FILES_DIR, 'Files', logger)


def check_logs_directory() -> Tuple[bool, str, str]:
    """Checks and creates the logs directory and returns log messages but does not make the actual log"""
    if not os.path.exists(OUTPUT_LOGS_DIR):
        os.makedirs(OUTPUT_LOGS_DIR)
        log_dir_exists = False
        log_message = f"'{OUTPUT_LOGS_DIR}' - Logs folder created"
    else:
        log_dir_exists = True
        log_message = f"'{OUTPUT_LOGS_DIR}' - Logs folder exists"
    return log_dir_exists, log_message, OUTPUT_LOGS_DIR
