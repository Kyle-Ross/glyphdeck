from typing import Optional
import os

import pandas as pd
import openpyxl  # noqa: F401  -- Not referenced, but avoids errors with pd.read_excel()

from CategoriGen.tools.loggers import (
    FileImportersToolsLogger,
    log_decorator,
    assert_and_log_error,
    log_and_raise_error,
)

logger = FileImportersToolsLogger().setup()


def assert_and_log_error_path(path: str, function_name: str):
    """Wrapper for asserting and logging correct path in file_importers"""
    assert_and_log_error(
        logger,
        "error",
        isinstance(path, str),
        f"'file_path' argument in {function_name} must be type 'str'",
    )


@log_decorator(logger)
def get_xlsx(file_path: str, **kwargs) -> pd.DataFrame:
    """Wrapper for pd.read_excel with additional logic"""
    assert_and_log_error_path(file_path, "get_xlsx()")
    return pd.read_excel(file_path, engine="openpyxl", **kwargs)


@log_decorator(logger)
def get_csv(file_path: str, **kwargs) -> pd.DataFrame:
    """Wrapper for pd.read_csv with additional logic"""
    assert_and_log_error_path(file_path, "get_csv()")
    return pd.read_csv(file_path, **kwargs)


@log_decorator(logger)
def file_validation(file_path: str) -> str:
    """Takes a file path, checks that the file exists and is in one of the compatible types, then returns the file type."""
    # Validating input
    assert_and_log_error(
        logger,
        "error",
        isinstance(file_path, str),
        f"file_path argument '{file_path}' is not a string",
    )

    # Initialising holder variable
    file_type: Optional[str] = None  # The source as a df

    # 1.0 - the string is a findable file path
    if os.path.isfile(file_path):
        # 1.1 - if the file is a .csv
        if file_path.endswith(".csv"):
            file_type = "csv"

        # 1.2 - if the file is an .xlsx
        elif file_path.endswith(".xlsx"):
            file_type = "xlsx"

        # 1.3 - if the file was valid, but not a supported type
        else:
            log_and_raise_error(
                logger,
                "error",
                AssertionError,
                f"'Provided data_source file path '{file_path}' is not one of the supported file types",
            )

    # 2.0
    else:
        log_and_raise_error(
            logger,
            "error",
            AssertionError,
            f"'Provided data_source file path '{file_path}' does not exist",
        )

    return file_type
