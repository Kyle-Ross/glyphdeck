import pandas as pd
import openpyxl  # noqa: F401  -- Not referenced, but avoids errors with pd.read_excel()

from CategoriGen.tools.loggers import (
    FileImportersToolsLogger,
    log_decorator,
    assert_and_log_error,
)

logger = FileImportersToolsLogger().setup()


def assert_and_log_error_path(path, function_name):
    """Wrapper for asserting and logging correct path in file_importers"""
    assert_and_log_error(
        logger,
        "error",
        isinstance(path, str),
        f"'file_path' argument in {function_name} must be type 'str'",
    )


@log_decorator(logger)
def get_xlsx(file_path, **kwargs):
    """Wrapper for pd.read_excel with additional logic"""
    assert_and_log_error_path(file_path, "get_xlsx()")
    return pd.read_excel(file_path, engine="openpyxl", **kwargs)


@log_decorator(logger)
def get_csv(file_path, **kwargs):
    """Wrapper for pd.read_csv with additional logic"""
    assert_and_log_error_path(file_path, "get_csv()")
    return pd.read_csv(file_path, **kwargs)
