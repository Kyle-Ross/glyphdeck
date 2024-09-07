from typing import Union, List

import pandas as pd

from CategoriGen.tools.loggers import PrepperLogger, log_decorator, assert_and_log_error
from CategoriGen.tools.file_importers import get_xlsx, get_csv
from CategoriGen.validation.data_types import Data

logger = PrepperLogger().setup()


@log_decorator(
    logger,
    "debug",
    suffix_message="Run the dataframe prepper",
)
def prepare_df(
    df: pd.DataFrame, id_column: str, data_columns: Union[str, List[str]]
) -> Data:
    """Prepares a dataframe into the common data dict where keys are ids and values are lists of selected data column values.
    Validates the provided arguments for id_columns and data_columns."""
    # Adapts strings into a list if provided
    data_columns = [data_columns] if isinstance(data_columns, str) else data_columns

    # Check that the id column is a string
    assert_and_log_error(
        logger,
        "error",
        isinstance(id_column, str),
        f"'id_column' argument '{id_column}' must be type 'str'",
    )
    # Checks that the id column exists
    assert_and_log_error(
        logger,
        "error",
        id_column in df.columns,
        f"Data loaded, but 'id_column' with name ('{id_column}') was not found in the dataframe.",
    )

    # Checks that the id column only has unique values
    assert_and_log_error(
        logger,
        "error",
        df[id_column].is_unique,
        f"'id_column' ({id_column}) must have unique values in every row.",
    )

    # Checks the data_columns argument is either a string or a list of strings
    assert_and_log_error(
        logger,
        "error",
        isinstance(data_columns, str)
        or (
            isinstance(data_columns, list)
            and all(isinstance(item, str) for item in data_columns)
        ),
        f"'data_columns' argument '{data_columns}' must be type 'str' or 'List[str]'",
    )

    # Check that no duplicate names are in the data_columns argument
    if isinstance(data_columns, list):  # Only for lists
        assert_and_log_error(
            logger,
            "error",
            len(data_columns) == len(set(data_columns)),
            "'data_columns' argument '{data_columns}' must only have unique column names.",
        )

    # Convert the dataframe into the Chain compatible 'Data' type and return
    output_data: Data = {}
    for _, row in df.iterrows():
        output_data[row[id_column]] = [row[column] for column in data_columns]
    return output_data


@log_decorator(
    logger,
    "info",
    suffix_message="Running dataframe prepper via xslx",
)
def prepare_xlsx(file_path, id_column, data_columns, **kwargs) -> Data:
    """Wrapper for prepare_df() that loads data from an xlsx file."""
    df = get_xlsx(file_path, **kwargs)
    return prepare_df(df, id_column, data_columns)


@log_decorator(
    logger,
    "info",
    suffix_message="Running dataframe prepper via csv",
)
def prepare_csv(file_path, id_column, data_columns, **kwargs) -> Data:
    """Wrapper for prepare_df() that loads data from a csv file."""
    df = get_csv(file_path, **kwargs)
    return prepare_df(df, id_column, data_columns)
