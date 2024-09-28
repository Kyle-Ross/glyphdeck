from typing import Union, List, Optional, Tuple

import pandas as pd

from CategoriGen.tools.loggers import (
    PrepperLogger,
    log_decorator,
    assert_and_log_error,
    log_and_raise_error,
)
from CategoriGen.tools.file_importers import get_xlsx, get_csv, file_validation
from CategoriGen.validation.data_types import (
    DataDict,
    Optional_DataDict,
)

logger = PrepperLogger().setup()


@log_decorator(
    logger,
    "debug",
    suffix_message="Run the dataframe prepper",
)
def prepare_df(
    source_table: pd.DataFrame, id_column: str, data_columns: Union[str, List[str]]
) -> Tuple[pd.DataFrame, DataDict]:
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
        id_column in source_table.columns,
        f"Data loaded, but 'id_column' with name ('{id_column}') was not found in the dataframe.",
    )

    # Checks that the id column only has unique values
    assert_and_log_error(
        logger,
        "error",
        source_table[id_column].is_unique,
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

    # For lists, check that no duplicate names are in the data_columns argument
    if isinstance(data_columns, list):
        assert_and_log_error(
            logger,
            "error",
            len(data_columns) == len(set(data_columns)),
            "'data_columns' argument '{data_columns}' must only have unique column names.",
        )

    # Convert the dataframe into the Chain compatible 'Data' type and return (df, output_data)
    prepared_data: DataDict = {}
    for _, row in source_table.iterrows():
        prepared_data[row[id_column]] = [row[column] for column in data_columns]
    return (source_table, prepared_data)


@log_decorator(
    logger,
    "info",
    suffix_message="Running dataframe prepper via xslx",
)
def prepare_xlsx(file_path, id_column, data_columns, **kwargs) -> Tuple[pd.DataFrame, DataDict]:
    """Wrapper for prepare_df() that loads data from an xlsx file."""
    source_table = get_xlsx(file_path, **kwargs)
    return prepare_df(source_table, id_column, data_columns)


@log_decorator(
    logger,
    "info",
    suffix_message="Running dataframe prepper via csv",
)
def prepare_csv(file_path, id_column, data_columns, **kwargs) -> Tuple[pd.DataFrame, DataDict]:
    """Wrapper for prepare_df() that loads data from a csv file."""
    source_table = get_csv(file_path, **kwargs)
    return prepare_df(source_table, id_column, data_columns)


def type_conditional_prepare(
    data_source: Union[str, pd.DataFrame], id_column, data_columns, encoding, sheet_name
) -> Tuple[pd.DataFrame, DataDict]:
    """Runs the prepare operation differently depending on input format. Supports DataFrames, csv and xlsx."""
    # Initialising variables
    source_table: Optional[pd.DataFrame] = None  # The source as a df
    prepared_data: Optional_DataDict = None  # The data in the 'Data' type

    # Assess the input argument and conditionally prepare the data
    data_source_type = type(data_source)
    # 1 - If it is a dataframe
    if data_source_type is pd.DataFrame:
        source_table, prepared_data = prepare_df(data_source, id_column, data_columns)

    # 2 - If it is a string
    elif data_source_type is str:
        # Check file exists, validate type, return type
        file_type = file_validation(data_source)

        # 2.1 - if the file is an .csv
        if file_type == "csv":
            source_table, prepared_data = prepare_csv(
                data_source, id_column, data_columns, encoding=encoding
            )

        # 2.2 - if the file is an .xlsx
        if file_type == "xlsx":
            source_table, prepared_data = prepare_xlsx(
                data_source, id_column, data_columns, sheet_name=sheet_name
            )

    # 3 - If it is neither a dataframe or a string
    else:
        log_and_raise_error(
            logger,
            "error",
            AssertionError,
            f"'Provided data_source argument '{data_source}' is of type '{type(data_source)}' - only DataFrames and file paths are supported",
        )

    return (source_table, prepared_data)
