from typing import Union, List
import os

import pandas as pd

from CategoriGen.tools.loggers import (
    PrepperLogger,
    log_decorator,
    assert_and_log_error,
    log_and_raise_error,
)
from CategoriGen.tools.file_importers import get_xlsx, get_csv
from CategoriGen.validation.data_types import (
    Data,
    Optional_Data,
    Str_or_dFrame,
    Optional_dFrame,
    dFrame_and_Data_Tuple,
)

logger = PrepperLogger().setup()


@log_decorator(
    logger,
    "debug",
    suffix_message="Run the dataframe prepper",
)
def prepare_df(
    source_table: pd.DataFrame, id_column: str, data_columns: Union[str, List[str]]
) -> dFrame_and_Data_Tuple:
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

    # Check that no duplicate names are in the data_columns argument
    if isinstance(data_columns, list):  # Only for lists
        assert_and_log_error(
            logger,
            "error",
            len(data_columns) == len(set(data_columns)),
            "'data_columns' argument '{data_columns}' must only have unique column names.",
        )

    # Convert the dataframe into the Chain compatible 'Data' type and return (df, output_data)
    prepared_data: Data = {}
    for _, row in source_table.iterrows():
        prepared_data[row[id_column]] = [row[column] for column in data_columns]
    return (source_table, prepared_data)


@log_decorator(
    logger,
    "info",
    suffix_message="Running dataframe prepper via xslx",
)
def prepare_xlsx(file_path, id_column, data_columns, **kwargs) -> dFrame_and_Data_Tuple:
    """Wrapper for prepare_df() that loads data from an xlsx file."""
    source_table = get_xlsx(file_path, **kwargs)
    return prepare_df(source_table, id_column, data_columns)


@log_decorator(
    logger,
    "info",
    suffix_message="Running dataframe prepper via csv",
)
def prepare_csv(file_path, id_column, data_columns, **kwargs) -> dFrame_and_Data_Tuple:
    """Wrapper for prepare_df() that loads data from a csv file."""
    source_table = get_csv(file_path, **kwargs)
    return prepare_df(source_table, id_column, data_columns)


def type_conditional_prepare(
    data_source: Str_or_dFrame, id_column, data_columns, encoding, sheet_name
) -> dFrame_and_Data_Tuple:
    # Initialising variables
    source_table: Optional_dFrame = None  # The source as a df
    prepared_data: Optional_Data = None  # The data in the 'Data' type

    # Assess the input argument and conditionally prepare the data
    data_source_type = type(data_source)
    # 1 - If it is a dataframe
    if data_source_type is pd.DataFrame:
        source_table, prepared_data = prepare_df(data_source, id_column, data_columns)

    # 2 - If it is a string
    elif data_source_type is str:
        # 2.1 - the string is a findable file path
        if os.path.isfile(data_source):
            # 2.1.1 - if the file is a .csv
            if data_source.endswith(".csv"):
                source_table, prepared_data = prepare_csv(
                    data_source, id_column, data_columns, encoding=encoding
                )

            # 2.1.2 - if the file is an .xlsx
            elif data_source.endswith(".xlsx"):
                source_table, prepared_data = prepare_xlsx(
                    data_source, id_column, data_columns, sheet_name=sheet_name
                )

            # 2.1.3 - if the file was valid, but not a supported type
            else:
                log_and_raise_error(
                    logger,
                    "error",
                    AssertionError,
                    f"'Provided data_source file path '{data_source}' is not one of the supported file types",
                )

        # 2.2
        else:
            log_and_raise_error(
                logger,
                "error",
                AssertionError,
                f"'Provided data_source file path '{data_source}' does not exist",
            )

    # 3 - If it is neither a dataframe or a string
    else:
        log_and_raise_error(
            logger,
            "error",
            AssertionError,
            f"'Provided data_source argument '{data_source}' is of type '{type(data_source)}' - only DataFrames and file paths are supported",
        )

    # if source_type ==

    return (source_table, prepared_data)
