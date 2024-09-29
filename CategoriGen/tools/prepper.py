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
    """Prepares a dataframe into a common data dictionary format.

    This function transforms a dataframe into a dictionary where keys are unique IDs
    from a specified column, and values are lists of selected data column values. It
    also validates the presence and uniqueness of the specified ID column and
    ensures the data columns are correctly specified.

    Args:
        source_table (pd.DataFrame): The source dataframe to be prepared.
        id_column (str): The name of the column that contains unique IDs.
        data_columns (Union[str, List[str]]): A single column name or a list of
            column names that contain the data to be extracted.

    Returns:
        Tuple[pd.DataFrame, DataDict]: A tuple containing the original dataframe
        and a dictionary with IDs as keys and lists of column values as values.

    Raises:
        AssertionError: If `id_column` is not a string, does not exist in the
            dataframe, or if the column contains duplicate values.
        AssertionError: If `data_columns` is neither a string nor a list of
            strings, or if the list contains duplicate column names.
    """
    # Prepares a dataframe into the common data dict where keys are ids and values are lists of selected data column values.
    # Validates the provided arguments for id_columns and data_columns.
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
def prepare_xlsx(
    file_path: str, id_column: str, data_columns: Union[str, List[str]], **kwargs
) -> Tuple[pd.DataFrame, DataDict]:
    """Loads data from an XLSX file and prepares it into a common data dictionary format.

    Args:
        file_path (str): The path to the XLSX file to be loaded.
        id_column (str): The name of the column that contains unique IDs.
        data_columns (Union[str, List[str]]): A single column name or a list of
            column names that contain the data to be extracted.
        **kwargs: Additional keyword arguments to pass to the file loader.

    Returns:
        Tuple[pd.DataFrame, DataDict]: A tuple containing the dataframe loaded from
        the XLSX file and a dictionary with IDs as keys and lists of column values
        as values.

    Raises:
        FileNotFoundError: If the specified XLSX file does not exist.
        ValueError: If the file cannot be read as an XLSX.
        AssertionError: If there are issues validating `id_column` or `data_columns`.
    """
    # Wrapper for prepare_df() that loads data from an xlsx file.
    source_table = get_xlsx(file_path, **kwargs)
    return prepare_df(source_table, id_column, data_columns)


@log_decorator(
    logger,
    "info",
    suffix_message="Running dataframe prepper via csv",
)
def prepare_csv(
    file_path: str, id_column: str, data_columns: Union[str, List[str]], **kwargs
) -> Tuple[pd.DataFrame, DataDict]:
    """Loads data from a CSV file and prepares it into a common data dictionary format.

    Args:
        file_path (str): The path to the CSV file to be loaded.
        id_column (str): The name of the column that contains unique IDs.
        data_columns (Union[str, List[str]]): A single column name or a list of
            column names that contain the data to be extracted.
        **kwargs: Additional keyword arguments to pass to the file loader.

    Returns:
        Tuple[pd.DataFrame, DataDict]: A tuple containing the dataframe loaded from
        the CSV file and a dictionary with IDs as keys and lists of column values
        as values.

    Raises:
        FileNotFoundError: If the specified CSV file does not exist.
        ValueError: If the file cannot be read as a CSV.
        AssertionError: If there are issues validating `id_column` or `data_columns`.
    """
    # Wrapper for prepare_df() that loads data from a csv file.
    source_table = get_csv(file_path, **kwargs)
    return prepare_df(source_table, id_column, data_columns)


def type_conditional_prepare(
    data_source: Union[str, pd.DataFrame],
    id_column: str,
    data_columns: Union[str, List[str]],
    encoding: str,
    sheet_name: str,
) -> Tuple[pd.DataFrame, DataDict]:
    """Conditionally prepares data from various formats into a common data dictionary format.

    Depending on the input format (dataframe, CSV file, or XLSX file), this function
    runs the appropriate preparation routine to convert the data into a common
    dictionary format.

    Args:
        data_source (Union[str, pd.DataFrame]): The data source to be prepared. This
            can be a dataframe, a CSV file path, or an XLSX file path.
        id_column (str): The name of the column that contains unique IDs.
        data_columns (Union[str, List[str]]): A single column name or a list of
            column names that contain the data to be extracted.
        encoding (str): The encoding to use when reading text files.
        sheet_name (str): The name of the sheet to read from in an XLSX file.

    Returns:
        Tuple[pd.DataFrame, DataDict]: A tuple containing the prepared dataframe and
        a dictionary with IDs as keys and lists of column values as values.

    Raises:
        AssertionError: If `data_source` is not a dataframe or a string path to a
            CSV/XLSX file.
        FileNotFoundError: If the specified CSV/XLSX file does not exist.
        ValueError: If the file cannot be read as a CSV/XLSX.
        AssertionError: If there are issues validating `id_column` or `data_columns`.
    """
    # Runs the prepare operation differently depending on input format. Supports DataFrames, csv and xlsx.
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
