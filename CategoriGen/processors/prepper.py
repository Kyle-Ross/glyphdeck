from typing import Union, List

import pandas as pd

from CategoriGen.tools.loggers import PrepperLogger, log_decorator, assert_and_log_error
from CategoriGen.validation.data_types import Data

logger = PrepperLogger().setup()


class Prepper:
    """A class to process data from xlsx or csv files into a dataframe as well as the data type used in the
    chain class"""

    @log_decorator(
        logger,
        "info",
        suffix_message="Initialise Prepper object",
    )
    def __init__(
        self,
        file_path: str,
        file_type: str,
        id_column: str,
        data_columns: Union[str, List[str]],
        sheet: Union[
            str, int
        ] = 0,  # Only used for xlsx, can be the sheet name or its 0 based index
        encoding: str = "utf-8",  # Only used for csv
    ):
        """Assert argument types and then store in self for Prepper object"""
        # Assert types
        supported_file_types = ("xlsx", "csv")

        # file_path assertions
        assert_and_log_error(
            logger,
            "error",
            isinstance(file_path, str),
            "'file_path' argument must be type 'str'",
        )
        # file_type assertions
        assert_and_log_error(
            logger,
            "error",
            isinstance(file_type, str),
            "'file_type' argument must be type 'str'",
        )
        assert_and_log_error(
            logger,
            "error",
            file_type in supported_file_types,
            f"'file_type' argument must be one of the supported file types: {supported_file_types}",
        )
        # id_column assertions
        assert_and_log_error(
            logger,
            "error",
            isinstance(id_column, str),
            "'id_column' argument must be type 'str'",
        )
        # data_columns assertions
        assert_and_log_error(
            logger,
            "error",
            isinstance(data_columns, str)
            or (
                isinstance(data_columns, list)
                and all(isinstance(item, str) for item in data_columns)
            ),
            "'data_columns' argument must be type 'str' or 'List[str]'",
        )
        if isinstance(data_columns, list):  # Only for lists
            assert_and_log_error(
                logger,
                "error",
                len(data_columns) == len(set(data_columns)),
                "'data_columns' argument must only have unique values.",
            )
        # sheet assertions
        assert_and_log_error(
            logger,
            "error",
            isinstance(sheet, (str, int)),
            "'sheet' argument must be type 'str' or 'int'",
        )
        # encoding assertions
        assert_and_log_error(
            logger,
            "error",
            isinstance(encoding, str),
            "'encoding' argument must be type 'str'",
        )

        # Store the validated attributes
        self.df = pd.DataFrame()
        self.output_data: Data = {}
        self.file_path: str = file_path
        self.file_type: str = file_type
        self.id_column: str = id_column
        self.data_columns: Union[str, List[str]] = (
            [data_columns]
            if isinstance(data_columns, str)
            else data_columns  # Puts str types in a list for compatibility
        )
        self.sheet: Union[str, int] = sheet
        self.encoding: str = encoding

    @log_decorator(logger)
    def load_data(self) -> "Prepper":
        """Load data from a file into a dataframe."""
        # Load the data from the file
        if self.file_type == "xlsx":
            self.df = pd.read_excel(self.file_path, sheet_name=self.sheet)
        if self.file_type == "csv":
            self.df = pd.read_csv(self.file_path, encoding=self.encoding)
        # Check the id exists
        assert_and_log_error(
            logger,
            "error",
            self.id_column in self.df.columns,
            f"Data loaded, but 'id_column' with name ('{self.id_column}') was not found in the dataframe.",
        )
        return self

    @log_decorator(logger)
    def set_data_dict(self) -> "Prepper":
        """Sets the data dict where keys are ids and values are lists of selected data column values.
        Checks that the id_column is unique, then saves the dict in the output_data attribute."""
        # Check that the id is unique before proceeding
        assert_and_log_error(
            logger,
            "error",
            self.df[self.id_column].is_unique,
            f"'id_column' ({self.id_column}) must have unique values in every row.",
        )
        # Build the dictionary
        for _, row in self.df.iterrows():
            self.output_data[row[self.id_column]] = [
                row[column] for column in self.data_columns
            ]
        return self

    @log_decorator(logger, "info", suffix_message="Prepare data")
    def prepare(self) -> "Prepper":
        """Abstraction method which runs all the Prepper methods, without separating the load and data dict step.
        Use this if no manual changes need to be made between the load_data() and set_data_dict() steps."""
        self.load_data()
        self.set_data_dict()
        return self
