from typing import Union, List

import pandas as pd

from CategoriGen.tools.loggers import PrepperLogger, log_and_raise_error, log_decorator
from CategoriGen.validation.data_types import Data

logger = PrepperLogger().setup()


class Prepper:
    """A class to process data from xlsx or csv files into a dataframe as well as the data type used in the
    chain class"""

    @log_decorator(logger, start="Initialising Prepper object", finish="Initialised Prepper object")
    def __init__(self):
        """Initialize an empty dataframe, id column logger_name, and data columns dictionary."""
        self.df = pd.DataFrame()
        self.id_column: str = ""
        self.data_columns: List[str] = []
        self.output_data: Data = {}

    @log_decorator(logger)
    def load_data(self,
                  file_path: str,
                  file_type: str,
                  sheet_name: Union[str, int] = 0,  # xlsx sheet index or logger_name - default being 0 (the first sheet)
                  encoding: str = "utf-8") -> 'Prepper':
        """Load data from a file into a dataframe."""
        if file_type == 'xlsx':
            self.df = pd.read_excel(file_path, sheet_name=sheet_name)
        elif file_type == 'csv':
            self.df = pd.read_csv(file_path, encoding=encoding)
        else:
            log_and_raise_error(logger, 'error', ValueError,
                                "Invalid file type. Only 'xlsx' and 'csv' are supported.")
        return self

    @log_decorator(logger)
    def set_id_column(self, id_column: Union[str, int]) -> 'Prepper':
        """Set the id column logger_name and check if it only has unique values."""
        if self.df[id_column].is_unique:
            self.id_column = str(id_column)
        else:
            log_and_raise_error(logger, 'error', ValueError,
                                "ID column must have unique values.")
        return self

    @log_decorator(logger)
    def set_data_columns(self, data_columns: Union[str, List[str]]) -> 'Prepper':
        """Set the id column logger_name and check if it only has unique values."""
        if isinstance(data_columns, str):
            self.data_columns = [data_columns]
        elif isinstance(data_columns, list):
            if len(data_columns) != len(set(data_columns)):
                log_and_raise_error(logger, 'error', ValueError,
                                    "Data columns must all be unique.")
            self.data_columns = data_columns
        else:
            log_and_raise_error(logger, 'error', ValueError,
                                "Data columns must be either a string or a list of strings.")
        return self

    @log_decorator(logger)
    def set_data_dict(self) -> 'Prepper':
        """Sets the data dict where keys are ids and values are lists of selected data column values."""
        for _, row in self.df.iterrows():
            self.output_data[row[self.id_column]] = [row[column] for column in self.data_columns]
        return self


if __name__ == "__main__":
    """Only runs below if script is run directly, not on import, so this is for testing purposes"""
    test_file = "../../scratch/Kaggle - Coronavirus tweets NLP - Text Classification/Corona_NLP_train.csv"
    processor = (Prepper()
                 .load_data(test_file, 'csv', encoding="ISO-8859-1")
                 .set_id_column('UserName')
                 .set_data_columns(['OriginalTweet', 'Location'])
                 .set_data_dict())
    print("print(processor.output_data)")
    print(processor.output_data)
    print("print(processor.df)")
    print(processor.df)
    print("print(processor.id_column)")
    print(processor.id_column)
    print("print(processor.data_columns)")
    print(processor.data_columns)
