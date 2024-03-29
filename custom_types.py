from functions.logs import assert_and_log_errors
from functions.logs import core_logger_setup
from typing import Union, Dict, List
from datetime import datetime, timedelta
import pandas as pd
import os

logger = core_logger_setup()  # Gets the logger ready if it isn't there yet
current_file_name: str = os.path.basename(__file__)  # Used for log messages

# Types that will be used across the project
Record = Union[str, Union[datetime, None, timedelta, dict, list]]
Records = Dict[int, Record]
IntStr = Union[int, str]
IntStrNone = Union[int, str, None]
Data = Dict[IntStr, List]
Data_or_None = Union[Data, None]
dFrame = pd.DataFrame
dFrame_or_None = Union[dFrame, None]
IntList = List[int]
StrList = List[str]
dfList = List[pd.DataFrame]
RecordList = List[Record]
StrList_or_None = Union[StrList, None]
List_or_Str = Union[str, list]


def assert_custom_type(variable: Data, custom_type: str, var_name: str):
    if custom_type.lower() == "data":
        assert_and_log_errors(logger, 'error', current_file_name, isinstance(variable, dict),
                              f"Expected custom 'Data' type in '{var_name}', instead got '{type(variable)}'")
        for key, value in variable.items():
            assert_and_log_errors(logger, 'error', current_file_name, isinstance(key, (int, str)),
                                  f"Expected int or str dict key in custom 'Data' type variable "
                                  f"'{var_name}', instead got {type(key)}")
            assert_and_log_errors(logger, 'error', current_file_name, isinstance(value, list),
                                  f"Expected list dict value in custom 'Data' type variable "
                                  f"'{var_name}', instead got {type(value)}")
