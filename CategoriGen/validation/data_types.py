from datetime import datetime, timedelta
from typing import Union, Dict, List, Optional, Tuple

import pandas as pd

from CategoriGen.tools.loggers import assert_and_log_error
from CategoriGen.tools.loggers import DataTypesLogger

logger = DataTypesLogger().setup()

# Types that will be used across the project
Record = Union[str, Union[datetime, None, timedelta, dict, list]]
Records = Dict[int, Record]
IntStr = Union[int, str]
Optional_IntStr = Optional[Union[int, str]]
Data = Dict[IntStr, List]
Optional_Data = Optional[Data]
dFrame = pd.DataFrame
Optional_dFrame = Optional[dFrame]
IntList = List[int]
StrList = List[str]
dfList = List[pd.DataFrame]
RecordList = List[Record]
Optional_StrList = Optional[StrList]
List_or_Str = Union[str, list]
Str_or_StrList = Union[str, StrList]
Str_or_dFrame = Union[str, dFrame]
Optional_Str = Optional[str]
dFrame_and_Data_Tuple = Tuple[pd.DataFrame, Data]


def assert_type_is_data(variable: Data, var_name: str):
    """Assert and log that a variable is custom type 'Data', and that the contained data is also of the correct type"""
    assert_and_log_error(
        logger,
        "error",
        isinstance(variable, dict),
        f"Expected custom 'Data' type in '{var_name}', instead got '{type(variable)}'",
    )
    for key, value in variable.items():
        assert_and_log_error(
            logger,
            "error",
            isinstance(key, (int, str)),
            f"Expected int or str dict key in custom 'Data' type variable "
            f"'{var_name}', instead got {type(key)}",
        )
        assert_and_log_error(
            logger,
            "error",
            isinstance(value, list),
            f"Expected list dict value in custom 'Data' type variable "
            f"'{var_name}', instead got {type(value)}",
        )
