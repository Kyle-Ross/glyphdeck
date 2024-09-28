from datetime import datetime, timedelta
from typing import Union, Dict, List, Optional

import pandas as pd

from CategoriGen.tools.loggers import assert_and_log_error
from CategoriGen.tools.loggers import DataTypesLogger

logger = DataTypesLogger().setup()

# Types that will be used across the project
DataDict = Dict[int, List]
Optional_DataDict = Optional[DataDict]
Record = Dict[str, Union[str, None,  datetime, timedelta, DataDict, pd.DataFrame]]
RecordsDict = Dict[int, Record]


def assert_and_log_type_is_data(variable: DataDict, var_name: str):
    """Assert and log that a variable is custom type 'Data', and that the contained data is also of the correct type"""
    assert_and_log_error(
        logger,
        "error",
        isinstance(variable, dict),
        f"Expected 'Data' type 'Dict[IntStr, List]' in '{var_name}', instead got '{type(variable)}'",
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


def assert_and_log_is_type_or_list_of(
    variable: Union[List[Union[int, str]], Union[int, str]],
    var_name: str,
    allowed_list_types: list,
    allow_none: bool = False,
):
    """Assert and log that a variable is one of the allowed types, or a list containing only those allowed types"""

    # Check if it is None,
    if not allow_none:
        assert_and_log_error(
            logger,
            "error",
            variable is not None,
            f"variable '{var_name}' is 'None' while allow_none == False'",
        )

    # With that checked, only run the rest of the checks if the variable is not None
    if variable is not None:
        # Set some common variables
        variable_type = type(variable)
        list_and_allowed_types = [list] + allowed_list_types

        # Check if the argument type
        assert_and_log_error(
            logger,
            "error",
            variable_type in [list] + list_and_allowed_types,
            f"variable '{var_name}' is not in allowed types '{list_and_allowed_types}', instead got '{variable_type}'",
        )

        # Check the values in the list if the argument is a list
        if variable_type is list:
            for value in variable:
                assert_and_log_error(
                    logger,
                    "error",
                    isinstance(value, tuple(allowed_list_types)),
                    f"Expected all items in list argument '{var_name}' to in types '{allowed_list_types}', instead got '{value}' of type '{type(value)}'",
                )
