from typing import Union, Dict, List
from datetime import datetime, timedelta
import pandas as pd

# Types that will be used across the project
Record = Union[str, Union[datetime, None, timedelta, dict, list]]
Records = Dict[int, Record]
IntStr = Union[int, str]
Data = Dict[IntStr, List]
Data_or_None = Union[Data, None]
dFrame = pd.DataFrame
dFrame_or_None = Union[dFrame, None]
IntList = List[int]
StrList = List[str]
StrList_or_None = Union[StrList, None]
List_or_Str = Union[str, list]


def assert_custom_type(variable: Data, custom_type: str, var_name: str):
    if custom_type.lower() == "data":
        assert isinstance(variable, dict), f"Expected custom 'Data' type in '{var_name}', " \
                                           f"instead got '{type(variable)}'"
        for key, value in variable.items():
            assert isinstance(key, (int, str)), f"Expected int or str dict key in custom 'Data' type variable " \
                                                f"'{var_name}', instead got {type(key)}"
            assert isinstance(value, list), f"Expected list dict value in custom 'Data' type variable " \
                                            f"'{var_name}', instead got {type(value)}"
