from typing import Union, Dict
from datetime import datetime, timedelta

# Types that will be used across the project
Record = Union[str, Union[datetime, None, timedelta, dict]]
Records = Dict[int, Record]
Data = Dict[int, str]

