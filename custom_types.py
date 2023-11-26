from typing import Union, Dict, List
from datetime import datetime, timedelta
import pandas as pd

# Types that will be used across the project
Record = Union[str, Union[datetime, None, timedelta, dict]]
Records = Dict[int, Record]
Data = Dict[int, List]
IntStr = Union[int, str]
dFrame = pd.DataFrame
dFrame_or_None = Union[dFrame, None]
IntList = List[int]
