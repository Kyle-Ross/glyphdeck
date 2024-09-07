from icecream import ic
import pandas as pd
import numpy as np
import logging
from CategoriGen.tools.prepper import prepare_df, prepare_xlsx, prepare_csv

# Disable logging for duration
logging.disable(logging.CRITICAL)

# Create an example DataFrame
np.random.seed(0)
df = pd.DataFrame(
    {
        "id": [1, 2, 3, 4, 5],  # ID column
        "data1": ["apple", "banana", "cherry", "date", "elderberry"],
        "data2": ["fig", "grape", "honeydew", "kiwi", "lemon"],
        "revenue1": [150.50, 200.75, 300.25, 450.10, 500.60],
        "revenue2": [250.30, 300.45, 400.60, 550.20, 600.70],
        "revenue3": [350.70, 400.85, 500.90, 650.30, 700.80],
    }
)

print("Prepare directly on the dataframe")
ic(prepare_df(df, "id", ["data1", "data2"]))

print("Prepare via xlsx file")
ic(
    prepare_xlsx(
        r"F:\Github\CategoriGen\scratch\Womens clothing reviews\Womens Clothing E-Commerce Reviews - 1000.xlsx",
        "Row ID",
        [
            "Review Text",
        ],
        sheet_name="Sheet1",
    )
)

print("Prepare via csv file")
ic(
    prepare_csv(
        r"F:\Github\CategoriGen\scratch\Womens clothing reviews\Womens Clothing E-Commerce Reviews - 1000.csv",
        "Row ID",
        "Review Text",
        encoding="ISO-8859-1",
    )
)

# Re-enable logging
logging.disable(logging.NOTSET)
