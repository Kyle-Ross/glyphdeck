import logging

# Disable logging for duration
logging.disable(logging.CRITICAL)

from icecream import ic  # noqa: E402
import pandas as pd  # noqa: E402

import glyphdeck as gd  # noqa: E402

print("\nCreate example dataframe")
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
ic(df)

print("\nPrepare directly on the dataframe")
ic(gd.prepare_df(df, "id", ["data1", "data2"]))

print("\nPrepare via xlsx file")
ic(
    gd.prepare_xlsx(
        r"tests\testdata.pizzashopreviews.xlsx",
        "Review Id",
        [
            "Review Text",
        ],
        sheet_name="Sheet1",
    )
)

print("\nPrepare via csv file")
ic(
    gd.prepare_csv(
        r"tests\testdata.pizzashopreviews.csv",
        "Review Id",
        "Review Text",
        encoding="ISO-8859-1",
    )
)

# Re-enable logging
logging.disable(logging.NOTSET)
