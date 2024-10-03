import logging

# Disable logging for duration
logging.disable(logging.CRITICAL)

import pandas as pd  # noqa: E402
from icecream import ic  # noqa: E402

from glyphdeck.processors.cascade import Cascade  # noqa: E402

# Establish test data
test_data = {
    1: ["door", "champ", "slam"],
    2: ["blam", "clam", "sam"],
    3: ["tim", "tam", "fam"],
}

# Create the test DataFrame
test_df = pd.DataFrame.from_dict(test_data, orient="index")
test_df = test_df.reset_index()  # Adds the index as a column
test_df.columns = ["Word ID", "Word1", "Word2", "Word3"]  # Rename cols

# (record key 1)
# Initialise, preparing the first record
cascade = Cascade(
    test_df,
    "Word ID",
    ["Word1", "Word2", "Word3"],
    # encoding="utf-8",  # Needed if you provided a .csv path
    sheet_name="Sheet1",  # Needed if you provided a .xlsx path
)

# (record key 2)
cascade.append(
    title="Example1",
    data={
        1: ["potato", "steak", "party"],
        2: ["carrot", "party", "alpha"],
        3: ["carrot", "party", "alpha"],
    },
)

# (record key 3)
cascade.append(
    title="Example2",
    data={
        1: ["potatoes", "carrot", "gary"],
        2: ["carrots", "pizza", "pasta"],
        3: ["bananas", "beast", "jeffery"],
    },
)

# Print off all the generated values
# Without a loop so ic will show the contents

# Attributes and properites of the whole cascade
ic(cascade.records)
ic(cascade.expected_len)
ic(cascade.latest_key)
ic(cascade.latest_record)
ic(cascade.latest_title)
ic(cascade.latest_dt)
ic(cascade.latest_data)

ic(cascade.latest_record)
ic(cascade.latest_df)

ic(cascade.latest_record_delta)
ic(cascade.latest_column_names)
ic(cascade.delta)

# Individual Records
print(
    "\nAccessing record 1 using getter functions, using record number or record title"
)
ic(cascade.record(1))
ic(cascade.record("prepared"))
ic(cascade.title(1))
ic(cascade.title("prepared"))
ic(cascade.title_key("prepared"))
ic(cascade.dt(1))
ic(cascade.dt("prepared"))
ic(cascade.data(1))
ic(cascade.data("prepared"))
ic(cascade.df(1))
ic(cascade.df("prepared"))
ic(cascade.record_delta(1))
ic(cascade.record_delta("prepared"))
ic(cascade.column_names(1))
ic(cascade.column_names("prepared"))

# Access records from record 1
print(
    "\nAccessing record 2 using getter functions, using record number or record title"
)
ic(cascade.record(2))
ic(cascade.record("Example1"))
ic(cascade.title(2))
ic(cascade.title("Example1"))
ic(cascade.title_key("Example1"))
ic(cascade.dt(2))
ic(cascade.dt("Example1"))
ic(cascade.data(2))
ic(cascade.data("Example1"))
ic(cascade.df(2))
ic(cascade.df("Example1"))
ic(cascade.record_delta(2))
ic(cascade.record_delta("Example1"))
ic(cascade.column_names(2))
ic(cascade.column_names("Example1"))

# Access records from record 2
print(
    "\nAccessing record 3 using getter functions, using record number or record title"
)
ic(cascade.record(3))
ic(cascade.record("Example2"))
ic(cascade.title(3))
ic(cascade.title("Example2"))
ic(cascade.title_key("Example2"))
ic(cascade.dt(3))
ic(cascade.dt("Example2"))
ic(cascade.data(3))
ic(cascade.data("Example2"))
ic(cascade.df(3))
ic(cascade.df("Example2"))
ic(cascade.record_delta(3))
ic(cascade.record_delta("Example2"))
ic(cascade.column_names(3))
ic(cascade.column_names("Example2"))

# Re-enable logging
logging.disable(logging.NOTSET)
