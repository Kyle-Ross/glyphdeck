import pandas as pd
from icecream import ic
import logging

from CategoriGen.processors.chain import Chain

# Disable logging for duration
logging.disable(logging.CRITICAL)

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
chain = Chain(
    test_df,
    "Word ID",
    ["Word1", "Word2", "Word3"],
    # encoding="utf-8",  # Needed if you provided a .csv path
    sheet_name="Sheet1"  # Needed if you provided a .xlsx path
)

# (record key 2)
# The table only needs to be added in the first step, but can be included again to add a new one
# Otherwise the table will be the last time 'table' was assigned, or the original source data as a dataframe
chain.append(
    title="Example1",
    data={
        1: ["potato", "steak", "party"],
        2: ["carrot", "party", "alpha"],
        3: ["carrot", "party", "alpha"],
    }
)

# (record key 3)
# Since table is not assigned table will just be the last table
chain.append(
    title="Example2",
    data={
        1: ["potatoes", "carrot", "gary"],
        2: ["carrots", "pizza", "pasta"],
        3: ["bananas", "beast", "jeffery"],
    },
)

# Print off all the generated values
# Without a loop so ic will show the contents

# Attributes and properites of the whole chain
ic(chain.records)
ic(chain.expected_len)
ic(chain.latest_key)
ic(chain.latest_record)
ic(chain.latest_title)
ic(chain.latest_dt)
ic(chain.latest_data)
ic(chain.latest_table)
ic(chain.latest_table_id_column)
ic(chain.latest_record_delta)
ic(chain.latest_column_names)
ic(chain.initial_key)
ic(chain.initial_record)
ic(chain.initial_title)
ic(chain.initial_dt)
ic(chain.initial_data)
ic(chain.initial_table)
ic(chain.initial_table_id_column)
ic(chain.initial_column_names)
ic(chain.delta)

# Individual Records
print("Accessing record 1 using getter functions, using record number or record title")
ic(chain.record(1))
ic(chain.record("prepared"))
ic(chain.title(1))
ic(chain.title("prepared"))
ic(chain.title_key("prepared"))
ic(chain.dt(1))
ic(chain.dt("prepared"))
ic(chain.data(1))
ic(chain.data("prepared"))
ic(chain.table(1))
ic(chain.table("prepared"))
ic(chain.table_id_column(1))
ic(chain.table_id_column("prepared"))
ic(chain.record_delta(1))
ic(chain.record_delta("prepared"))
ic(chain.column_names(1))
ic(chain.column_names("prepared"))

# Access records from record 1
print("Accessing record 2 using getter functions, using record number or record title")
ic(chain.record(2))
ic(chain.record("Example1"))
ic(chain.title(2))
ic(chain.title("Example1"))
ic(chain.title_key("Example1"))
ic(chain.dt(2))
ic(chain.dt("Example1"))
ic(chain.data(2))
ic(chain.data("Example1"))
ic(chain.table(2))
ic(chain.table("Example1"))
ic(chain.table_id_column(2))
ic(chain.table_id_column("Example1"))
ic(chain.record_delta(2))
ic(chain.record_delta("Example1"))
ic(chain.column_names(2))
ic(chain.column_names("Example1"))

# Access records from record 2
print("Accessing record 3 using getter functions, using record number or record title")
ic(chain.record(3))
ic(chain.record("Example2"))
ic(chain.title(3))
ic(chain.title("Example2"))
ic(chain.title_key("Example2"))
ic(chain.dt(3))
ic(chain.dt("Example2"))
ic(chain.data(3))
ic(chain.data("Example2"))
ic(chain.table(3))
ic(chain.table("Example2"))
ic(chain.table_id_column(3))
ic(chain.table_id_column("Example2"))
ic(chain.record_delta(3))
ic(chain.record_delta("Example2"))
ic(chain.column_names(3))
ic(chain.column_names("Example2"))

# Re-enable logging
logging.disable(logging.NOTSET)