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
print("\nInitialise chain (Record Key 1)")
chain = Chain(
    test_df,
    "Word ID",
    ["Word1", "Word2", "Word3"],
    # encoding="utf-8",  # Needed if you provided a .csv path
    sheet_name="Sheet1"  # Needed if you provided a .xlsx path
)
ic(chain.record(1))
ic(chain.latest_data)
ic(chain.sanitiser.input_data)

# (record key 2)
# The table only needs to be added in the first step, but can be included again to add a new one
# Otherwise the table will be the last time 'table' was assigned, or the original source data as a dataframe
print("\nAppend Record, Example1 (Record Key 2)")
chain.append(
    title="Example1",
    data={
        1: ["potato", "steak", "party"],
        2: ["carrot", "party", "alpha"],
        3: ["carrot", "party", "alpha"],
    }
)
ic(chain.record(2))
ic(chain.latest_data)
ic(chain.sanitiser.input_data)

# (record key 3)
# Since table is not assigned table will just be the last table
print("\nAppend Record, Example2 (Record Key 3)")
chain.append(
    title="Example2",
    data={
        1: ["potatoes", "carrot", "gary"],
        2: ["carrots", "pizza", "pasta"],
        3: ["bananas", "beast", "jeffery"],
    },
)
ic(chain.record(3))
ic(chain.latest_data)
ic(chain.sanitiser.input_data)

# Print off all the generated values
# Without a loop so ic will show the contents

# Attributes and properites of the whole chain
ic(chain.records)

# Re-enable logging
logging.disable(logging.NOTSET)
