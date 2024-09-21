# import logging

# # Disable logging for duration
# logging.disable(logging.CRITICAL)

import pandas as pd  # noqa: E402
from icecream import ic  # noqa: E402

from CategoriGen.processors.chain import Chain  # noqa: E402

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
    sheet_name="Sheet1",  # Needed if you provided a .xlsx path
)

# (record key 2)
chain.append(
    title="Record2",
    data={
        1: ["potato", "steak", "party"],
        2: ["carrot", "party", "alpha"],
        3: ["carrot", "party", "alpha"],
    },
)

# (record key 3)
chain.append(
    title="Record3",
    data={
        1: ["jeff", "sam", "legend"],
        2: ["alpha", "sigma", "oppa"],
        3: ["senpai", "relive", "septum"],
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

ic(chain.latest_record)
ic(chain.latest_df)

ic(chain.latest_record_delta)
ic(chain.latest_column_names)
ic(chain.delta)

# Individual Records
print("\nAccessing record 1")
ic(chain.record(1))

# Access records from record 2
print("\nAccessing record 2")
ic(chain.record(2))

# Access records from record 3
print("\nAccessing record 3")
ic(chain.record(3))

print("\nWriting output of the latest record, which is 'Record3'")
chain.write_output("xlsx", "chain single record output")

print("\nWriting output of both record 2 and 3'")
# TODO - expected output columns to each have a suffix, but it isn't doing that, needs testing
chain.write_output("xlsx", "chain multi record output", [2, 3])

# # Re-enable logging
# logging.disable(logging.NOTSET)
