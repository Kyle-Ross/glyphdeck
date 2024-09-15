import logging

# Disable logging for duration
logging.disable(logging.CRITICAL)

from icecream import ic  # noqa: E402
import copy  # noqa: E402

import pandas as pd  # noqa: E402

from CategoriGen.processors.chain import Chain  # noqa: E402
from CategoriGen.validation.data_types import Data  # noqa: E402

# Example data with targets for removal
data_record1: Data = {
    1: [
        r"Record One! - I like apple bottom jeans 156.a19878, 11/10/2020, jimbo@gmail.com",
        "My birthday is 11/10/2021",
        "Product info: https://t.co/KNkANrdypk \r\r\nTo order",
    ],
    2: [
        "Nothing wrong with this",
        "My email is jeff@babe.com, my ip address is 192.158.1.38",
        "Go to this website: www.website.com.au",
    ],
    3: [
        r"Big number is 1896987, I store my files in C:\Users\username\Documents\GitHub",
        "I like blue jeans, my card number is 2222 4053 4324 8877",
        r"I was born 15/12/1990, a file path is C:\Users\username\Pictures\BYG0Djh.png",
    ],
}

data_record2: Data = {
    1: [
        r"Record Two! - I like apple bottom jeans 156.a19878, 11/10/2020, jimbo@gmail.com",
        "My birthday is 11/10/2021",
        "Product info: https://t.co/KNkANrdypk \r\r\nTo order",
    ],
    2: [
        "Nothing wrong with this",
        "My email is jeff@babe.com, my ip address is 192.158.1.38",
        "Go to this website: www.website.com.au",
    ],
    3: [
        r"Big number is 1896987, I store my files in C:\Users\username\Documents\GitHub",
        "I like blue jeans, my card number is 2222 4053 4324 8877",
        r"I was born 15/12/1990, a file path is C:\Users\username\Pictures\BYG0Djh.png",
    ],
}

# Create the test DataFrame
test_df = pd.DataFrame.from_dict(data_record1, orient="index")
test_df = test_df.reset_index()  # Adds the index as a column
test_df.columns = ["Person Id", "Comment1", "Comment2", "Comment3"]  # Rename cols

# (Record Key 1)
# Initialise, preparing the first record
chain = Chain(
    test_df,
    "Person Id",
    ["Comment1", "Comment2", "Comment3"],
    # encoding="utf-8",  # Needed if you provided a .csv path
    # sheet_name="Sheet1"  # Needed if you provided a .xlsx path
)

# (record key 2)
# The table only needs to be added in the first step, but can be included again to add a new one
# Otherwise the table will be the last time 'table' was assigned, or the original source data as a dataframe
chain.append(title="data_record2", data=data_record2)

# Making a copy of the chain for testing a custom set up below, rather than the default values
chain_custom = copy.deepcopy(chain)

# With the default settings
print("\nRunning Sanitiser WIHTOUT customisations, using all pattern groups as default")
print("Before sanitisation")
ic(chain.latest_data)
print("After sanitisation")
ic(chain.sanitiser.run())
ic(chain.latest_data)

# With the custom settings
print(
    "\nRunning Sanitiser WITH customisations, using specified pattern groups as default"
)
print("Before updating pattern groups")
ic(chain_custom.sanitiser.active_groups)
ic(
    chain_custom.sanitiser.select_groups(
        [
            "date",
        ]
    )
)
print("After updating pattern groups")
ic(chain_custom.sanitiser.active_groups)
print("Before sanitisation")
ic(chain_custom.latest_data)
print("After sanitisation")
ic(chain_custom.sanitiser.run())
ic(chain_custom.latest_data)

# Re-enable logging
logging.disable(logging.NOTSET)
