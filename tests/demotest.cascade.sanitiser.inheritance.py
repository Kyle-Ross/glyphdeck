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
print("\nInitialise cascade (Record Key 1)")
cascade = Cascade(
    test_df,
    "Word ID",
    ["Word1", "Word2", "Word3"],
    # encoding="utf-8",  # Needed if you provided a .csv path
    sheet_name="Sheet1",  # Needed if you provided a .xlsx path
)
ic(cascade.record(1))
ic(cascade.latest_data)
ic(cascade.sanitiser.input_data)
ic(cascade.base_sanitiser.input_data)
ic("select_groups() - used in init")
ic(cascade.sanitiser.all_groups)
ic(cascade.sanitiser.active_groups)
ic(cascade.sanitiser.inactive_groups)
ic(cascade.base_sanitiser.all_groups)
ic(cascade.base_sanitiser.active_groups)
ic(cascade.base_sanitiser.inactive_groups)

# (record key 2)
# The table only needs to be added in the first step, but can be included again to add a new one
# Otherwise the table will be the last time 'table' was assigned, or the original source data as a dataframe
print("\nAppend Record, Example1 (Record Key 2)")
cascade.append(
    title="Example1",
    data={
        1: ["potato", "steak", "party"],
        2: ["carrot", "party", "alpha"],
        3: ["carrot", "party", "alpha"],
    },
)
ic(cascade.record(2))
ic(cascade.latest_data)
ic(cascade.sanitiser.input_data)
ic(cascade.sanitiser.select_groups(["number"]))
ic(cascade.sanitiser.all_groups)
ic(cascade.sanitiser.active_groups)
ic(cascade.sanitiser.inactive_groups)
ic(cascade.base_sanitiser.input_data)
ic(cascade.base_sanitiser.select_groups(["number"]))
ic(cascade.base_sanitiser.all_groups)
ic(cascade.base_sanitiser.active_groups)
ic(cascade.base_sanitiser.inactive_groups)

# (record key 3)
# Since table is not assigned table will just be the last table
print("\nAppend Record, Example2 (Record Key 3)")
cascade.append(
    title="Example2",
    data={
        1: ["potatoes", "carrot", "gary"],
        2: ["carrots", "pizza", "pasta"],
        3: ["bananas", "beast", "jeffery"],
    },
)
ic(cascade.record(3))
ic(cascade.latest_data)
ic(cascade.sanitiser.input_data)
ic(cascade.sanitiser.select_groups(["number", "date"]))
ic(cascade.sanitiser.all_groups)
ic(cascade.sanitiser.active_groups)
ic(cascade.sanitiser.inactive_groups)
ic(cascade.base_sanitiser.input_data)
ic(cascade.base_sanitiser.select_groups(["number", "date"]))
ic(cascade.base_sanitiser.all_groups)
ic(cascade.base_sanitiser.active_groups)
ic(cascade.base_sanitiser.inactive_groups)

# Print off all the generated values
# Without a loop so ic will show the contents

print("\nIs the sanitiser referencing the correct cascade?")
ic(cascade)
ic(cascade.base_sanitiser.outer_cascade)
ic(cascade == cascade.base_sanitiser.outer_cascade)

print(
    "\nAppending with the sanitiser representation of the cascade shows changes in the normal cascade (Record Key 4)"
)
# (Record Key 4)
cascade.sanitiser.outer_cascade.append(
    title="Via Sanitiser",
    data={
        1: ["Yellow", "Blue", "Green"],
        2: ["Red", "Purple", "Brown"],
        3: ["Black", "White", "Grey"],
    },
)
ic(cascade.record(4))
ic(cascade.latest_data)
ic(cascade.sanitiser.input_data)
ic(cascade.base_sanitiser.input_data)

print("\nSanitiser input_data can be changed directly")
ic(cascade.sanitiser.input_data)

print("Setting target input to record 1")
print("cascade.sanitiser.use_selected = True")
cascade.sanitiser.use_selected = True
print("cascade.sanitiser.selected_data = cascade.record(1)")
cascade.sanitiser.selected_data = cascade.record(1)

ic(cascade.latest_data)
ic(cascade.sanitiser.selected_data)
ic(cascade.sanitiser.input_data)

# Attributes and properites of the whole cascade
print("\nThe whole cascade")
ic(cascade.records)

# Re-enable logging
logging.disable(logging.NOTSET)
