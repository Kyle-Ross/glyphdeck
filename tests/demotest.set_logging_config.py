import pandas as pd  # noqa: E402
from icecream import ic  # noqa: E402

import glyphdeck as gd  # noqa: E402

print("\nChange the logging config")
print("\nComment sections in and out to test changes")

print("\nThis example turns on the defaults")
ic(gd.configure_logging(setting_type="default"))

# print("\nThis example uses set_all to set global levels for all loggers")
# ic(gd.set_logging_config(setting_type="set_all", set_all_levels=(10,10)))

# print("\nThis example turns on granular control and disables sanitiser logging")
# ic(gd.set_logging_config(setting_type="granular", sanitiser_levels=(90,90)))

# print("\nRestores the config to its original values")
# ic(gd.restore_logger_config())

print("\nRun some chain operations...")

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

# Initialise, preparing the first record
cascade = gd.Cascade(
    test_df,
    "Word ID",
    ["Word1", "Word2", "Word3"],
)

print(
    "\nChanges don't take effect until the module is re-initialised, so run script again to see changes"
)
