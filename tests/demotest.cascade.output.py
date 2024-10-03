import logging

# Disable logging for duration
logging.disable(logging.CRITICAL)

import pandas as pd  # noqa: E402
from icecream import ic  # noqa: E402

import glyphdeck as gd  # noqa: E402

# Establish test data
test_data = {
    1: ["base", "data", "which"],
    2: ["the", "cascade", "was"],
    3: ["created", "with", "ok"],
}

# Create the test DataFrame
test_df = pd.DataFrame.from_dict(test_data, orient="index")
test_df = test_df.reset_index()  # Adds the index as a column
test_df.columns = ["Word ID", "Word1", "Word2", "Word3"]  # Rename cols

print("\nAppended data as record 1 in cascade __init__")
cascade = gd.Cascade(
    test_df,
    "Word ID",
    ["Word1", "Word2", "Word3"],
    # encoding="utf-8",  # Needed if you provided a .csv path
    sheet_name="Sheet1",  # Needed if you provided a .xlsx path
)
ic(cascade.record(1))

print("\nAppended record 2 in cascade __init__")
cascade.append(
    title="Record2",
    data={
        1: ["potato", "steak", "party"],
        2: ["carrot", "party", "alpha"],
        3: ["carrot", "party", "alpha"],
    },
)
ic(cascade.record(2))

print("\nAppended record 3")
cascade.append(
    title="Record3",
    data={
        1: ["jeff", "sam", "legend"],
        2: ["alpha", "sigma", "oppa"],
        3: ["senpai", "relive", "septum"],
    },
)
ic(cascade.record(3))

print("\n\nxlsx write_output() usages\n")

print("\nDefault xlsx and csv take the latest record and rebase it")
ic(cascade.write_output("xlsx", "cascade-xlsx-default-args"))
ic(cascade.write_output("csv", "cascade-csv-default-args"))
print("each file has the title of the latest record in the path")
print("behaviour is the same when a single record is specified")

print("\nWhen we specify multiple records, eg [2, 3]")

print("\nxlsx, multi record, rebase=False, combine=False, xlsx_use_sheets=False")
ic(
    cascade.write_output(
        file_type="xlsx",
        file_name_prefix="cascade-xlsx-rebase=False-combine=False-sheets=False",
        record_identifiers=[2, 3],
        rebase=False,
        combine=False,
        xlsx_use_sheets=False,
    )
)
print(
    "Result - Multiple xlsx files, one for each record, with no changes. Each with title of record in path."
)

print("\nxlsx, multi record, rebase=True, combine=False, xlsx_use_sheets=False")
ic(
    cascade.write_output(
        file_type="xlsx",
        file_name_prefix="cascade-xlsx-rebase=True-combine=False-sheets=False",
        record_identifiers=[2, 3],
        rebase=True,
        combine=False,
        xlsx_use_sheets=False,
    )
)
print("Result - Same as before, with the base dataframe added back on")

print("\nxlsx, multi record, rebase=True, combine=True, xlsx_use_sheets=False")
ic(
    cascade.write_output(
        file_type="xlsx",
        file_name_prefix="cascade-xlsx-rebase=True-combine=True-sheets=False",
        record_identifiers=[2, 3],
        rebase=True,
        combine=True,
        xlsx_use_sheets=False,
    )
)
print(
    "Result - All records are output in the same table, combined with the base dataframe added back on"
)

print("\nxlsx, multi record, rebase=True, combine=False, xlsx_use_sheets=True")
ic(
    cascade.write_output(
        file_type="xlsx",
        file_name_prefix="cascade-xlsx-rebase=True-combine=False-sheets=True",
        record_identifiers=[2, 3],
        rebase=True,
        combine=False,
        xlsx_use_sheets=True,
    )
)
print(
    "Result - Each record is rebased individually and put in its own sheet of a single xlsx"
)

print("\ncsv, multi record, rebase=False, combine=False")
ic(
    cascade.write_output(
        file_type="csv",
        file_name_prefix="cascade-csv-rebase=False-combine=False",
        record_identifiers=[2, 3],
        rebase=False,
        combine=False,
    )
)
print("Result - Each record is put in its own csv")

print("\ncsv, multi record, rebase=True, combine=False")
ic(
    cascade.write_output(
        file_type="csv",
        file_name_prefix="cascade-csv-rebase=True-combine=False",
        record_identifiers=[2, 3],
        rebase=True,
        combine=False,
    )
)
print("Result - Each record is rebased individually and put in its own csv")

print("\ncsv, multi record, rebase=True, combine=True")
ic(
    cascade.write_output(
        file_type="csv",
        file_name_prefix="cascade-csv-rebase=True-combine=True",
        record_identifiers=[2, 3],
        rebase=True,
        combine=True,
    )
)
print(
    "Result - Records are combined in the same table and the base dataframe is added back on"
)

# Re-enable logging
logging.disable(logging.NOTSET)
