import logging

# Disable logging for duration
logging.disable(logging.CRITICAL)

import pandas as pd  # noqa: E402
from icecream import ic  # noqa: E402

from CategoriGen.processors.chain import Chain  # noqa: E402

# Establish test data
test_data = {
    1: ["base", "data", "which"],
    2: ["the", "chain", "was"],
    3: ["created", "with", "ok"],
}

# Create the test DataFrame
test_df = pd.DataFrame.from_dict(test_data, orient="index")
test_df = test_df.reset_index()  # Adds the index as a column
test_df.columns = ["Word ID", "Word1", "Word2", "Word3"]  # Rename cols

print("\nAppended data as record 1 in chain __init__")
chain = Chain(
    test_df,
    "Word ID",
    ["Word1", "Word2", "Word3"],
    # encoding="utf-8",  # Needed if you provided a .csv path
    sheet_name="Sheet1",  # Needed if you provided a .xlsx path
)
ic(chain.record(1))

print("\nAppended record 2 in chain __init__")
chain.append(
    title="Record2",
    data={
        1: ["potato", "steak", "party"],
        2: ["carrot", "party", "alpha"],
        3: ["carrot", "party", "alpha"],
    },
)
ic(chain.record(2))

print("\nAppended record 3")
chain.append(
    title="Record3",
    data={
        1: ["jeff", "sam", "legend"],
        2: ["alpha", "sigma", "oppa"],
        3: ["senpai", "relive", "septum"],
    },
)
ic(chain.record(3))

print("\n\nxlsx write_output() usages\n")

print("\nDefault xlsx and csv take the latest record and rebase it")
ic(chain.write_output("xlsx", "chain-xlsx-default-args"))
ic(chain.write_output("csv", "chain-csv-default-args"))
print("each file has the title of the latest record in the path")
print("behaviour is the same when a single record is specified")

print("\nWhen we specify multiple records, eg [2, 3]")

print("\nxlsx, multi record, rebase=False, combine=False, xlsx_use_sheets=False")
ic(
    chain.write_output(
        file_type="xlsx", 
        file_name_prefix="chain-xlsx-rebase=False-combine=False-sheets=False", 
        record_keys=[2, 3],
        rebase=False,
        combine=False,
        xlsx_use_sheets=False
    )
)
print("Result - Multiple xlsx files, one for each record, with no changes. Each with title of record in path.")

print("\nxlsx, multi record, rebase=True, combine=False, xlsx_use_sheets=False")
ic(
    chain.write_output(
        file_type="xlsx", 
        file_name_prefix="chain-xlsx-rebase=True-combine=False-sheets=False", 
        record_keys=[2, 3],
        rebase=True,
        combine=False,
        xlsx_use_sheets=False
    )
)
print("Result - Same as before, with the base dataframe added back on")

print("\nxlsx, multi record, rebase=True, combine=True, xlsx_use_sheets=False")
ic(
    chain.write_output(
        file_type="xlsx", 
        file_name_prefix="chain-xlsx-rebase=True-combine=True-sheets=False", 
        record_keys=[2, 3],
        rebase=True,
        combine=True,
        xlsx_use_sheets=False
    )
)
print("Result - All records are output in the same table, combined with the base dataframe added back on")

print("\nxlsx, multi record, rebase=True, combine=False, xlsx_use_sheets=True")
ic(
    chain.write_output(
        file_type="xlsx", 
        file_name_prefix="chain-xlsx-rebase=True-combine=False-sheets=True" ,
        record_keys=[2, 3],
        rebase=True,
        combine=False,
        xlsx_use_sheets=True
    )
)
print("Result - Each record is rebased individually and put in its own sheet of a single xlsx")

print("\ncsv, multi record, rebase=False, combine=False")
ic(
    chain.write_output(
        file_type="csv", 
        file_name_prefix="chain-csv-rebase=False-combine=False", 
        record_keys=[2, 3],
        rebase=False,
        combine=False,
    )
)
print("Result - Each record is put in its own csv")

print("\ncsv, multi record, rebase=True, combine=False")
ic(
    chain.write_output(
        file_type="csv", 
        file_name_prefix="chain-csv-rebase=True-combine=False", 
        record_keys=[2, 3],
        rebase=True,
        combine=False,
    )
)
print("Result - Each record is rebased individually and put in its own csv")

print("\ncsv, multi record, rebase=True, combine=True")
ic(
    chain.write_output(
        file_type="csv", 
        file_name_prefix="chain-csv-rebase=True-combine=True", 
        record_keys=[2, 3],
        rebase=True,
        combine=True,
    )
)
print("Result - Records are combined in the same table and the base dataframe is added back on")

# Re-enable logging
logging.disable(logging.NOTSET)
