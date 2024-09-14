import logging

# Disable logging for duration
logging.disable(logging.CRITICAL)

import pandas as pd  # noqa: E402
from icecream import ic  # noqa: E402

from CategoriGen.validation.data_types import Data  # noqa: E402
from CategoriGen.processors.chain import Chain  # noqa: E402
from CategoriGen.validation import validators  # noqa: E402

test_data: Data = {
    1: ["berries", "insects", "small mammals"],
    2: ["fruits", "fish", "seeds"],
    3: ["nuts", "leaves", "grubs"],
}

# Create the test DataFrame
test_df = pd.DataFrame.from_dict(test_data, orient="index")
test_df = test_df.reset_index()  # Adds the index as a column
test_df.columns = ["Word ID", "Word1", "Word2", "Word3"]  # Rename cols

print("\n(Record Key 1)")
print("Chain initialisation and first record from that")
# Initialise, preparing the first record
chain = Chain(
    test_df,
    "Word ID",
    ["Word1", "Word2", "Word3"],
    # encoding="utf-8",  # Needed if you provided a .csv path
    # sheet_name="Sheet1"  # Needed if you provided a .xlsx path
)
ic(chain.latest_data)

print("\n(Record Key 2)")
print("Appending record Example1")
# The table only needs to be added in the first step, but can be included again to add a new one
# Otherwise the table will be the last time 'table' was assigned, or the original source data as a dataframe
chain.append(
    title="Example1",
    data={
        1: ["plankton", "small fish", "seaweed"],
        2: ["algae", "crustaceans", "sea vegetables"],
        3: ["aquatic plants", "larvae", "mollusks"],
    },
)
ic(chain.latest_data)

print("\n(Record Key 3)")
print("Initialise the llm_handler, which uses chain.latest_data by default")
# Initialise the llm_handler
chain.set_llm_handler(
    provider="OpenAI",
    model="gpt-3.5-turbo",
    role="An expert word categorisation system",
    request="Analyse the words and provide a primary category representing the animal most likely to eat them",
    validation_model=validators.PrimaryCategory,
    cache_identifier="chain_llm_print_test_primary_category",
    use_cache=False,
    temperature=0.2,
    max_validation_retries=3,
    max_preprepared_coroutines=10,
)

print(
    "Run the llm_handler (on latest_data, aka record 2), which creates and appends a record with the results"
)
chain.llm_handler.run()
ic(chain.latest_data)

print("\n(Record Key 4)")
print(
    "Change the llm_handler input_data to something specific, like the data in Record 1"
)
print("Second arg in use_selected sets the column_names")
ic(chain.llm_handler.new_column_names)
ic(chain.llm_handler.use_selected(chain.data(1)), ["Type1", "Type2", "Type3"])
ic(chain.llm_handler.new_column_names)
ic(chain.llm_handler.input_data)
print("Run it again")
print("Note: As of 2024-09-09 this may incorrectly re-use completions found in the cache, despite the input_data changing!")  # TODO
chain.llm_handler.run()
ic(chain.latest_data)

# Re-enable logging
logging.disable(logging.NOTSET)
