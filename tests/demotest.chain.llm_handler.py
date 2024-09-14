import logging

# Disable logging for duration
logging.disable(logging.CRITICAL)

import pandas as pd  # noqa: E402
from icecream import ic  # noqa: E402

from CategoriGen.validation.data_types import Data  # noqa: E402
from CategoriGen.processors.chain import Chain  # noqa: E402
from CategoriGen.validation import validators  # noqa: E402

meals_dict: Data = {
    1: ["Sushi", "Ramen", "Tempura"],
    2: ["Paella", "Tapas", "Churros"],
    3: ["Pizza", "Pasta", "Gelato"],
    4: ["Biryani", "Naan", "Butter Chicken"],
}

# Create the test DataFrame
meals_df = pd.DataFrame.from_dict(meals_dict, orient="index")
meals_df = meals_df.reset_index()  # Adds the index as a column
meals_df.columns = ["Meal Id", "Meal1", "Meal2", "Meal3"]  # Rename cols

print("\n(Record Key 1)")
print("Chain initialisation and first record from that")
# Initialise, preparing the first record
print("chain = ...")
chain = ic(
    Chain(
        meals_df,
        "Meal Id",
        ["Meal1", "Meal2", "Meal3"],
        # encoding="utf-8",  # Needed if you provided a .csv path
        # sheet_name="Sheet1"  # Needed if you provided a .xlsx path
    )
)
ic(chain.latest_data)

# -------------------------------------------------------------------------

print("\n(Record Key 2)")
print("Appending record meals_data2")
# The table only needs to be added in the first step, but can be included again to add a new one
# Otherwise the table will be the last time 'table' was assigned, or the original source data as a dataframe
ic(
    chain.append(
        title="meals_data2",
        data={
            1: ["Tacos", "Guacamole", "Churros"],
            2: ["Falafel", "Hummus", "Baklava"],
            3: ["Samosas", "Biryani", "Lassi"],
            4: ["Goulash", "Paprikash", "Langos"],
        },
    )
)
ic(chain.latest_data)

print("\nInitialise the llm_handler, which uses chain.latest_data by default")
# Initialise the llm_handler
ic(
    chain.set_llm_handler(
        provider="OpenAI",
        model="gpt-3.5-turbo",
        role="An expert food country identication system",
        request="Analyse the foods and provide a primary category representing their country of origin",
        validation_model=validators.PrimaryCategory,
        cache_identifier="chain_llm_print_test_primary_category_food_country",
        use_cache=True,
        temperature=0.2,
        max_validation_retries=3,
        max_preprepared_coroutines=10,
    )
)
ic(chain.llm_handler.active_input_data)
ic(chain.llm_handler.active_record_title)

# -------------------------------------------------------------------------

print("\n(Record Key 3)")
print(
    "Appending record meals_data3, after initialising the handler. The handler instance's internal representation of the input data also updates"
)
ic(
    chain.append(
        title="meals_data3",
        data={
            1: ["Jollof Rice", "Suya", "Egusi Soup"],
            2: ["Ceviche", "Lomo Saltado", "Pisco Sour"],
            3: ["Gyros", "Souvlaki", "Baklava"],
            4: ["Pho", "Spring Rolls", "Banh Mi"],
        },
    )
)
ic(chain.latest_data)
ic(chain.llm_handler.active_record_title)
ic(chain.llm_handler.active_input_data)

print("\n(Record Key 4)")
print(
    "Run the llm_handler (on latest_data, aka record 3), which creates and appends a record with the results"
)
ic(chain.llm_handler.run("HandlerOutput1"))
ic(chain.latest_data)

# -------------------------------------------------------------------------

print("\n(Record Key 5)")
print(
    """Change the selected llm_handler data to something specific,
like a specifed Data object, or a literal dictionary"""
)
print("Set the target data manually (data, record_title, column_names)")
ic(chain.llm_handler.active_column_names)
ic(
    chain.llm_handler.use_selection(
        {
            1: ["Pad Thai", "Tom Yum Goong", "Mango Sticky Rice"],
            2: ["Cassoulet", "Coq au Vin", "Croissants"],
            3: ["Kebabs", "Dolma", "Baklava"],
            4: ["Pierogi", "Zurek", "Bigos"],
        },
        "meals_data4 - use selection",
        # Not specifying the column names means the latest column names will be used
    )
)
ic(chain.llm_handler.active_record_title)
ic(chain.llm_handler.active_column_names)
ic(chain.llm_handler.active_input_data)
print("\nRun it again, with the new data")
print(
    "Note: As of 2024-09-09 this may incorrectly re-use completions found in the cache, despite the input_data changing!"
)  # TODO
ic(chain.llm_handler.run("HandlerOutput2"))
ic(chain.latest_data)

# -------------------------------------------------------------------------

print("\n(Record Key 6)")
print(
    """Select data for the llm_handler using only a record key or title, like record 1"""
)
ic(chain.llm_handler.use_record(1))
ic(chain.llm_handler.active_record_title)
ic(chain.llm_handler.active_input_data)
print("\nRun it again, with the data selected with the record id")
ic(chain.llm_handler.run("HandlerOutput3"))
ic(chain.latest_data)

print("\nSwap it back to using the latest data")
ic(chain.llm_handler.use_latest())
ic(chain.llm_handler.active_record_title)
ic(chain.llm_handler.active_record_key)
ic(chain.llm_handler.active_column_names)
ic(chain.llm_handler.active_input_data)

# Re-enable logging
logging.disable(logging.NOTSET)