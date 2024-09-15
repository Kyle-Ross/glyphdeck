import pandas as pd  # noqa: E402

from CategoriGen.validation.data_types import Data  # noqa: E402
from CategoriGen.processors.chain import Chain  # noqa: E402

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
chain = Chain(
    meals_df,
    "Meal Id",
    ["Meal1", "Meal2", "Meal3"],
)

chain.write_output()
