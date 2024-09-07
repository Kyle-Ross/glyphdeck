from icecream import ic
import logging

from CategoriGen.processors.prepper import Prepper

# Disable logging for duration
logging.disable(logging.CRITICAL)

test_file = r"F:\Github\CategoriGen\scratch\Kaggle - Coronavirus tweets NLP - Text Classification\Corona_NLP_train - 10.csv"

print("\nInitialise the prepper object")
print("Columns not specified are removed")
prepper = ic(
    Prepper(
        file_path=test_file,
        file_type="csv",
        encoding="ISO-8859-1",
        id_column="UserName",
        data_columns=["OriginalTweet", "Location"],
    )
)

print("\nPrepare the data")
ic(prepper.prepare())

print("\nPrepper Attributes & Properties")
ic(prepper.output_data)
ic(prepper.df)
ic(prepper.id_column)
ic(prepper.data_columns)

# Re-enable logging
logging.disable(logging.NOTSET)
