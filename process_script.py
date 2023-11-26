from Prepper import Prepper
from Chain import Chain
from icecream import ic

# Set file variables
source_file = "scratch/Kaggle - Coronavirus tweets NLP - Text Classification/Corona_NLP_train.csv"
source_file_type = source_file.split(".")[-1]

# Preparing data
prepared_data = (Prepper()
                 .load_data(source_file, source_file_type, encoding="ISO-8859-1")
                 .set_id_column('UserName')
                 .set_data_columns(['OriginalTweet', 'Location'])
                 .set_data_dict())

# Initialising chain instance and appending the prepared data
chain = Chain()
chain.append(
    title='prepared data',
    data=prepared_data.data_dict,
    table=prepared_data.df
)

print(chain.latest_key)
print(chain.latest_title)
print(chain.record_delta(chain.latest_key))
ic(prepared_data.id_column)
ic(prepared_data.data_columns)
print(chain.latest_data)

