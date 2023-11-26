from Prepper import Prepper
from Chain import Chain
from RegexSanitiser import RegexSanitiser
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
chain.append(title='prepared data', data=prepared_data.output_data, table=prepared_data.df)

# Running the regex sanitiser
sanitised = RegexSanitiser(chain.latest_data)
sanitised.select_groups(['ip', 'card', 'date', 'email', 'path', 'url', 'number'])
sanitised.sanitise()

# Leaving out table means the previous one is referenced
chain.append(title='sanitised data', data=sanitised.output_data)

print(chain.latest_key)
print(chain.latest_title)
print(chain.record_delta(chain.latest_key))
ic(prepared_data.id_column)
ic(prepared_data.data_columns)
print(chain.latest_data)
