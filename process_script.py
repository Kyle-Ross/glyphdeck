from utility import time_since_start
from RegexSanitiser import RegexSanitiser
from LLMHandler import LLMHandler
import type_models
from Prepper import Prepper
from Chain import Chain
from icecream import ic
import json

print_progress = True


def prog_print(message: str):
    """Prints a message if a variable is true"""
    if print_progress:
        delta_time = time_since_start()
        print(f"({delta_time}) {message} ")


prog_print("Started Script")

# Set file variables
source_file = "scratch/Kaggle - Coronavirus tweets NLP - Text Classification/Corona_NLP_train - 1000.csv"
source_file_type = source_file.split(".")[-1]

prog_print("Set file vars")

# Preparing data
prog_print("Starting data prepper")
prepared_data = (Prepper()
                 .load_data(source_file, source_file_type, encoding="ISO-8859-1")
                 .set_id_column('UserName')
                 .set_data_columns(['OriginalTweet', 'Location'])
                 .set_data_dict())
prog_print("Finished data prepper")

# Initialising chain instance and appending the prepared data
prog_print("Starting chain initialisation and appending of prep data")
chain = Chain()
chain.append(
    title='prepared data',
    data=prepared_data.output_data,
    table=prepared_data.df,
    column_names=prepared_data.data_columns)
prog_print("Finished chain initialisation and appending of prep data")

# Running the regex sanitiser
prog_print("Starting regex sanitiser")
sanitised = RegexSanitiser(chain.latest_data) \
    .select_groups(['ip', 'card', 'date', 'email', 'path', 'url', 'number']) \
    .sanitise()
prog_print("Finished regex sanitiser")

# Leaving out table means the previous one is referenced
chain.append(title='sanitised data', data=sanitised.output_data)
prog_print("Appended sanitised data to chain")

# Access the API key from the local secrets file
with open("secrets.json") as file:
    my_api_key = json.load(file)["openai_api_key"]
prog_print("Set OpenAI API key")

prog_print("Starting initialisation of LLMHandler class")
# Initialising the llm handler
handler = LLMHandler(chain.latest_data,
                     provider="OpenAI",
                     model="gpt-3.5-turbo",
                     role="An expert customer feedback analyst nlp system",
                     request="Analyse the feedback and return results in the correct format",
                     validation_model=type_models.SentimentScore,
                     cache_identifier='NLP_SentimentScore_Twitter',
                     use_cache=True,
                     temperature=0.2,
                     max_validation_retries=3
                     )
prog_print("Finished initialisation of LLMHandler class")

prog_print("Starting running handler")
# Running the handler
handler.run()
prog_print("Finished running handler")

prog_print("Starting handler data flattening")
handler.flatten_output_data(column_names=chain.latest_column_names)
prog_print("Finished handler data flattening")

prog_print("Starting appending llm output")
# Appending the llm output
chain.append(title='llm output', data=handler.output_data, column_names=handler.column_names)
prog_print("Finished appending llm output")

prog_print("Printing results")
# Printing the results
ic(chain.latest_key)
ic(chain.latest_title)
ic(chain.record_delta(chain.latest_key))
print(chain.latest_data)
prog_print("Finished Script")
