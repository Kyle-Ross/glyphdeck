from utility import time_since_start
from Sanitiser import RegexSanitiser
from LLMHandler import LLMHandler
import type_models
from Prepper import Prepper
from Chain import Chain

print_progress = True


def prog_print(message: str):
    """Prints a message if a variable is true"""
    if print_progress:
        delta_time = time_since_start()
        print(f"({delta_time}) {message} ")


prog_print("Started Script")

# Set file variables
source_file = "scratch/Womens clothing reviews/Womens Clothing E-Commerce Reviews - 100.csv"
source_file_type = source_file.split(".")[-1]

prog_print("Set file vars")

# Preparing data
prog_print("Starting data prepper")
prepared_data = (Prepper()
                 .load_data(source_file, source_file_type, encoding="ISO-8859-1")
                 .set_id_column('Row ID')
                 .set_data_columns(['Review Text'])
                 .set_data_dict())
prog_print("Finished data prepper")

# Initialising chain instance and appending the prepared data
prog_print("Starting chain initialisation and appending of prep data")
chain = Chain()
chain.append(
    title='prepared data',
    data=prepared_data.output_data,
    table=prepared_data.df,
    table_id_column=prepared_data.id_column,
    column_names=prepared_data.data_columns)
prog_print("Finished chain initialisation and appending of prep data")

# Running the regex sanitiser
prog_print("Starting regex sanitiser")
sanitised = RegexSanitiser(chain.latest_data) \
    .select_groups(['date', 'email', 'path', 'url', 'number']) \
    .sanitise()
prog_print("Finished regex sanitiser")

# Leaving out table means the previous one is referenced
chain.append(title='sanitised data', data=sanitised.output_data)
prog_print("Appended sanitised data to chain")

prog_print("Starting initialisation of LLMHandler class")
# Initialising the llm handler
handler = LLMHandler(chain.latest_data,
                     provider="OpenAI",
                     model="gpt-3.5-turbo",
                     role="An expert customer feedback analyst nlp system",
                     request="Analyse the feedback and return results in the correct format",
                     validation_model=type_models.SubCategoriesWithPerItemSentimentAndOverallSentiment,
                     cache_identifier='NLPPerCategorySentimentAndOverallSentimentWomensClothesReview',
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
chain.append(
    title='llmoutput',
    data=handler.output_data,
    column_names=handler.column_names,
    update_expected_len=True)  # Updating len since the validation model can produce multiple columns per input
prog_print("Finished appending llm output")

prog_print("Starting creating output file(s)")
chain.output(
    records=[chain.latest_title],
    file_type='xlsx',
    name_prefix='Chain Test',
    rejoin=True,
    split=True)
prog_print("Finished creating output file(s)")

prog_print("Finished Script")

# TODO - Make script use environment variable for api key, remove all use of json secret method
# TODO - Add togglable log / print functionality
# TODO - Class to categorise into existing schema
# TODO - Script - Auto category schema generator logic - which then loops back to categorise against that scheme
