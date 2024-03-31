from functions.logs import BaseScriptLogger, UnhandledErrorsLogger
from Sanitiser import RegexSanitiser
from Handler import LLMHandler
from Prepper import Prepper
from Chain import Chain
import validators
import traceback
import os

logger = BaseScriptLogger().setup()
unhandled_errors_logger = UnhandledErrorsLogger().setup()

current_file_name = os.path.basename(__file__)


def log_start(process_name: str) -> str:
    """Shorthand function to log the start of a process"""
    logger.info(f"Started: {process_name}")
    return process_name


def log_end(process_name: str):
    """Shorthand function to log the end of a process"""
    logger.info(f"Finished: {process_name}")


# Try, Except over everything to log any errors
try:
    p1 = log_start(current_file_name)

    # Set the source file path
    source_file = "scratch/Womens clothing reviews/Womens Clothing E-Commerce Reviews - 100.csv"
    source_file_type = source_file.split(".")[-1]

    logger.info("Set file vars")

    p2 = log_start('Chain initialisation')
    chain = Chain()
    log_end(p2)

    p2 = log_start('Prepper')
    prepared_data = (Prepper()
                     .load_data(source_file, source_file_type, encoding="ISO-8859-1")
                     .set_id_column('Row ID')
                     .set_data_columns(['Review Text'])
                     .set_data_dict())
    log_end(p2)

    p2 = log_start('Appending prepper data')
    chain.append(
        title='prepared data',
        data=prepared_data.output_data,
        table=prepared_data.df,
        table_id_column=prepared_data.id_column,
        column_names=prepared_data.data_columns)
    log_end(p2)

    p2 = log_start('Sanitiser')
    sanitised = RegexSanitiser(chain.latest_data) \
        .select_groups(['date', 'email', 'path', 'url', 'number']) \
        .sanitise()
    log_end(p2)

    chain.append(title='sanitised data', data=sanitised.output_data)  # Other arguments are inherited
    logger.info("Appended sanitised data to chain")

    p2 = log_start('LLMHandler initialisation')
    handler = LLMHandler(chain.latest_data,
                         provider="OpenAI",
                         model="gpt-3.5-turbo",
                         role="An expert customer feedback analyst nlp system",
                         request="Analyse the feedback and return results in the correct format",
                         validation_model=validators.SubCategoriesWithPerItemSentimentAndOverallSentiment,
                         cache_identifier='NLPPerCategorySentimentAndOverallSentimentWomensClothesReview',
                         use_cache=False,
                         temperature=0.2,
                         max_validation_retries=3
                         )
    log_end(p2)

    p2 = log_start('Running handler')
    handler.run()
    log_end(p2)

    p2 = log_start('Handler data flattening')
    handler.flatten_output_data(column_names=chain.latest_column_names)
    log_end(p2)

    p2 = log_start('Appending handler output')
    chain.append(
        title='llmoutput',
        data=handler.output_data,
        column_names=handler.column_names,
        update_expected_len=True)  # Updating len since the validation model can produce multiple columns per input
    log_end(p2)

    p2 = log_start('Creating output file(s)')
    chain.output(
        records=[chain.latest_title],
        file_type='xlsx',
        name_prefix='Chain Test',
        rejoin=True,
        split=False)
    log_end(p2)

    log_end(p1)

# Logging any unhandled exceptions
except Exception as error:
    # Handled exceptions should have the name 'HandledError' (see log_and_raise_error())
    # So if the exception has this name, just re-raise it
    if type(error).__name__ == 'HandledError':
        raise
    # Other log the unhandled error as critical and then re_raise
    else:
        traceback_message: bool = False
        # Conditionally log a more detailed message with the full traceback appended
        if traceback_message:
            error_message = f"{type(error).__name__}\n{traceback.format_exc()}#ENDOFLOG#"
        else:
            error_message = str(error)
        # Log the message as CRITICAL
        unhandled_errors_logger.critical(error_message)
        raise

# TODO - Example class to categorise into existing schema
# TODO - Finalise project structure - i.e. directory organisation, where to put scripts vs classes
# TODO - Make python environment / imports work like a VENV - so it can be set up easily on other machines
# TODO - Create a front-end
