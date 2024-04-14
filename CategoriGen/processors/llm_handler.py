import os
import asyncio

import instructor
import openai
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from CategoriGen.validation.data_types import Data, assert_custom_type, StrList, Data_or_None, StrList_or_None
from CategoriGen.validation import validators
from CategoriGen.tools.loggers import assert_and_log_error, LLMHandlerLogger
from CategoriGen.tools.strings import string_cleaner
from CategoriGen.tools.caching import openai_cache

logger = LLMHandlerLogger().setup()
logger.debug("Initialised logger_arg")


class LLMHandler:
    """Write your docstring for the class here."""

    def check_validation_model(self):
        """Checks that the provided class is an instance or inheritance of the Pydantic BaseValidatorModel class."""
        check: bool = issubclass(self.validation_model, validators.BaseValidatorModel)
        assert_and_log_error(logger, 'error', check,
                             f'{self.validation_model.__name__} is not a subclass of the '
                             f'Pydantic BaseValidatorModel class')

    def __init__(self,
                 input_data: Data,  # Input variable of the custom 'Data' type
                 provider: str,  # The provider of the llm
                 model: str,  # The model of the llm
                 role: str,  # The role to provide the llm in the prompts
                 request: str,  # The request to make to the llm
                 validation_model,  # Pydantic class to use for validating output, checked by check_validation_model()
                 cache_identifier: str,  # A Unique string used to identify discrete jobs and avoid cache mixing
                 use_cache: bool = True,  # Set whether to check the cache for values or not
                 temperature: float = 0.2,  # How deterministic (low num) or random (high num) the responses will be
                 max_validation_retries: int = 2):  # Max times the request can retry on the basis of failed validation
        """__init__ func which is run when the object is initialised."""

        logger.debug("Function - __init__() - Start - Initialising LLMHandler object")

        # Assert the variable type of the provided arguments
        assert_custom_type(input_data, "Data", "input_data")  # Check the custom data type 'Data'
        assert_and_log_error(logger, 'error', isinstance(provider, str),
                             "'provider' argument must be type 'str'")
        assert_and_log_error(logger, 'error', isinstance(model, str),
                             "'model' argument must be type 'str'")
        assert_and_log_error(logger, 'error', isinstance(role, str),
                             "'role' argument must be type 'str'")
        assert_and_log_error(logger, 'error', isinstance(request, str),
                             "'request' argument must be type 'str'")
        assert_and_log_error(logger, 'error', isinstance(cache_identifier, str),
                             "'cache_identifier' argument must be type 'str'")
        assert_and_log_error(logger, 'error', isinstance(use_cache, bool),
                             "'use_cache' argument must be type 'str'")
        assert_and_log_error(logger, 'error', isinstance(temperature, float),
                             "'temperature' argument must be type 'float'")
        assert_and_log_error(logger, 'error', isinstance(max_validation_retries, int),
                             "'max_validation_retries' argument must be type 'int'")

        # Storing the input variable, of the 'Data' type as typically delivered by a 'Chain' object
        self.input_data: Data = input_data
        # raw_output_data keeps keys, replaces with [None, None, ...] lists
        # Helps to insert output in the correct position later in 'await_tasks'
        self.raw_output_data: Data = {key: [None] * len(value) for key, value in input_data.items()}

        self.new_output_data: Data_or_None = None  # Generated by self.flatten_output_data()
        self.new_column_names: StrList_or_None = None  # Generated by self.flatten_output_data()

        # Storing and Checking available providers against a list
        self.available_providers: tuple = ("openai",)
        self.provider: str = provider
        self.provider_clean: str = string_cleaner(self.provider)
        assert_and_log_error(logger, 'error', self.provider_clean in self.available_providers,
                             f"{self.provider} is not in the list of available providers: "
                             f"\n{self.available_providers}")

        # Object level llm information that serves as the default value if tools don't specify customisations
        self.model: str = model
        self.role: str = role
        self.request: str = request
        self.validation_model = validation_model
        self.cache_identifier: str = cache_identifier  # Referenced in lru_cache by accessing self
        self.use_cache: bool = use_cache  # Referenced in lru_cache by accessing sel
        self.temperature: float = temperature
        self.max_validation_retries = max_validation_retries

        # Checks that model comes from customer Pydantic BaseValidatorModel class
        self.check_validation_model()

        # Preparing openai client
        if self.provider_clean == 'openai':
            openai.api_key = os.getenv("OPENAI_API_KEY")
            logger.debug("Function - __init__() - Action - Set openai api key")

            # Initialising the client
            # instructor patches in variable validation via pydantic with the response_model and max_retries attributes
            self.openai_client = instructor.patch(openai.AsyncOpenAI())
            logger.debug("Function - __init__() - Action - Set openai_client and patched with instructor")

        logger.debug("Function - __init__() - Finish - Initialised LLMHandler object")

    @property
    def output_data(self):
        """Accesses output data but only if the data has been flattened."""
        logger.debug("Function - output_data() - Start - Checking if self.new_output_data is not none, "
                     "indicating data has been flattened")
        assert_and_log_error(logger, 'error', self.new_output_data is not None,
                             "output_data is empty, run self.flatten_output_data() first.")
        logger.debug("Function - output_data() - Finish - Returning self.new_output_data")
        return self.new_output_data

    @property
    def column_names(self):
        """Accesses column names but only if the new names have been generated during data flattening."""
        logger.debug("Function - column_names() - Start - checking if self.new_output_data is not none, "
                     "indicating data has been flattened")
        assert_and_log_error(logger, 'error', self.new_column_names is not None,
                             "column_names is empty, run self.flatten_output_data() first.")
        logger.debug("Function - column_names() - Finish - Returning self.new_column_names")
        return self.new_column_names

    # Tenacity retry decorator for the following errors with exponential backoff
    @retry(
        retry=retry_if_exception_type(
            (openai.APITimeoutError,
             openai.ConflictError,
             openai.InternalServerError,
             openai.UnprocessableEntityError,
             openai.APIConnectionError,
             openai.RateLimitError)
        ),
        # Waits for (sec) 0.9375, 1.875, 3.75, 7.5, 15, 30, 60 (max)
        wait=wait_exponential(multiplier=2, min=0.9375, max=60),
        stop=stop_after_attempt(300))  # About 5 hours of retries!
    @openai_cache('async_openai_cache')  # Can overwrite other args here which otherwise use default values
    async def async_openai(self,
                           input_text: str,
                           key,
                           index: int,
                           item_model: str = None,
                           item_role: str = None,
                           item_request: str = None,
                           item_validation_model=None,
                           item_temperature: float = None,
                           item_max_validation_retries: int = None) -> tuple:
        """Asynchronous Per-item coroutine generation with OpenAI. Has exponential backoff on specified errors."""

        logger.debug("Function - async_openai() - Start")

        # If no arguments are provided, uses the values set in the handler class instance
        # Necessary to do it this way since self is not yet defined in this function definition
        if item_model is None:
            item_model = self.model
        if item_role is None:
            item_role = self.role
        if item_request is None:
            item_request = self.request
        if item_validation_model is None:
            item_validation_model = self.validation_model
        if item_temperature is None:
            item_temperature = self.temperature
        if item_max_validation_retries is None:
            item_max_validation_retries = self.max_validation_retries

        # Asserting value limitations specific to OpenAI
        assert_and_log_error(logger, 'error', 0 <= item_temperature <= 1,
                             "For OpenAI, temperature must be between 0 and 1")

        # Sending the request
        # Having patched with instructor changes the response object...
        # response information is now accessed like item_validation_model.field_name
        chat_params = {
            'model': item_model,
            'response_model': item_validation_model,
            'max_retries': item_max_validation_retries,
            'temperature': item_temperature,
            'messages': [
                {"role": "system", "content": item_role},
                {"role": "user", "content": item_request + ' ' + str(input_text)}
            ]
        }
        logger.debug(f"Function - async_openai() - Action - Set chat_params as: \n {chat_params}")

        # Running the chat completion and saving as an instructor model
        logger.debug("Function - async_openai() - Action - Attempting chat completion")
        instructor_model = await self.openai_client.chat.completions.create(**chat_params)
        logger.debug("Function - async_openai() - Action - Chat completion success, saved to instructor_model")

        # Storing the response object (as made by the patched openai_client)
        # Extracting a dict of the fields using the pydantic basemodel
        response = dict(instructor_model)

        # Returning the response as a tuple (shorthand syntax)
        logger.debug(f"Function - async_openai() - Finish - Returning (response, key, index) tuple: \n"
                     f"({response}, {key}, {index})")
        return response, key, index

    async def create_coroutines(self, func) -> list:
        """Creates coroutines for the provided input data, using the specified LLM function."""
        # Create and store the tasks across the whole variable in a list
        logger.debug(f"Function - create_coroutines() - Start - Creating coroutines")
        coroutines = [func(input_text=item_value, key=key, index=index)  # List of coroutines
                      for key, list_value in self.input_data.items()  # For every key in the input_data dict
                      for index, item_value in enumerate(list_value)]  # For every item in every list
        logger.debug(f"Function - create_coroutines() - Finish - Returning coroutines")
        return coroutines

    async def await_coroutines(self, func):
        """Await coroutines,  returning results in the order of completion, not the order they are run."""
        logger.debug(f"Function - await_coroutines() - Start")
        logger.debug(f"Function - await_coroutines() - Action - Running create_coroutines(func)")
        coroutines = await self.create_coroutines(func)
        logger.debug(f"Function - await_coroutines() - Action - Ran and saved create_coroutines(func)")
        # Loop over the futures
        logger.debug(f"Function - await_coroutines() - Action - Looping over futures of coroutines using as_completed")
        for future in asyncio.as_completed(coroutines):
            logger.debug("Function - await_coroutines() - Action - Inside future loop - Trying to await future")
            result = await future
            logger.debug(f"Function - await_coroutines() - Action - Inside future loop - "
                         f"Successfully awaited future, result is: \n {result}")
            response = result[0]
            key = result[1]
            index = result[2]
            self.raw_output_data[key][index] = response
        logger.debug(f"Function - await_coroutines() - Finish - Looped over futures of coroutines")

    def run(self):
        """Asynchronously query the selected LLM across the whole variable and save results to the output"""
        logger.debug(f"Function - run() - Start")
        if self.provider_clean == "openai":
            asyncio.run(self.await_coroutines(self.async_openai))
        logger.debug(f"Function - run() - Finish - Returning self")
        return self

    def flatten_output_data(self, column_names: StrList):
        """Flattens output data into a dictionary of lists for compatibility with the chain class.
        Also creates the new column names for the eventual output."""
        logger.debug(f"Function - flatten_output_data() - Start")
        new_output_data = {}  # Storage for key: list pairs representing rows
        new_column_names = []  # Storing a list of the column names corresponding to the ordered list

        for row_key, values_list in self.raw_output_data.items():  # For each row
            new_row_values = []
            for col_index, col_value in enumerate(values_list):  # For each column
                for returned_data_key, returned_data_value in col_value.items():  # For each returned value
                    full_column_name = column_names[col_index] + "_" + returned_data_key
                    if full_column_name not in new_column_names:  # Add the column to the list if it isn't there
                        new_column_names.append(full_column_name)
                    if type(returned_data_value) == list:  # Flatten lists into comma delimited strings
                        returned_data_value = [str(x) for x in returned_data_value]  # Ensuring items are strings
                        returned_data_value = ','.join(returned_data_value)
                    new_row_values.append(returned_data_value)  # Storing the value (in order) to the new list
            new_output_data[row_key] = new_row_values  # Storing the flattened list of values

        self.new_output_data = new_output_data  # Save the new output data to self
        self.new_column_names = new_column_names  # Save the new column names to self

        logger.debug(f"Function - flatten_output_data() - Finish - Returning self")
        return self


if __name__ == "__main__":
    """Only runs below if script is run directly, not on import, so this is for testing purposes"""
    data_example: Data = \
        {
            1: [
                r"If you are after a perfect dinner do not hesitate. Came here for my birthday dinner and must say "
                r"this place is a 10/10. If I could give them more then 5 stars I would. The pizza & pasta were great. "
                r"You can always tell the difference when a restaurant is operated as a family business. "
                r"The staff were very polite aswell.",
                'Plenty of parking',
                'Where are the bloody chicken nuggets? My pizza was bereft of nuggets'],
            2: [
                'Got disappointed. Not good customer service. definitely not great pizza. We went there yesterday to '
                'dine in and saw the offer of installing app to get $5 discount. We made the order but been told by the'
                ' staff that no specials are served in-store. We went with kids and family and though all the tables '
                'were empty and no other customers were in the store, they did not let us sit.',
                'We had to take away the pizza disappointing children. If the tables were busy and customers waiting, '
                'we could have understood.',
                'I loved how we were served raw dog meat in our car, which had three wheels and a floppy ear'],
            3: [
                'Highly recommended Pizza Guru! Good service, yummy food and friendly staff! Also good price for '
                'family! We love the fresh garlic bread so much! Thank you for giving my son a coloring paper. '
                'Definitely will come back! :)',
                'I have found 3 pennies in my pasta 4 times in a row, they hurt my mouth but make me feel a '
                'little richer every time',
                'It has taken me 3 years of living in the area to finally discover this amazing local pizza place! '
                'Tasty with fresh ingredients and not too greasy, I would recommend.',
                'The bread was good'
            ]
        }

    handler = LLMHandler(data_example,
                         provider="OpenAI",
                         model="gpt-3.5-turbo",
                         role="An expert customer feedback analyst nlp system",
                         request="Analyse the feedback and return results in the correct format",
                         validation_model=validators.PrimaryCategoryAndSubCategory,
                         cache_identifier='NLP-Categorise-TestData',
                         use_cache=False,
                         temperature=0.2,
                         max_validation_retries=3
                         )

    handler.run()
    handler.flatten_output_data(['Col1', 'Col2', 'Col3', 'Col4', 'Col5'])
    print(handler.output_data)
