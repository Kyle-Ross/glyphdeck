from typing import Optional
import asyncio
import copy
import os

import instructor
import openai
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)

from CategoriGen.validation.data_types import (
    Data,
    assert_and_log_type_is_data,
    StrList,
    Optional_Data,
    Optional_StrList,
)
from CategoriGen.validation import validators
from CategoriGen.tools.loggers import (
    assert_and_log_error,
    LLMHandlerLogger,
    log_decorator,
)
from CategoriGen.logger_constants import log_output_data, log_input_data
from CategoriGen.tools.strings import string_cleaner
from CategoriGen.tools.caching import openai_cache

logger = LLMHandlerLogger().setup()
logger.debug(" | Step | llm_handler.py | Action | Initialised logger")


class LLMHandler:
    """Write your docstring for the class here."""

    @log_decorator(
        logger,
        "debug",
        suffix_message="Check class is an instance or inheritance of the Pydantic BaseValidatorModel class",
        show_nesting=False,
    )
    def check_validation_model(self):
        """Checks that the provided class is an instance or inheritance of the Pydantic BaseValidatorModel class."""
        check: bool = issubclass(self.validation_model, validators.BaseValidatorModel)
        assert_and_log_error(
            logger,
            "error",
            check,
            f"{self.validation_model.__name__} is not a subclass of the "
            f"Pydantic BaseValidatorModel class",
        )

    def __init__(
        self,
        input_data: Data,  # Input variable of the custom 'Data' type
        provider: str,  # The provider of the llm
        model: str,  # The model of the llm
        role: str,  # The role to provide the llm in the prompts
        request: str,  # The request to make to the llm
        validation_model,  # Pydantic class to use for validating output, checked by check_validation_model()
        cache_identifier: str,  # A Unique string used to identify discrete jobs and avoid cache mixing
        use_cache: bool = True,  # Set whether to check the cache for values or not
        temperature: float = 0.2,  # How deterministic (low num) or random (high num) the responses will be
        max_validation_retries: int = 2,  # Max times the request can retry on the basis of failed validation
        # Below: Maximum amount of 'pre-prepared' coroutines, that can exist before being awaited
        # Lower limits can help manage memory usage and reduce context switching overhead
        # Context Handling being grabbing it from memory when 'pre-prepared', vs just creating it as needed
        # Optimal number can vary, depending on the task and your system (CPU + Memory)
        max_preprepared_coroutines: int = 10,
        # Below: After creation, the maximum amount of coroutines that can be 'awaiting' at once
        # Essentially represents the max api calls that can be in-memory, waiting on the api return at once
        # High values will run faster incrementally faster, but consume more memory
        max_awaiting_coroutines: int = 100,
    ):
        logger.debug(
            " | Function | LLMHandler.__init__() | Start | Initialising LLMHandler object"
        )

        # Assert the variable type of the provided arguments
        # Check the custom data type 'Data'
        assert_and_log_type_is_data(input_data, "input_data")
        assert_and_log_error(
            logger,
            "error",
            isinstance(provider, str),
            "'provider' argument must be type 'str'",
        )
        assert_and_log_error(
            logger,
            "error",
            isinstance(model, str),
            "'model' argument must be type 'str'",
        )
        assert_and_log_error(
            logger, "error", isinstance(role, str), "'role' argument must be type 'str'"
        )
        assert_and_log_error(
            logger,
            "error",
            isinstance(request, str),
            "'request' argument must be type 'str'",
        )
        assert_and_log_error(
            logger,
            "error",
            isinstance(cache_identifier, str),
            "'cache_identifier' argument must be type 'str'",
        )
        assert_and_log_error(
            logger,
            "error",
            isinstance(use_cache, bool),
            "'use_cache' argument must be type 'str'",
        )
        assert_and_log_error(
            logger,
            "error",
            isinstance(temperature, float),
            "'temperature' argument must be type 'float'",
        )
        assert_and_log_error(
            logger,
            "error",
            isinstance(max_validation_retries, int),
            "'max_validation_retries' argument must be type 'int'",
        )
        assert_and_log_error(
            logger,
            "error",
            isinstance(max_preprepared_coroutines, int),
            "'max_preprepared_coroutines' argument must be type 'int'",
        )
        assert_and_log_error(
            logger,
            "error",
            isinstance(max_awaiting_coroutines, int),
            "'max_awaiting_coroutines' argument must be type 'int'",
        )

        # Initialise Semaphores (these are class level and will not change between LLM providers)
        self.max_preprepared_coroutines_semaphore = asyncio.Semaphore(
            max_preprepared_coroutines
        )
        self.max_awaiting_coroutines_semaphore = asyncio.Semaphore(
            max_awaiting_coroutines
        )

        # Storing the input variable, of the 'Data' type as typically delivered by a 'Chain' object
        self.input_data: Data = input_data
        # raw_output_data keeps keys, replaces with [None, None, ...] lists
        # Helps to insert output in the correct position later in 'await_tasks'
        self.raw_output_data: Data = {
            key: [None] * len(value) for key, value in input_data.items()
        }

        # Generated later by self.flatten_output_data()
        self.new_output_data: Optional_Data = None
        # Generated by self.flatten_output_data()
        self.new_column_names: Optional_StrList = None

        # Storing and Checking available providers against a list
        self.available_providers: tuple = ("openai",)
        self.provider: str = provider
        self.provider_clean: str = string_cleaner(self.provider)
        assert_and_log_error(
            logger,
            "error",
            self.provider_clean in self.available_providers,
            f"{self.provider} is not in the list of available providers: "
            f"{self.available_providers}",
        )

        # Object level llm information that serves as the default value if tools don't specify customisations
        self.model: str = model
        self.role: str = role
        self.request: str = request
        self.validation_model = validation_model
        self.temperature: float = temperature
        self.max_validation_retries = max_validation_retries

        # Referenced in lru_cache by accessing self
        self.cache_identifier: str = cache_identifier
        self.use_cache: bool = use_cache

        # Checks that model comes from customer Pydantic BaseValidatorModel class
        self.check_validation_model()

        # Preparing openai client
        if self.provider_clean == "openai":
            openai.api_key = os.getenv("OPENAI_API_KEY")
            logger.debug(
                " | Step | LLMHandler.__init__() | Action | Set openai api key"
            )

            # Initialising the client
            # instructor patches in variable validation via pydantic with the response_model and max_retries attributes
            self.openai_client = instructor.patch(openai.AsyncOpenAI())
            logger.debug(
                " | Step | LLMHandler.__init__() | Action | Set openai_client and patched with instructor"
            )

        logger.debug(
            " | Function | LLMHandler.__init__() | Finish | Initialising LLMHandler object"
        )

    @property
    @log_decorator(
        logger,
        "debug",
        is_property=True,
        suffix_message="Checking if self.new_output_data is not none, indicating data has been flattened",
        show_nesting=False,
    )
    def output_data(self):
        """Accesses output data but only if the data has been flattened."""
        assert_and_log_error(
            logger,
            "error",
            self.new_output_data is not None,
            "output_data is empty, run self.flatten_output_data() first.",
        )
        return self.new_output_data

    @property
    @log_decorator(
        logger,
        "debug",
        is_property=True,
        suffix_message="Checking if self.new_column_names is not none, indicating data has been flattened",
        show_nesting=False,
    )
    def column_names(self):
        """Accesses column names but only if the new names have been generated during data flattening."""
        assert_and_log_error(
            logger,
            "error",
            self.new_column_names is not None,
            "column_names is empty, run self.flatten_output_data() first.",
        )
        return self.new_column_names

    @log_decorator(
        logger,
        "debug",
        suffix_message="Async coroutine generation with OpenAI",
        show_nesting=False,
    )
    # Tenacity retry decorator for the following errors with exponential backoff
    @retry(
        retry=retry_if_exception_type(
            (
                openai.APITimeoutError,
                openai.ConflictError,
                openai.InternalServerError,
                openai.UnprocessableEntityError,
                openai.APIConnectionError,
                openai.RateLimitError,
            )
        ),
        # Waits for (sec) 0.9375, 1.875, 3.75, 7.5, 15, 30, 60 (max)
        wait=wait_exponential(multiplier=2, min=0.9375, max=60),
        # About 5 hours of retries!
        stop=stop_after_attempt(300),
    )
    # Can overwrite other args here which otherwise use default values
    @openai_cache("async_openai_cache")
    async def async_openai(
        self,
        input_text: str,
        key,
        index: int,
        item_model: Optional[str] = None,
        item_role: Optional[str] = None,
        item_request: Optional[str] = None,
        item_validation_model=None,
        item_temperature: Optional[float] = None,
        item_max_validation_retries: Optional[int] = None,
    ) -> tuple:
        """Asynchronous Per-item coroutine generation with OpenAI. Has exponential backoff on specified errors."""
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
        assert_and_log_error(
            logger,
            "error",
            0 <= item_temperature <= 1,
            "For OpenAI, temperature must be between 0 and 1",
        )

        # Sending the request
        # Having patched with instructor changes the response object...
        # response information is now accessed like item_validation_model.field_name
        chat_params = {
            "model": item_model,
            "response_model": item_validation_model,
            "max_retries": item_max_validation_retries,
            "temperature": item_temperature,
            "messages": [
                {"role": "system", "content": item_role},
                {"role": "user", "content": f"{item_request} {str(input_text)}"},
            ],
        }

        # Log the parameters unchanged if log_input_data = True
        if log_input_data:
            logger.debug(
                f" | Step | async_openai() | Action | chat_params = {chat_params}"
            )
        # Log the parameters with the input information removed otherwise
        else:
            # Make a deepcopy of the dict, overwise changes will flow back
            chat_params_log = copy.deepcopy(chat_params)
            chat_params_log["messages"][1]["content"] = f"{item_request} <INPUT_TEXT>"
            logger.debug(
                f" | Step | async_openai() | Action | chat_params = {chat_params_log}"
            )

        # Running the chat completion and saving as an instructor model
        logger.debug(" | Step | async_openai() | Start | Chat completion")
        instructor_model = await self.openai_client.chat.completions.create(
            **chat_params
        )
        logger.debug(" | Step | async_openai()  | Finish | Chat completion")

        # Storing the response object (as made by the patched openai_client)
        # Extracting a dict of the fields using the pydantic basemodel
        response = dict(instructor_model)

        # Submit a log, including or excluding the output depending on settings
        # Include/Exclude log data per settings
        completion_log = f" = ({response}, {key}, {index})" if log_output_data else ""
        logger.debug(
            f" | Step | async_openai() | Action | Returning (response, key, index){completion_log}"
        )
        # Returning the response as a tuple (shorthand syntax)
        return response, key, index

    @log_decorator(
        logger,
        "debug",
        suffix_message="Create coroutines",
        show_nesting=False,
    )
    async def create_coroutines(self, func) -> list:
        """Creates coroutines for the provided input data, using the specified LLM function."""
        # Create and store the tasks across the whole variable in a list
        coroutines = []
        for key, list_value in self.input_data.items():
            for index, item_value in enumerate(list_value):
                # Limits concurrent tasks, releasing semaphore when with block is exited, aka coroutine is created
                async with self.max_preprepared_coroutines_semaphore:
                    coroutine = func(input_text=item_value, key=key, index=index)
                    coroutines.append(coroutine)
        return coroutines

    @log_decorator(
        logger,
        "debug",
        suffix_message="Await coroutines,  returning in order of completion",
        show_nesting=False,
    )
    async def await_coroutines(self, func):
        """Await coroutines,  returning results in the order of completion, not the order they are run."""
        coroutines = await self.create_coroutines(func)
        # Loop over the futures
        logger.debug(
            " | Step | await_coroutines() | Start | Looping over futures of coroutines using as_completed()"
        )
        for future in asyncio.as_completed(coroutines):
            # Limiting the amount of coroutines running / waiting on the api (and taking up memory)
            # as_completed() above means the semaphore is released as the future is completed, in any order
            async with self.max_awaiting_coroutines_semaphore:
                logger.debug(
                    " | Step | await_coroutines() | Start | In future loop, trying to await future"
                )
                result = await future
                # Include/Exclude log data per settings
                result_log = f", result = {result}" if log_output_data else ""
                logger.debug(
                    f" | Step | await_coroutines() | Finish | In future loop, successfully awaited future{result_log}"
                )
                response = result[0]
                key = result[1]
                index = result[2]
                self.raw_output_data[key][index] = response
        logger.debug(
            " | Step | await_coroutines() | Finish | Looping over futures of coroutines using as_completed()"
        )

    @log_decorator(
        logger,
        "info",
        suffix_message="Asynchronous data fetching from LLM or cache",
        show_nesting=False,
    )
    def run_async(self):
        """Asynchronously query the selected LLM across the whole variable and save results to the output"""
        if self.provider_clean == "openai":
            asyncio.run(self.await_coroutines(self.async_openai))
        return self

    @log_decorator(
        logger,
        "info",
        suffix_message="Convert output data for compatibility with chain class",
        show_nesting=False,
    )
    def flatten_output_data(self, column_names: StrList):
        """Flattens output data into a dictionary of lists for compatibility with the chain class.
        Also creates the new column names for the eventual output."""
        # Storage for key: list pairs representing rows
        new_output_data = {}
        # Storing a list of the column names corresponding to the ordered list
        new_column_names = []

        for row_key, values_list in self.raw_output_data.items():
            # For each row
            new_row_values = []
            for col_index, col_value in enumerate(values_list):
                # For each column
                for (
                    returned_data_key,
                    returned_data_value,
                ) in col_value.items():
                    # For each returned value
                    full_column_name = column_names[col_index] + "_" + returned_data_key
                    # Add the column to the list if it isn't there
                    if full_column_name not in new_column_names:
                        new_column_names.append(full_column_name)
                    # Flatten lists into comma delimited strings
                    if type(returned_data_value) is list:
                        # Ensuring items are strings
                        returned_data_value = [str(x) for x in returned_data_value]
                        returned_data_value = ",".join(returned_data_value)
                    # Storing the value (in order) to the new list
                    new_row_values.append(returned_data_value)
            # Storing the flattened list of values
            new_output_data[row_key] = new_row_values

        # Save the new output data to self
        self.new_output_data = new_output_data
        # Save the new column names to self
        self.new_column_names = new_column_names
        return self
