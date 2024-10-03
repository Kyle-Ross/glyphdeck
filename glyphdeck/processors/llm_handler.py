from typing import Optional, List, Tuple, Dict, Union, Coroutine
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

from glyphdeck.validation.data_types import (
    DataDict,
    assert_and_log_type_is_data,
    Optional_DataDict,
)
from glyphdeck.validation import validators
from glyphdeck.tools.logging_ import (
    assert_and_log_error,
    LLMHandlerLogger,
    log_decorator,
)
from glyphdeck.config.logger_levels import log_output_data, log_input_data
from glyphdeck.tools.strings import string_cleaner
from glyphdeck.tools.caching import openai_cache

logger = LLMHandlerLogger().setup()
logger.debug(" | Step | llm_handler.py | Action | Initialised logger")


class LLMHandler:
    """Handler for interacting with Large Language Models (LLMs) and managing their settings, inputs, and outputs.

    Attributes:
        input_data: Dictionary containing the input data.
        provider: Name of the LLM provider.
        model: Model identifier for the LLM.
        system_message: The system message to provide in the LLM prompts.
        validation_model: Pydantic class used for validating LLM outputs.
        cache_identifier: Unique string used to identify discrete jobs and avoid cache mixing.
        use_cache: Boolean indicating whether to use cache or not.
        temperature: Determines if the responses are deterministic (lower value) or random (higher value).
        max_validation_retries: Maximum number of retries for validation attempts.
        max_preprepared_coroutines_semaphore: Semaphore to limit the number of pre-prepared coroutines.
        max_awaiting_coroutines_semaphore: Semaphore to limit the number of awaiting coroutines.
        raw_output_data: Dictionary to store the intermediate LLM outputs.
        new_output_data: Flattened output data to be generated.
        new_column_names: Generated column names to be used in the flattened output data.
        available_providers: List of LLM providers that are available.
    """

    @log_decorator(
        logger,
        "debug",
        suffix_message="Check class is an instance or inheritance of the Pydantic BaseValidatorModel class",
        show_nesting=False,
    )
    def _check_validation_model(self):
        """Check if the validation model is an instance of Pydantic BaseValidatorModel.

        Raises:
            AssertionError: If the validation model is not a subclass of the Pydantic BaseValidatorModel class.
        """
        check: bool = issubclass(self.validation_model, validators.BaseValidatorModel)
        assert_and_log_error(
            logger,
            "error",
            check,
            f"{self.validation_model.__name__} is not a subclass of the "
            f"Pydantic BaseValidatorModel class",
        )

    # WARNING!!! ON CHANGES - Manually syncronise these args, type hints & defaults with cascade.set_llm_handler() function
    # Until such time you figure out how to have a function and a class share the same signature
    def __init__(
        self,
        input_data: DataDict,  # Input variable of the custom 'Data' type
        provider: str,  # The provider of the llm
        model: str,  # The model of the llm
        system_message: str,  # The system message to provide the llm in the prompts
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
        """Initializes the LLMHandler with necessary configurations and validations.

        Args:
            input_data: Dictionary containing the input data.
            provider: Name of the LLM provider.
            model: Model identifier for the LLM.
            system_message: The system message to provide to the LLM in prompts.
            validation_model: Pydantic class used for validating output.
            cache_identifier: Unique string used to identify discrete jobs and avoid cache mixing.
            use_cache: Boolean indicating whether to use cache or not. Defaults to True.
            temperature: Determines if the responses are deterministic (lower value) or random (higher value). Defaults to 0.2.
            max_validation_retries: Maximum number of retries for failed validation attempts. Defaults to 2.
            max_preprepared_coroutines: Maximum number of prepared coroutines before awaiting. Defaults to 10.
            max_awaiting_coroutines: Maximum number of coroutines awaiting at once. Defaults to 100.

        Raises:
            AssertionError: If any of the provided arguments are of incorrect type or invalid values.
        """
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
            logger,
            "error",
            isinstance(system_message, str),
            "'system_message' argument must be type 'str'",
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

        # Storing the input variable, of the 'Data' type as typically delivered by a 'Cascade' object
        self.input_data: DataDict = input_data
        # raw_output_data keeps keys, replaces with [None, None, ...] lists
        # Helps to insert output in the correct position later in 'await_tasks'
        self._raw_output_data: DataDict = {
            key: [None] * len(value) for key, value in input_data.items()
        }

        # Generated later by self.flatten_output_data()
        self.new_output_data: Optional_DataDict = None
        # Generated by self.flatten_output_data()
        self.new_column_names: Optional[List[str]] = None

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
        self.system_message: str = system_message
        self.validation_model = validation_model
        self.temperature: float = temperature
        self.max_validation_retries = max_validation_retries

        # Referenced in lru_cache by accessing self
        self.cache_identifier: str = cache_identifier
        self.use_cache: bool = use_cache

        # Checks that model comes from customer Pydantic BaseValidatorModel class
        self._check_validation_model()

        # Preparing openai client
        if self.provider_clean == "openai":
            openai.api_key = os.getenv("OPENAI_API_KEY")
            logger.debug(
                " | Step | LLMHandler.__init__() | Action | Set openai api key"
            )

            # Initialising the client
            # instructor patches in variable validation via pydantic with the response_model and max_retries attributes
            self._openai_client = instructor.patch(openai.AsyncOpenAI())
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
    def output_data(self) -> DataDict:
        """Accesses the output data after it has been flattened.

        Returns:
            DataDict: The flattened output data.

        Raises:
            AssertionError: If output_data is accessed before flatten_output_data() has been run.
        """
        # Accesses output data but only if the data has been flattened.
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
    def column_names(self) -> List[str]:
        """Accesses the column names after they have been generated during data flattening.

        Returns:
            List[str]: The list of column names.

        Raises:
            AssertionError: If column_names is accessed before flatten_output_data() has been run.
        """
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
    async def _async_openai(
        self,
        input_text: str,
        key,
        index: int,
        item_model: Optional[str] = None,
        item_system_message: Optional[str] = None,
        item_validation_model=None,
        item_temperature: Optional[float] = None,
        item_max_validation_retries: Optional[int] = None,
    ) -> Tuple[Dict, Union[str, int], int]:
        """Asynchronous Per-item coroutine generation with OpenAI.

        Args:
            input_text: The input text to be processed by the LLM.
            key: The key associated with the input text.
            index: The index position of the input in the list.
            item_model: Optional override for the LLM model.
            item_system_message: Optional override for the system message.
            item_validation_model: Optional override for the validation model.
            item_temperature: Optional override for the response randomness.
            item_max_validation_retries: Optional override for the maximum number of validation retries.

        Returns:
            Tuple: Containing the response dictionary, key, and index.

        Raises:
            AssertionError: If the temperature value is not between 0 and 1.
        """
        # If no arguments are provided, uses the values set in the handler class instance
        # Necessary to do it this way since self is not yet defined in this function definition
        if item_model is None:
            item_model = self.model
        if item_system_message is None:
            item_system_message = self.system_message
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
                {"role": "system", "content": item_system_message},
                {"role": "user", "content": str(input_text)},
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
            chat_params_log["messages"][1]["content"] = "<INPUT_TEXT>"
            logger.debug(
                f" | Step | async_openai() | Action | chat_params = {chat_params_log}"
            )

        # Running the chat completion and saving as an instructor model
        logger.debug(" | Step | async_openai() | Start | Chat completion")
        instructor_model = await self._openai_client.chat.completions.create(
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
    async def _create_coroutines(self, func) -> List[Coroutine]:
        """Creates coroutines for the provided input data using the specified LLM function.

        Args:
            func: The function to generate coroutines for.

        Returns:
            List[Coroutine]: A list of created coroutines.
        """
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
    async def _await_coroutines(self, func):
        """Await coroutines and return results in completion order.

        Args:
            func: The function used to generate coroutines.
        """
        coroutines = await self._create_coroutines(func)
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
                self._raw_output_data[key][index] = response
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
        """Asynchronously query the selected LLM across the whole data and save results to the output.

        Returns:
            self: Instance of the LLMHandler class.
        """
        if self.provider_clean == "openai":
            asyncio.run(self._await_coroutines(self._async_openai))
        return self

    @log_decorator(
        logger,
        "info",
        suffix_message="Convert output data for compatibility with cascade class",
        show_nesting=False,
    )
    def flatten_output_data(self, column_names: List[str]):
        """Flattens output data into a dictionary of lists for compatibility with the cascade class. Also creates the new column names for the eventual output.

        Args:
            column_names: List of column names to be used.

        Returns:
            Dictionary of flattened output data.
        """
        # Storage for key: list pairs representing rows
        _new_output_data = {}
        # Storing a list of the column names corresponding to the ordered list
        _new_column_names = []

        for row_key, values_list in self._raw_output_data.items():
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
                    if full_column_name not in _new_column_names:
                        _new_column_names.append(full_column_name)
                    # Flatten lists into comma delimited strings
                    if type(returned_data_value) is list:
                        # Ensuring items are strings
                        returned_data_value = [str(x) for x in returned_data_value]
                        returned_data_value = ",".join(returned_data_value)
                    # Storing the value (in order) to the new list
                    new_row_values.append(returned_data_value)
            # Storing the flattened list of values
            _new_output_data[row_key] = new_row_values

        # Save the new output data to self
        self.new_output_data = _new_output_data
        # Save the new column names to self
        self.new_column_names = _new_column_names
        return self
