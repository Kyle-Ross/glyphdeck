from pydantic import BaseModel
import openai
from custom_types import Data
from typing import Union, List, Dict
import llm_output_structures
from icecream import ic
import asyncio
import time
import backoff
import itertools
import functools

start_time = None


def print_time_since_start():
    global start_time
    if start_time is None:
        start_time = time.time()
    elapsed_time = time.time() - start_time
    return f'Delta: {elapsed_time} sec'


# Basic function clean input strings
def string_cleaner(input_str: str) -> str:
    return input_str.strip().lower().replace(' ', '')


class LLMHandler:
    """Write your docstring for the class here."""

    def check_instance(self):
        """Checks that the provided attribute is an instance or inheritance of the Pydantic BaseModel class."""
        check: bool = isinstance(self.output_structure, BaseModel)
        assert check, f'{self.output_structure} is not an instance of, or inherited from the Pydantic BaseModel class'

    def __init__(self,
                 input_data: Data,
                 parser: str,
                 output_structure,
                 provider: str,
                 model: str,
                 api_key: str,
                 role: str,
                 request: str,
                 max_coroutine_retries: int = 10,
                 max_event_loop_retries: int = 10) -> None:

        # Storing the input data, of the 'Data' type as delivered by a 'Chain' object
        self.input_data: Data = input_data
        # output_data keeps keys, replaces with [None, None, ...] lists
        # Helps to insert output in the correct position later in 'await_tasks'
        self.output_data: Data = {key: [None] * len(value) for key, value in input_data.items()}

        self.parser: str = parser  # The attribute of the method used to advise output structure
        self.output_structure = output_structure

        # Storing and Checking available providers against a list
        self.available_providers: tuple = ("openai",)
        self.provider: str = provider
        self.provider_clean: str = string_cleaner(self.provider)
        assert self.provider_clean in self.available_providers, f"{self.provider} is not in the list of " \
                                                                f"available providers:" \
                                                                f"\n{self.available_providers}"

        # Object level llm information that serves as the default value if functions don't specify customisations
        self.model: str = model
        self.api_key: str = api_key
        self.role: str = role
        self.request: str = request

        # Check that the class attribute is the correct type
        self.check_instance()

        # A semaphore is a synchronization primitive used to control access to a common resource
        # by multiple processes in a concurrent system
        # The Semaphore is initialized with 1, meaning only one coroutine can acquire it at a time.
        # Other coroutines trying to acquire it will be blocked until it's released.
        # This is used later to force certain backoff exceptions to stop running any other new coroutines
        self.semaphore = asyncio.Semaphore(1)

        # Max retries for backoff wrappers applied to either coroutine or event loop
        self.max_coroutine_retries = max_coroutine_retries
        self.max_event_loop_retries = max_event_loop_retries

        # Storing lists of exceptions for different uses
        # Once you have other providers just add their errors into the appropriate list

        # coroutine_backoff exceptions are allowed to be retried inside each individual coroutine
        self.coroutine_backoff = [openai.APITimeoutError,
                                  openai.ConflictError,
                                  openai.InternalServerError,
                                  openai.UnprocessableEntityError]
        # coroutine_exceptions end a coroutine and record the error without ending code execution
        self.coroutine_exceptions = [openai.BadRequestError,
                                     openai.NotFoundError]
        # loop_backoff exceptions retry a single coroutine but delay the whole event loop using the semaphore
        self.loop_backoff = [openai.APIConnectionError,
                             openai.RateLimitError]
        # loop_exceptions should completely stop the script
        self.loop_exceptions = [openai.PermissionDeniedError]

    def coroutine_backoff_wrapper(self, func):
        """Wrapper for the backoff decorator used with coroutines, allowing use of self attributes which isn't
        usually possible for decorators since they are defined before __init__"""

        @functools.wraps(func)  # Maintains the original attribute of the func
        @backoff.on_exception(backoff.expo,
                              self.coroutine_backoff,
                              max_tries=self.max_coroutine_retries)
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)

        return wrapper

    def event_loop_backoff_wrapper(self, func):
        """Wrapper for the backoff decorator used with pausing event loops, allowing use of self attributes
        which isn't usually possible for decorators since they are defined before __init__"""

        @functools.wraps(func)  # Maintains the original attribute of the func
        @backoff.on_exception(backoff.expo,
                              self.event_loop_backoff,
                              max_tries=self.max_event_loop_retries)
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)

        return wrapper

    async def openai(self,
                     input_text: str,
                     key,
                     index: int,
                     item_parser: str = None,
                     item_output_structure=None,
                     item_model: str = None,
                     item_api_key: str = None,
                     item_role: str = None,
                     item_request: str = None) -> tuple:
        """Asynchronous Per-item coroutine generation with OpenAI. Has exponential backoff on specified errors."""
        # If no arguments are provided, uses the values set in the instance
        # Necessary to do it this way since self is not yet defined in this function definition
        if item_parser is None:
            item_parser = self.parser
        if item_output_structure is None:
            item_output_structure = self.output_structure
        if item_model is None:
            item_model = self.model
        if item_api_key is None:
            item_api_key = self.api_key
        if item_role is None:
            item_role = self.role
        if item_request is None:
            item_request = self.request

        # Initialising the client
        openai_client = openai.AsyncOpenAI(api_key=item_api_key)

        # Sending the request
        chat_completion = await openai_client.chat.completions.create(
            model=item_model,
            messages=[

                {"role": "system",
                 "content": item_role},

                {"role": "user",
                 "content": item_request + ' ' + input_text}
            ]
        )

        # Storing the response
        response = chat_completion.choices[0].message.content

        # Returning the response as a tuple (shorthand syntax)
        return response, key, index

    async def create_coroutines(self, func) -> list:
        """Creates individual coroutines for running the provided function on every value in the data.
        If you wanted to have different per response settings for the prompt, you would set them on func here."""
        # Applying the backoff wrapper to the func
        func = self.coroutine_backoff_wrapper(func)
        # Create and store the tasks across the whole data in a list
        coroutines = [func(input_text=item_value, key=key, index=index)  # List of coroutines
                      for key, list_value in self.input_data.items()  # For every key in the input_data dict
                      for index, item_value in enumerate(list_value)]  # For every item in every list
        return coroutines

    async def event_loop_backoff(self, coroutine):
        """Execute a coroutine, save results and handle exceptions with exponential backoff.
        Replaces the output with an error attribute under certain conditions."""
        try:
            result = await coroutine
            response = result[0]
            key = result[1]
            index = result[2]
            print(f'SUCCESS | Key: {key} | Index: {index} | | {print_time_since_start()}')
            self.output_data[key][index] = response
        except self.coroutine_exceptions as error:
            self.output_data[key][index] = type(error).__name__  # TODO - Get key and index working here
            print(f'FAILURE | Key: {key} | Index: {index} | Error: {type(error).__name_} | {print_time_since_start()}')

    async def await_coroutines(self, func):
        """Create and manage coroutines using the function passed as an argument. A semaphore is used to ensure that
        only one coroutine is being retried at a time. When a coroutine encounters an exception and needs to be
        retried, it first acquires the semaphore. This causes any other coroutines that encounter an exception to
        wait until the semaphore is released before they can proceed. This effectively pauses the loop until the
        current coroutine has been successfully retried. """
        # Applying the backoff wrapper to the func
        func = self.event_loop_backoff_wrapper(func)
        # Awaits the list of coroutines provided by self.create_coroutines
        coroutines = await self.create_coroutines(func)
        # get results as coroutines are completed
        for future in asyncio.as_completed(coroutines):
            # Acquire semaphore
            await self.semaphore.acquire()
            try:
                await self.event_loop_backoff(future)
            finally:
                # Release semaphore
                self.semaphore.release()

    def run(self):
        """Asynchronously query the selected LLM across the whole data and save results to the output"""
        try:
            if self.provider_clean == "openai":
                asyncio.run(self.await_coroutines(self.openai))
            return self
        except self.loop_exceptions as error:
            print(f"Known Blocking Error: {type(error).__name_} | Stopping execution:")
            raise  # re-raises the last message that was active in the current scope and terminates the program


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

    import json

    # Access the API key from the local secrets file
    with open("secrets.json") as file:
        my_api_key = json.load(file)["openai_api_key"]

    # Example model instance
    pydantic_model_instance = llm_output_structures.SentimentScore(sentiment_score=0.55)

    handler = LLMHandler(data_example,
                         parser="pydantic",
                         output_structure=pydantic_model_instance,
                         provider="OpenAI",
                         model="gpt-3.5-turbo",
                         api_key=my_api_key,
                         role="An expert nlp comment analysis system, trained to accurately categorise " \
                              "customer feedback",
                         request="Provide the top 5 most relevant categories for the comment. Just provide the title"
                                 "of each category. There may not always be 5 categories.",
                         max_coroutine_retries=10,
                         max_event_loop_retries=10
                         )

    handler.run()
    ic(handler.output_data)

# https://superfastpython.com/asyncio-as_completed/#Example_of_as_completed_with_Coroutines
# TODO - Test backoff system somehow, coroutine and event loop level
# TODO - Print messages for different excepted errors, logging etc
# TODO - Add coroutine timeout condition to avoid hangs
# TODO - Get structured output happening using the pydantic classes - see openai functions (saved medium article)
# TODO - Add retry loops on incorrect output, re-query response correction on incorrect output format?
# TODO - test chain integration
