from pydantic import BaseModel
import openai
from custom_types import Data
from typing import Union, List
import llm_output_structures
from icecream import ic
import pandas as pd
import os
import asyncio
import time
import backoff  # for exponential backoff

start_time = None


def print_time_since_start():
    global start_time
    if start_time is None:
        start_time = time.time()
    elapsed_time = time.time() - start_time
    return f'Time since start: {elapsed_time} seconds'


def decorator_attributes(attribute_name: str) -> Union[List]:
    """A wrapper for a setting self attributes, which allows them to both be set in __init__ and used in a
    decorator function. This is necessary since decorators are used at runtime, before __init__ is run
    and self attributes are set."""
    # Coroutine runtime_attributes are allowed to be retried inside each individual coroutine
    # Event Loop runtime_attributes are handled by and delay the whole event loop using the semaphore
    # Blockers should end a coroutine or eventloop and record the error
    dictionary: dict = {'openai_coroutine_exceptions': [openai.APITimeoutError, openai.ConflictError,
                                                        openai.InternalServerError, openai.UnprocessableEntityError],
                        'openai_coroutine_blockers': [openai.BadRequestError, openai.NotFoundError],
                        'event_loop_exceptions': [openai.APIConnectionError, openai.RateLimitError],
                        'event_loop_blockers': [openai.PermissionDeniedError]}
    return_value = dictionary if attribute_name.lower().strip() == 'all' else dictionary[attribute_name]
    return return_value


class LLMHandler:
    """Write your docstring for the class here."""

    def check_instance(self):
        """Checks that the provided attribute_name is an instance or inheritance of the Pydantic BaseModel class."""
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
                 request: str) -> None:

        self.input_data: Data = input_data
        # output_data keeps keys, replaces with [None, None, ...] lists
        # Helps to insert output in the correct position later in 'await_tasks'
        self.output_data: Data = {key: [None] * len(value) for key, value in input_data.items()}

        self.parser: str = parser  # The name of the method used to advise output structure
        self.output_structure = output_structure

        self.available_providers: tuple = ("openai",)
        self.provider: str = provider
        self.provider_clean: str = self.provider.strip().lower().replace(' ', '')
        assert self.provider_clean in self.available_providers, f"{self.provider} is not in the list of " \
                                                                f"available providers:" \
                                                                f"\n{self.available_providers}"

        self.model: str = model
        self.api_key: str = api_key
        self.role: str = role
        self.request: str = request

        # Check that the class attribute is the correct type
        self.check_instance()

        # Exceptions allowed in the exponential backoff retries
        # Class methods won't use the new value if you update this attribute
        self.runtime_attributes = decorator_attributes('all')

        # Semaphore is initialized with 1, meaning only one coroutine can acquire it at a time.
        # Other coroutines trying to acquire it will be blocked until it's released.
        # A semaphore is a synchronization primitive used to control access to a common resource
        # by multiple processes in a concurrent system
        self.semaphore = asyncio.Semaphore(1)

    @backoff.on_exception(backoff.expo, decorator_attributes('openai_coroutine_exceptions'), max_tries=10)
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

    async def create_coroutines(self, func):
        """Runs the provided function on every value in the data"""
        # Create and store the tasks across the whole data in a list
        coroutines = [func(input_text=item_value, key=key, index=index)  # List of coroutines
                      for key, list_value in self.input_data.items()  # For every key in the input_data dict
                      for index, item_value in enumerate(list_value)]  # For every item in every list
        return coroutines

    @backoff.on_exception(backoff.expo, decorator_attributes('event_loop_exceptions'), max_tries=10)
    async def event_loop_backoff(self, coroutine):
        """Execute a coroutine, save results and handle exceptions with exponential backoff."""
        result = await coroutine
        response = result[0]
        key = result[1]
        index = result[2]
        print(f'FINISH | Key: {key} | Index: {index} | {print_time_since_start()}')
        self.output_data[key][index] = response

    async def await_coroutines(self, func):
        """Create and manage coroutines using the function passed as an argument. A semaphore is used to ensure that
        only one coroutine is being retried at a time. When a coroutine encounters an exception and needs to be
        retried, it first acquires the semaphore. This causes any other coroutines that encounter an exception to
        wait until the semaphore is released before they can proceed. This effectively pauses the loop until the
        current coroutine has been successfully retried. """
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
        if self.provider_clean == "openai":
            asyncio.run(self.await_coroutines(self.openai))
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
                                 "of each category. There may not always be 5 categories."
                         )

    handler.run()
    ic(handler.output_data)

# https://superfastpython.com/asyncio-as_completed/#Example_of_as_completed_with_Coroutines
# TODO - Add Error checking
# TODO - Get structured output happening using the pydantic classes
# TODO - Add retry loops on incorrect output
# TODO - Add re-query response correction on incorrect output format
# TODO - test chain integration
# TODO - Add more useful methods and stuff
