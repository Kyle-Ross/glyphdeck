from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from utility import print_time_since_start, string_cleaner
from custom_types import Data, assert_custom_type
from icecream import ic
import llm_output_structures
import instructor
import asyncio
import openai


class LLMHandler:
    """Write your docstring for the class here."""

    def check_validation_model(self):
        """Checks that the provided class is an instance or inheritance of the Pydantic BaseValidatorModel class."""
        check: bool = issubclass(self.validation_model, llm_output_structures.BaseValidatorModel)
        assert check, f'{self.validation_model.__name__} is not a subclass of the Pydantic BaseValidatorModel class'

    def __init__(self,
                 input_data: Data,  # Input variable of the custom 'Data' type
                 provider: str,  # The provider of the llm
                 model: str,  # The model of the llm
                 api_key: str,  # The api key to be used
                 role: str,  # The role to provide the llm in the prompts
                 request: str,  # The request to make to the llm
                 validation_model,  # Pydantic class to use for validating output, checked by check_validation_model()
                 temperature: float = 0.2,  # How deterministic (low num) or random (high num) the responses will be
                 max_validation_retries: int = 2):  # Max times the request can retry on the basis of failed validation
        """__init__ func which is run when the object is initialised."""

        # Assert the variable type of the provided arguments
        assert_custom_type(input_data, "Data", "input_data")  # Check the custom data type 'Data'
        assert isinstance(provider, str), "'provider' argument must be type 'str'"
        assert isinstance(model, str), "'model' argument must be type 'str'"
        assert isinstance(api_key, str), "'api_key' argument must be type 'str'"
        assert isinstance(role, str), "'role' argument must be type 'str'"
        assert isinstance(request, str), "'request' argument must be type 'str'"
        assert isinstance(temperature, float), "'temperature' argument must be type 'float'"
        assert isinstance(max_validation_retries, int), "'max_validation_retries' argument must be type 'int'"

        # Storing the input variable, of the 'Data' type as typically delivered by a 'Chain' object
        self.input_data: Data = input_data
        # output_data keeps keys, replaces with [None, None, ...] lists
        # Helps to insert output in the correct position later in 'await_tasks'
        self.output_data: Data = {key: [None] * len(value) for key, value in input_data.items()}

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
        self.validation_model = validation_model
        self.temperature: float = temperature
        self.max_validation_retries = max_validation_retries

        # Checks that model comes from customer Pydantic BaseValidatorModel class
        self.check_validation_model()

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
    async def async_openai(self,
                           input_text: str,
                           key,
                           index: int,
                           item_model: str = None,
                           item_api_key: str = None,
                           item_role: str = None,
                           item_request: str = None,
                           item_validation_model=None,
                           item_temperature: float = None,
                           item_max_validation_retries: int = None) -> tuple:
        """Asynchronous Per-item coroutine generation with OpenAI. Has exponential backoff on specified errors."""

        # If no arguments are provided, uses the values set in the handler class instance
        # Necessary to do it this way since self is not yet defined in this function definition
        if item_model is None:
            item_model = self.model
        if item_api_key is None:
            item_api_key = self.api_key
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
        assert 0 <= item_temperature <= 1, "For OpenAI, temperature must be between 0 and 1"

        # Initialising the client
        # instructor patches in variable validation via pydantic with the response_model and max_retries attributes
        openai_client = instructor.apatch(openai.AsyncOpenAI(api_key=item_api_key))

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
                {"role": "user", "content": item_request + ' ' + input_text}
            ]
        }
        # Accessing parameters via dict unpack
        instructor_model = await openai_client.chat.completions.create(**chat_params)

        # Storing the response object (as made by the patched openai_client)
        # Extracting a dict of the fields using the pydantic basemodel
        response = dict(instructor_model)

        # Returning the response as a tuple (shorthand syntax)
        return response, key, index

    async def create_coroutines(self, func) -> list:
        """Creates coroutines for the provided input data, using the specified LLM function."""
        # Create and store the tasks across the whole variable in a list
        coroutines = [func(input_text=item_value, key=key, index=index)  # List of coroutines
                      for key, list_value in self.input_data.items()  # For every key in the input_data dict
                      for index, item_value in enumerate(list_value)]  # For every item in every list
        return coroutines

    async def await_coroutines(self, func):
        """Await coroutines,  returning results in the order of completion, not the order they are run."""
        coroutines = await self.create_coroutines(func)
        completions = 0
        total_coroutines_str_len = len(str(len(coroutines)))
        # Loop over the futures
        for future in asyncio.as_completed(coroutines):
            result = await future
            response = result[0]
            key = result[1]
            index = result[2]
            # Print a string to track progress while script is running
            completions += 1
            completions_str = str(completions)
            completions_string = ((total_coroutines_str_len - len(completions_str)) * '0') + str(completions)
            print(f'{completions_string} | SUCCESS | Key: {key} | Index: {index} | {print_time_since_start()}')
            self.output_data[key][index] = response

    def run(self):
        """Asynchronously query the selected LLM across the whole variable and save results to the output"""
        if self.provider_clean == "openai":
            asyncio.run(self.await_coroutines(self.async_openai))
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

    handler = LLMHandler(data_example,
                         provider="OpenAI",
                         model="gpt-3.5-turbo",
                         api_key=my_api_key,
                         role="An expert customer feedback analyst nlp system",
                         request="Analyse the feedback and return results in the correct format",
                         validation_model=llm_output_structures.PrimaryCategoryAndSubCategory,
                         temperature=0.2,
                         max_validation_retries=3
                         )

    handler.run()
    ic(handler.output_data)

# TODO - Test backoff system
# TODO - Test errors
# TODO - Logging for backoffs
# TODO - Logging for exceptions
# TODO - Add Coroutine timeout
