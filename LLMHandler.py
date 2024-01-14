from pydantic import BaseModel
from openai import OpenAI
from custom_types import Data
import llm_output_structures
from icecream import ic
import pandas as pd
import os
import asyncio
from openai import AsyncOpenAI


class LLMHandler:
    """Write your docstring for the class here."""

    def check_instance(self):
        """Checks that the provided variable is an instance or inheritance of the Pydantic BaseModel class."""
        check: bool = isinstance(self.output_structure, BaseModel)
        assert check, f'{self.output_structure} is not an instance of, or inherited from the Pydantic BaseModel class'

    def __init__(self,
                 input_data: Data,
                 parser: str,
                 output_structure,
                 model: str,
                 api_key: str,
                 role: str,
                 request: str) -> None:

        self.input_data: Data = input_data
        self.output_data: Data = input_data  # Will be changed by processes below

        self.parser: str = parser  # The name of the method used to advise output structure
        self.output_structure = output_structure

        self.model: str = model
        self.api_key: str = api_key
        self.role: str = role
        self.request: str = request

        # Check that the class attribute is the correct type
        self.check_instance()

    def run_openai(self,
                   item_text: str,
                   item_parser: str = None,
                   item_output_structure=None,
                   item_model: str = None,
                   item_api_key: str = None,
                   item_role: str = None,
                   item_request: str = None):
        """Per-item I/O with OpenAI."""
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
        openai_client = OpenAI(api_key=item_api_key)

        # Sending the request
        chat_completion = openai_client.chat.completions.create(
            model=item_model,
            messages=[

                {"role": "system",
                 "content": item_role},

                {"role": "user",
                 "content": item_request + ' ' + item_text}
            ]
        )

        # Storing the response
        response = chat_completion.choices[0].message.content

        # Returning the output
        return response

    def run_test(self):
        for key, list_value in self.input_data.items():
            print(f'List {key} start')
            result_list = []
            for item_value in list_value:
                print(f'List {key} - Item x start')
                response = self.run_openai(item_value)
                result_list.append(response)
                print(f'List {key} - Item x end')
            self.output_data[key] = result_list
            print(f'List {key} end')
        print(self.output_data)


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
                'Tasty with fresh ingredients and not too greasy, I would recommend.'
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
                         model="gpt-3.5-turbo",
                         api_key=my_api_key,
                         role="An expert nlp comment analysis system, trained to accurately categorise " \
                              "customer feedback",
                         request="Provide the top 5 most relevant categories for the comment. Just provide the title"
                                 "of each category. There may not always be 5 categories."
                         )

    handler.run_test()

# TODO - https://github.com/openai/openai-python - has async built in!