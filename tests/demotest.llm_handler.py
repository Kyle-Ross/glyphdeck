import logging

# Disable logging for duration
logging.disable(logging.CRITICAL)

from icecream import ic  # noqa: E402

from CategoriGen.validation.data_types import Data  # noqa: E402
from CategoriGen.processors.llm_handler import LLMHandler  # noqa: E402
from CategoriGen.validation import validators  # noqa: E402

print("\nCreate the test data")
test_data: Data = {
    1: ["berries", "insects", "small mammals"],
    2: ["fruits", "fish", "seeds"],
    3: ["nuts", "leaves", "grubs"],
}
ic(test_data)

print("\nCreate the handler object")
handler = LLMHandler(
    test_data,
    provider="OpenAI",
    model="gpt-3.5-turbo",
    role="An expert word categorisation system",
    request="Analyse the words and provide a primary category representing the animal most likely to eat them",
    validation_model=validators.PrimaryCategory,
    cache_identifier="chain_llm_print_test_primary_category",
    use_cache=False,
    temperature=0.2,
    max_validation_retries=3,
    max_preprepared_coroutines=10,
)
ic(handler)

print("\nRun the handler on the data")
ic(handler.run_async())

print(
    "\nFlatten the output into the chain format (same as the test data). Can generate more than one response column for each input column."
)
ic(handler.flatten_output_data(["Col1", "Col2", "Col3"]))
ic(handler.output_data)
