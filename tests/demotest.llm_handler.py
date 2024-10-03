import logging

# Disable logging for duration
logging.disable(logging.CRITICAL)

from icecream import ic  # noqa: E402

import glyphdeck as gd  # noqa: E402

print("\nCreate the test data")
test_data: gd.DataDict = {
    1: ["berries", "insects", "small mammals"],
    2: ["fruits", "fish", "seeds"],
    3: ["nuts", "leaves", "grubs"],
}
ic(test_data)

print("\nCreate the handler object")
handler = gd.LLMHandler(
    test_data,
    provider="OpenAI",
    model="gpt-4o-mini",
    system_message=(
        "You are an expert word categorisation system. "
        "Analyse the words and provide a primary category "
        "representing the animal most likely to eat them."
    ),
    validation_model=gd.validators.PrimaryCat,
    cache_identifier="cascade_llm_print_test_primary_category",
    use_cache=False,
    temperature=0.2,
    max_validation_retries=3,
    max_preprepared_coroutines=10,
)
ic(handler)

print("\nRun the handler on the data")
ic(handler.run_async())

print(
    "\nFlatten the output into the cascade format (same as the test data). Can generate more than one response column for each input column."
)
ic(handler.flatten_output_data(["Col1", "Col2", "Col3"]))
ic(handler.output_data)

# Re-enable logging
logging.disable(logging.NOTSET)
