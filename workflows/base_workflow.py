import glyphdeck as gd

from glyphdeck.tools._logging import (
    exception_logger,
)
from glyphdeck.tools._time import RuntimeLogBlock

@exception_logger(gd.loggers.unhandled_errors)
def main():
    # Set file vars
    source_file = r"tests\testdata.pizzashopreviews.xlsx"

    # Intialising a cascade object, __init__ appending its first record from the source file
    cascade = gd.Cascade(
        data_source=source_file,
        id_column="Review Id",
        data_columns=["Review Text"],
        encoding="ISO-8859-1",
    )

    # Sanitising the data of sensitive information, on default patterns and appends the result
    cascade.sanitiser.run()

    # Set the LLM Handler for this cascade instance
    cascade.set_llm_handler(
        provider="OpenAI",
        model="gpt-3.5-turbo",
        system_message="You are an expert customer feedback analyst nlp system. Analyse the feedback and return results in the correct format.",
        validation_model=gd.validators.SubCats,
        cache_identifier="PizzaShipComment_Sub_Categories",
        use_cache=True,
        temperature=0.2,
        max_validation_retries=3,
        max_preprepared_coroutines=10,
        max_awaiting_coroutines=100,
    )

    # Run the llm_handler
    cascade.llm_handler.run("HandlerOutput1")

    # Output the result in the specified format
    # Latest record is used by default, but we specify it here
    cascade.write_output(
        file_type="xlsx",
        file_name_prefix="Cascade Test",
        record_identifiers=["sanitised", "HandlerOutput1"],
        rebase=True,
        xlsx_use_sheets=False,
    )


if (
    __name__ == "__main__"
):  # Run the main code, with decorators and context managers in effect
    with RuntimeLogBlock(gd.loggers.workflow):  # These run nested, in order (outer to inner)
        main()
