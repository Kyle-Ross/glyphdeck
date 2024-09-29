from CategoriGen.processors.chain import Chain
from CategoriGen.tools.loggers import (
    BaseWorkflowLogger,
    UnhandledErrorsLogger,
    exception_logger,
)
from CategoriGen.tools.time import RuntimeLogBlock
from CategoriGen.validation import validators

logger = BaseWorkflowLogger().setup()
unhandled_errors_logger = UnhandledErrorsLogger().setup()


@exception_logger(unhandled_errors_logger)
def main():
    # Set file vars
    source_file = r"F:\Github\CategoriGen\scratch\Womens clothing reviews\Womens Clothing E-Commerce Reviews - 100.csv"

    # Intialising a chain object, __init__ appending its first record from the source file
    chain = Chain(
        data_source=source_file,
        id_column="Row ID",
        data_columns=["Review Text"],
        encoding="ISO-8859-1",
    )

    # Sanitising the data of sensitive information, on default patterns and appends the result
    chain.sanitiser.run()

    # Set the LLM Handler for this chain instance
    chain.set_llm_handler(
        provider="OpenAI",
        model="gpt-3.5-turbo",
        system_message="You are an expert customer feedback analyst nlp system. Analyse the feedback and return results in the correct format.",
        validation_model=validators.SubCats,
        cache_identifier="WomensClothesReview_Comment_Sub_Categories",
        use_cache=True,
        temperature=0.2,
        max_validation_retries=3,
        max_preprepared_coroutines=10,
        max_awaiting_coroutines=100,
    )

    # Run the llm_handler
    chain.llm_handler.run("HandlerOutput1")

    # Output the result in the specified format
    # Latest record is used by default, but we specify it here
    chain.write_output(
        file_type="xlsx",
        file_name_prefix="Chain Test",
        record_identifiers=["sanitised", "HandlerOutput1"],
        rebase=True,
        xlsx_use_sheets=False,
    )


if (
    __name__ == "__main__"
):  # Run the main code, with decorators and context managers in effect
    with RuntimeLogBlock(logger):  # These run nested, in order (outer to inner)
        main()
