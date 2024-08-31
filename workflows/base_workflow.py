import os

from CategoriGen.processors.sanitiser import Sanitiser
from CategoriGen.processors.llm_handler import LLMHandler
from CategoriGen.processors.prepper import Prepper
from CategoriGen.processors.chain import Chain
from CategoriGen.tools.loggers import (
    BaseWorkflowLogger,
    UnhandledErrorsLogger,
    LogBlock,
    exception_logger,
)
from CategoriGen.tools.time import RuntimeLogBlock
from CategoriGen.validation import validators

logger = BaseWorkflowLogger().setup()
unhandled_errors_logger = UnhandledErrorsLogger().setup()
current_file_name = os.path.basename(__file__)


@exception_logger(unhandled_errors_logger)
def main():
    # Set file vars
    source_file = r"F:\Github\CategoriGen\scratch\Womens clothing reviews\Womens Clothing E-Commerce Reviews - 1000.xlsx"
    source_file_type = source_file.split(".")[-1]

    # Intialising a chain object, ready to have data appended
    chain = Chain()

    # Initialising prepper object,
    # Then loading, validating and preparing the data
    prep = Prepper(
        file_path=source_file,
        file_type=source_file_type,
        encoding="ISO-8859-1",
        id_column="Row ID",
        data_columns=["Review Text"],
    ).prepare()

    # Adding the prepared data to the chain object
    chain.append(
        title="prepared data",
        data=prep.output_data,
        table=prep.df,
        table_id_column=prep.id_column,
        column_names=prep.data_columns,
    )

    # Sanitising the data of sensitive information, on specified patterns
    sanitised = Sanitiser(
        chain.latest_data, pattern_groups=["date", "email", "path", "url", "number"]
    ).sanitise()

    # Adding sanitised results to the chain
    # Unspecified arguments (table, table_id_column, column_names) are inherited from the previous entry if referenced
    chain.append(title="sanitised data", data=sanitised.output_data)

    handler = LLMHandler(
        chain.latest_data,
        provider="OpenAI",
        model="gpt-3.5-turbo",
        role="An expert customer feedback analyst nlp system",
        request="Analyse the feedback and return results in the correct format",
        validation_model=validators.SubCategoriesWithPerItemSentimentAndOverallSentiment,
        cache_identifier="NLPPerCategorySentimentAndOverallSentimentWomensClothesReview",
        use_cache=True,
        temperature=0.2,
        max_validation_retries=3,
        max_preprepared_coroutines=10,
        max_awaiting_coroutines=100,
    )
    handler.run()
    handler.flatten_output_data(column_names=chain.latest_column_names)
    chain.append(
        title="llmoutput",
        data=handler.output_data,
        column_names=handler.column_names,
        update_expected_len=True,
    )  # Updating len since the validators can produce multiple columns per input

    chain.output(
        records=[chain.latest_title],
        file_type="xlsx",
        name_prefix="Chain Test",
        rejoin=True,
        split=False,
    )


if (
    __name__ == "__main__"
):  # Run the main code, with decorators and context managers in effect
    with RuntimeLogBlock(logger), LogBlock(
        current_file_name, logger
    ):  # These run nested, in order (outer to inner)
        main()
