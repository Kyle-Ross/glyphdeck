import glyphdeck as gd

# Set file vars
source_file = r"tests\testdata.pizzashopreviews.xlsx"

# Intialising a cascade object, setting its first record from the source file
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
    model="gpt-4o-mini",
    system_message="You are an expert customer feedback analyst nlp system. Analyse the feedback and return results in the correct format.",
    validation_model=gd.validators.SubCats,
    cache_identifier="PizzaShopComment_Sub_Categories",
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
