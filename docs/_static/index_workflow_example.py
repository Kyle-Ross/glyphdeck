import glyphdeck as gd

# Provide a dataframe or a path to a file (csv or xlsx)
data_source = r"tests\testdata.pizzashopreviews.xlsx"

# Intialise cascade instance and identify the unique id (required) and target data
cascade = gd.Cascade(data_source, "Review Id", "Review Text")

# Optionally remove private information
cascade.sanitiser.run()

# Prepare the llm
cascade.set_llm_handler(
    provider="OpenAI",
    model="gpt-4o-mini",
    system_message="You are an expert pizza shop customer feedback analyst system. Analyse the feedback and return results in the correct format.",
    validation_model=gd.validators.PrimaryCatSentiment,
    cache_identifier="pizzshop_sentiment",
)

# Run the llm_handler
cascade.llm_handler.run("llm_category_sentiment")

# Get the result
output = cascade.get_output()
