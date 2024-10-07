# GlyphDeck

The glyphdeck library is a comprehensive toolkit designed to streamline
& simplify various aspects of asynchronous LLM data processing workflows
over high-volume, dense semantic data - like customer feedback, reviews
and comments.

Common aspects of the LLM data workflow are handled end-to-end including
async llm calls, data validation, sanitisation, transformation, caching
and step chaining, facilitating the fast development of robust, error
free LLM data workflows.

Its also equiped with caching and a configurable logging facility that
makes complex asyncronous LLM workflows much easier to configure,
understand and debug.

[![view - Documentation](https://img.shields.io/badge/view-Documentation-blue?style=for-the-badge)](https://glyphdeck.readthedocs.io/ "Go to project documentation")

## Features

-   **Asynchronous**: Rip through tabular data with asynchronous llm
    usage
-   **Auto-scaling**: Don't worry about api errors - the `LLMHandler`
    automatically scales to your api limits
-   **Rapid config**: Simple syntax in a highly abstracted interface
    makes set up a breeze
-   **Data chaining**: Automatically pass data between steps with the
    `Cascade` class
-   **Self validating**: The Cascade class automatically maintains
    dataset integrity
-   **Pydantic models**: Use built-in or custom Pydantic data models for
    structured llm output
-   **Type enforcement**: Get your structured data in the correct format
    from the llm every time
-   **Private data**: `Sanitiser` class strips out common private data
    patterns before making api calls
-   **Automatic caching**: Save on API costs and re-use results
-   **Logging**: Built-in logging facility helps to debug your set-ups &
    peer inside the event loop

## Set-up

### Install

``` 
pip install glyphdeck
```

### Set API Key

Set your `OPENAI_API_KEY` with a
[command](https://platform.openai.com/docs/quickstart?language-preference=python),
or add it to your [environment
variables](https://en.wikipedia.org/wiki/Environment_variable).

``` 
setx OPENAI_API_KEY "your_api_key_here"
```

### Import

``` python
import glyphdeck as gd
```

## Usage

### Cascades

The `Cascade` class is the primary interface for the glyphdeck library.
It handles and processes data in a record-like structure, providing easy
to use syntax for LLM data handling workflows.

It validates and enforces all data movements against a common id,
ensuring that each record has a unique, immutable identifier that
remains consistent, regardless of other changes.

This all combines to let you spin up LLM workflows in record time.

### Example

``` python
import glyphdeck as gd

# Provide a dataframe or a path to a file (csv or xlsx)
data_source = r"tests\testdata.pizzashopreviews.xlsx"

# Intialise cascade instance and identify the unique id (required) and target data
cascade = gd.Cascade(data_source, "Review", "Review Text")

# Optionally remove private information
cascade.sanitiser.run()

# Prepare the llm
cascade.set_llm_handler(
    provider="OpenAI",
    model="gpt-4o-mini",
    system_message=(
        "You are an expert pizza shop customer feedback analyst system."
        "Analyse the feedback and return results in the correct format."
    ),
    validation_model=gd.validators.SubCatsSentiment,
    cache_identifier="pizzshop_sentiment",
)

# Run the llm_handler
cascade.llm_handler.run("llm_category_sentiment")

```

### Output

| Review Id | Review                                                                                                                                  | Review_sub_categories                                                          | Review_sentiment_score |
|-----------|-----------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|------------------------|
| 1         | I love this place, fantastic pizza and great service. It's family run and it shows. Out of heaps of good local options it is still the… | Pizza Quality,Service Quality,Family Run,Local Options,Pricing,Parking         | 0.95                   |
| 2         | Got disappointed. Not good customer service. definitely not great pizza.We went there yesterday to…                                     | Customer Service,Pizza Quality,Dine-In Experience,Promotions,Family Experience | -0.75                  |

------------------------------------------------------------------------

#### 🚀 That's just the start!

Glyphdeck is highly customisable with useful optional commands in every
function, class & method. Use the Cascade and other tools to mix and
match, create feedback loops and more - all with blazing asynchronous
speed through the OpenAI API.
