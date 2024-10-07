validators
==================================

Key to glyphdeck is the Pydantic validation model that is passed to the ``LLMHandler`` to direct its output.

.. rubric:: Access
    :heading-level: 2

.. code-block:: python

    gd.validators.ModelName

.. rubric:: Usage
    :heading-level: 2

.. code-block:: python
    :emphasize-lines: 8

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

.. rubric:: Built-in
    :heading-level: 2

For a given text input the LLM will be instructed to return the:

- :doc:`.Sentiment` - sentiment score (-1.00 to 1.00)
- :doc:`.PrimaryCat` - primary category 
- :doc:`.Top5Cats` - top 5 categories
- :doc:`.SubCats` - sub-categories
- :doc:`.PrimaryCatSentiment` - primary category and sentiment score
- :doc:`.PrimarySubCat` - primary category and sub-categories
- :doc:`.SubCatsSentiment` - sub-categories and sentiment score
- :doc:`.SubCatsPerItemSentiment` - sub-categories with sentiment per item
- :doc:`.SubCatsPerItemOverallSentiment` - sub-categories with sentiment score per item and the overall sentiment score
- :doc:`.TopCatsSentiment` - top categories and sentiment score
- :doc:`.CatHierarchySentiment` - full hierarchy of categories and the overall sentiment score

.. rubric:: Custom
    :heading-level: 2

Any valid Pydantic ``BaseModel`` can be used for validation. You can also inherit from :doc:`.BaseValidatorModel` to make use of the logic used in the built-in validators.

Refer to the Pydantic `documentation <https://docs.pydantic.dev/latest/concepts/models/#validation>`_ for more info.
