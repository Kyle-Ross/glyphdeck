"""Pydantic models, types, fields, and classes for data validation.

For use by the `BaseLLMHandler` class to enforce specific output types from the LLM.
"""

from typing import Union, List

from pydantic import BaseModel, Field, field_validator

from glyphdeck.tools.logging_ import ValidatorsLogger, assert_and_log_error

logger = ValidatorsLogger().setup()

# Pydantic Models, Types, Fields and Classes for import and use elsewhere in the program for data validation
# Used to assert and advise the expected output from provider calls

# Global variables which define the min and max characters allowed in various fields
sentiment_min = -1.00
sentiment_max = 1.00


# ---BASE MODELS---
# Add field names to the field_validator arguments if you want them to be validated by a method
class BaseValidatorModel(BaseModel):
    """Base class for validator models. Provides common field validations, which are used in columns match the arguments.

    Multiple validations can apply to a single field if the column name is in multiple validation rules.

    Args:
        BaseModel: The base class for Pydantic models.

    Returns:
        BaseValidatorModel: An instance of the base validator model.

    """

    # Decorator needed to check field uses since the item_model inherits from base
    @field_validator("sentiment_score", check_fields=False)
    def check_decimal_places(cls, v: Union[float, int]) -> Union[float, int]:
        """Check that the value has no more than two decimal places.

        Args:
            v: The value to be validated, either float or int.

        Returns:
            The validated value if it passes the check.

        """
        if isinstance(v, float) or v in (
            -1,
            0,
            1,
        ):  # Allows some integers if inside the range
            assert_and_log_error(
                logger,
                "warning",
                round(v, 2) == v,
                "value cannot have more than 2 decimal places",
            )
        return v

    @field_validator("sentiment_score", check_fields=False)
    def sentiment_float_in_range(cls, v: Union[float, int]) -> Union[float, int]:
        """Ensure the sentiment score is within the allowed range.

        Args:
            v: The value to be validated, either float or int.

        Returns:
            The validated value if it is within the specified range.

        """
        global sentiment_min, sentiment_max
        if isinstance(v, float) or v in (-1, 0, 1):
            minimum: float = sentiment_min
            maximum: float = sentiment_max
            assert_and_log_error(
                logger,
                "warning",
                minimum <= v <= maximum,
                f"sentiment float must be between {minimum} to {maximum}",
            )
        return v

    @field_validator("per_sub_category_sentiment_scores", check_fields=False)
    def list_of_sentiment_floats_in_range(
        cls, v: List[Union[float, int]]
    ) -> List[Union[float, int]]:
        """Validate a list of sentiment scores ensuring values are floats within the allowed range.

        Args:
            v: The list of values to be validated.

        Returns:
            The validated list of values.

        """
        global sentiment_min, sentiment_max
        if isinstance(v, list):
            for x in v:
                assert_and_log_error(
                    logger,
                    "warning",
                    type(x) is float or x in (-1, 0, 1),
                    f"{x} is not a float",
                )
                assert_and_log_error(
                    logger,
                    "warning",
                    round(x, 2) == x,
                    f"value {x} cannot have more than 2 decimal places",
                )
                # Assert sentiment is in the allowed range as assigned in the global variables
                minimum: float = sentiment_min
                maximum: float = sentiment_max
                assert_and_log_error(
                    logger,
                    "warning",
                    minimum <= x <= maximum,
                    f"sentiment float {x} must be between {minimum} to {maximum}",
                )
        return v

    @field_validator("top_categories", check_fields=False)
    def list_1_to_5(cls, v: List) -> List:
        """Validate that the list contains between 1 to 5 entries.

        Args:
            v: The list to be validated.

        Returns:
            The validated list if it passes the check.

        """
        if isinstance(v, list):
            minimum: int = 1
            maximum: int = 5
            assert_and_log_error(
                logger,
                "warning",
                minimum <= len(v) <= maximum,
                f"list must contain between {minimum} to {maximum} entries",
            )
        return v

    @field_validator("sub_categories", check_fields=False)
    def list_1_to_30(cls, v: List) -> List:
        """Validate that the list contains between 1 to 30 entries.

        Args:
            v: The list to be validated.

        Returns:
            The validated list if it passes the check.

        """
        if isinstance(v, list):
            minimum: int = 1
            maximum: int = 30
            assert_and_log_error(
                logger,
                "warning",
                minimum <= len(v) <= maximum,
                f"list must contain between {minimum} to {maximum} entries",
            )
        return v


# ---FIELDS---
# Field() can customise the pydantic JSON schema
# Used here to add a description to each field
# Defined separately to avoid repetition in similar classes below
sentiment_score: float = Field(
    description="A 2 decimal value that represents the overall sentiment of the input. Ranges from -1.00 "
    "(max negative sentiment) to 1.00 (max positive sentiment), with 0.00 indicating neutral "
    "sentiment. It must be between -1.00 and 1.00"
)

per_sub_category_sentiment_scores: list = Field(
    description="A list of sentiment scores corresponding to the list of identified sub-categories. Each score is "
    "a 2 decimal value that represents sentiment of the corresponding sub-categories as it was used inside "
    "the input. Each score ranges from -1.00 (max negative sentiment) to 1.00 (max positive "
    "sentiment), with 0.00 indicating neutral sentiment. It must be between -1.00 and 1.00. The list "
    "should be of equal length to the list of corresponding sub-categories, and in the same order."
)

primary_category: str = Field(
    description="The primary category identified inside the input. Each category name should be concise."
)

top_5_categories: list = Field(
    description="The top 1 to 5 sub-categories identified inside the input in order of relevance. "
    "Each category name should be concise."
)

categories_1_to_30: list = Field(
    description="All sub-categories identified inside the input in order of relevance, making sure to capture all the "
    "topics, with least 1 and no more than 30 categories. Each category name should be concise."
)


# ---CLASSES---
# Classes use the common fields above
# Validated according to @field_validator methods defined in the BaseValidatorModel if the field name matches
# Type annotations need to be repeated, but use the above as a reference.
# Pydantic excludes attributes that start with an underscore from validation actions


# ---Sentiment Classes---
class Sentiment(BaseValidatorModel):
    """Validation model for representing sentiment scores.

    Attributes:
        _field_count: The number of fields in the model.
        sentiment_score: The overall sentiment score.

    """

    _field_count: int = 1
    sentiment_score: float = sentiment_score


# ---Categorising Classes---
class PrimaryCat(BaseValidatorModel):
    """Validation model for representing the primary category.

    Attributes:
        _field_count: The number of fields in the model.
        primary_category: The primary category identified.

    """

    _field_count: int = 1
    primary_category: str = primary_category


class Top5Cats(BaseValidatorModel):
    """Validation model for representing the top 1 to 5 categories identified.

    Attributes:
        _field_count: The number of fields in the model.
        top_categories: The top 1 to 5 sub-categories identified inside the input in order of relevance.

    """

    _field_count: int = 1
    top_categories: list = top_5_categories


class SubCats(BaseValidatorModel):
    """Validation model for representing sub-categories.

    Attributes:
        _field_count: The number of fields in the model.
        sub_categories: All sub-categories identified inside the input in order of relevance.

    """

    _field_count: int = 1
    sub_categories: list = categories_1_to_30


class PrimaryCatSentiment(BaseValidatorModel):
    """Validation model for representing a primary category with its associated sentiment score.

    Attributes:
        _field_count: The number of fields in the model.
        primary_category: The primary category identified.
        sentiment_score: The overall sentiment score.

    """

    _field_count: int = 2
    primary_category: str = primary_category
    sentiment_score: float = sentiment_score


class PrimarySubCat(BaseValidatorModel):
    """Validation model for representing a primary category with its associated sub-categories.

    Attributes:
        _field_count: The number of fields in the model.
        primary_category: The primary category identified.
        sub_categories: All sub-categories identified inside the input in order of relevance.

    """

    _field_count: int = 2
    primary_category: str = primary_category
    sub_categories: list = categories_1_to_30


class SubCatsSentiment(BaseValidatorModel):
    """Validation model for representing sub-categories with an associated overall sentiment score.

    Attributes:
        _field_count: The number of fields in the model.
        sub_categories: All sub-categories identified inside the input in order of relevance.
        sentiment_score: The overall sentiment score.

    """

    _field_count: int = 2
    sub_categories: list = categories_1_to_30
    sentiment_score: float = sentiment_score


class SubCatsPerItemSentiment(BaseValidatorModel):
    """Validation model for representing sub-categories with individual sentiment scores.

    Attributes:
        _field_count: The number of fields in the model.
        sub_categories: All sub-categories identified inside the input in order of relevance.
        per_sub_category_sentiment_scores: A list of sentiment scores corresponding to the list of identified sub-categories.

    """

    _field_count: int = 2
    sub_categories: list = categories_1_to_30
    per_sub_category_sentiment_scores: list = per_sub_category_sentiment_scores


class SubCatsPerItemOverallSentiment(BaseValidatorModel):
    """Validation model for representing sub-categories with individual sentiment scores and an overall sentiment score.

    Attributes:
        _field_count: The number of fields in the model.
        sub_categories: All sub-categories identified inside the input in order of relevance.
        per_sub_category_sentiment_scores: A list of sentiment scores corresponding to the list of identified sub-categories.
        sentiment_score: The overall sentiment score.

    """

    _field_count: int = 3
    sub_categories: list = categories_1_to_30
    per_sub_category_sentiment_scores: list = per_sub_category_sentiment_scores
    sentiment_score: float = sentiment_score


class TopCatsSentiment(BaseValidatorModel):
    """Validation model for representing the top sub-categories with an associated overall sentiment score.

    Attributes:
        _field_count: The number of fields in the model.
        top_categories: The top 1 to 5 sub-categories identified inside the input in order of relevance.
        sentiment_score: The overall sentiment score.

    """

    _field_count: int = 2
    top_categories: list = top_5_categories
    sentiment_score: float = sentiment_score


class CatHierarchySentiment(BaseValidatorModel):
    """Validation model for representing a category hierarchy with an associated overall sentiment score.

    Attributes:
        _field_count: The number of fields in the model.
        primary_category: The primary category identified.
        sub_categories: All sub-categories identified inside the input in order of relevance.
        sentiment_score: The overall sentiment score.

    """

    _field_count: int = 3
    primary_category: str = primary_category
    sub_categories: list = categories_1_to_30
    sentiment_score: float = sentiment_score


def list_models():
    """Print out the built-in validation models, including their names and descriptions."""
    # Models to print out
    validation_models = [
        Sentiment,
        PrimaryCat,
        Top5Cats,
        SubCats,
        PrimaryCatSentiment,
        PrimarySubCat,
        SubCatsSentiment,
        SubCatsPerItemSentiment,
        SubCatsPerItemOverallSentiment,
        TopCatsSentiment,
        CatHierarchySentiment,
    ]

    def print_line(char: str, length: int, newlines_after: int):
        """Print a line consisting of a repeated character followed by a specified number of newlines.

        Args:
            char (str): The character to repeat in the line.
            length (int): The number of times to repeat the character.
            newlines_after (int): The number of newlines to print after the line.

        Returns:
            None

        """
        line = char * length
        newlines = "\n" * newlines_after
        print(line + newlines)

    print("\n")
    print_line("-", 40, 0)
    print("List of built-in validation models:")
    print_line("-", 40, 2)

    for cls in validation_models:
        print_line("*", 25, 0)
        print(cls.__name__)
        print_line("*", 25, 1)
        print(f"{cls.__doc__}")
