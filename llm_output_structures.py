# TODO - In NLP processing section add retry loop on ValidationError, with prompt add to get ChatGPT to fix the output
# TODO - Review any rechecking methods available in langchain

from pydantic import BaseModel, Field, conset, constr, field_validator

# Pydantic Models, Types, Fields and Classes for import and use elsewhere in the program
# Used to assert and advise the expected output from llm calls


# ---BASE MODELS---
class DecimalValidatorModel(BaseModel):
    """Adds decimal place validation to the base item_model. Only checks 'sentiment_score'"""
    @field_validator('sentiment_score', check_fields=False)  # Check fields uses since the item_model inherits from base
    def check_decimal_places(cls, v):
        if isinstance(v, float):
            assert round(v, 2) == v, 'value has more than 2 decimal places'
        return v


# ---TYPES---
# Custom data types used in pydantic fields below
category_constr_type = constr(strip_whitespace=True, to_lower=True, min_length=3, max_length=30)
sub_categories_type = conset(item_type=category_constr_type, min_length=1)
top_5_sub_categories_type = conset(item_type=category_constr_type, min_length=1, max_length=5)

# ---FIELDS---
# Common Fields to be added to pydantic models below, using generic python types and types defined above
main_category_field: category_constr_type = Field(
    description="The primary category identified in the comment"
)

categories_field: sub_categories_type = Field(
    description="The sub-categories identified in the comment in order of relevance"
)

top_5_sub_categories_field: top_5_sub_categories_type = Field(
    description="The top 1 to 5 sub-categories identified in the comment in order of relevance"
)

sentiment_score_field: float = \
    Field(
        description="Decimal value that represents sentiment of a comment. Ranges from -1 (negative sentiment) to 1 "
                    "(positive sentiment), with 0 indicating neutral sentiment",
        ge=-1,  # Greater than or equal to
        le=1  # Less than or equal to
    )

# ---CLASSES---
# Common classes to be imported and used elsewhere, combining the Fields defined above
# Unfortunately the type annotations need to be repeated, but use the above as a reference.


class SentimentScore(DecimalValidatorModel):
    sentiment_score: float = sentiment_score_field


class CategoryList(BaseModel):
    categories: sub_categories_type = categories_field


class CategoryListAndSentiment(DecimalValidatorModel):
    categories: sub_categories_type = categories_field
    sentiment_score: float = sentiment_score_field


class TopCategoriesListAndSentiment(DecimalValidatorModel):
    categories: top_5_sub_categories_type = top_5_sub_categories_field
    sentiment_score: float = sentiment_score_field


class CategoryListHierarchyAndSentiment(DecimalValidatorModel):
    main_category: category_constr_type = main_category_field
    sub_categories: top_5_sub_categories_type = top_5_sub_categories_field
    sentiment_score: float = sentiment_score_field


if __name__ == "__main__":
    from icecream import ic
    """Only runs below if script is run directly, not on import, so this is for testing purposes"""
    # Creating an instance and checking if it is from BaseModel
    pydantic_model_instance = SentimentScore(sentiment_score=0.55)
    float_variable = 0.666
    ic(pydantic_model_instance)
    ic(float_variable)
    ic(type(pydantic_model_instance))
    ic(isinstance(pydantic_model_instance, BaseModel))
    ic(isinstance(float_variable, BaseModel))
