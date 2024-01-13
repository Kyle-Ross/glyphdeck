# TODO - In NLP processing section add retry loop on ValidationError, with prompt add to get ChatGPT to fix the output
# TODO - Review any rechecking methods available in langchain

from pydantic import BaseModel, Field, conlist, constr

# Pydantic Types, Fields and Classes for import and use elsewhere in the program
# Used to assert and advise the expected output from llm calls

# ---TYPES---
# Custom data types used in pydantic fields below
category_constr_type = constr(strip_whitespace=True, to_lower=True, min_length=3, max_length=30)
sub_categories_type = conlist(item_type=category_constr_type, min_length=1, unique_items=True)
top_5_sub_categories_type = conlist(item_type=category_constr_type, min_length=1, max_length=5, unique_items=True)

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
        decimal_places=2,  # Max allowed decimals
        ge=-1,  # Greater than or equal to
        le=1  # Less than or equal to
    )

# ---CLASSES---
# Common classes to be imported and used elsewhere, combining the Fields defined above


class SentimentScore(BaseModel):
    sentiment_score = sentiment_score_field


class CategoryList(BaseModel):
    categories = categories_field


class CategoryListAndSentiment(BaseModel):
    categories = categories_field
    sentiment_score = sentiment_score_field


class TopCategoriesListAndSentiment(BaseModel):
    categories = top_5_sub_categories_field
    sentiment_score = sentiment_score_field


class CategoryListHierarchyAndSentiment(BaseModel):
    main_category = main_category_field
    sub_categories = top_5_sub_categories_field
    sentiment_score = sentiment_score_field
