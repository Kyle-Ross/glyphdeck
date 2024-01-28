# TODO - In NLP processing section add retry loop on ValidationError, with prompt add to get ChatGPT to fix the output
# TODO - Review any rechecking methods available in langchain

from pydantic import BaseModel, Field, conset, constr, field_validator

# Pydantic Models, Types, Fields and Classes for import and use elsewhere in the program
# Used to assert and advise the expected output from provider calls


# ---BASE MODELS---
# Add field names to the field_validator arguments if you want them to be validated by a method
class BaseValidatorModel(BaseModel):
    """Adds field validators to the resulting BaseValidatorModel class, which are used if columns match the arguments.
    Multiple validators can apply to a single field if the column name is in multiple validators."""
    @field_validator('score', check_fields=False)  # Check fields uses since the item_model inherits from base
    def check_decimal_places(cls, v):
        if isinstance(v, float):
            assert round(v, 2) == v, 'value has more than 2 decimal places'
        return v

    @field_validator('score', check_fields=False)
    def float_in_range(cls, v):
        if isinstance(v, float):
            minimum: float = -1.00
            maximum: float = 1.00
            assert minimum <= v <= maximum, f'float is not between {minimum} to {maximum}'
        return v

    @field_validator('top_categories', check_fields=False)
    def list_1_to_5(cls, v):
        if isinstance(v, list):
            minimum: int = 1
            maximum: int = 5
            assert minimum <= len(v) <= maximum, f'list does not contain between {minimum} to {maximum} entries'
        return v

    @field_validator('sub_categories', check_fields=False)
    def list_1_to_30(cls, v):
        if isinstance(v, list):
            minimum: int = 1
            maximum: int = 30
            assert minimum <= len(v) <= maximum, f'list does not contain between {minimum} to {maximum} entries'
        return v

    @field_validator('primary_category', check_fields=False)
    def string_3_to_20(cls, v):
        if isinstance(v, str):
            minimum: int = 3
            maximum: int = 20
            assert minimum <= len(v) <= maximum, f'string is not between {minimum} to {maximum} characters'
        return v

    @field_validator('top_categories', 'sub_categories', check_fields=False)
    def strings_in_list_3_to_20(cls, v):
        if isinstance(v, list):
            for x in v:
                assert type(x) == str, f'{x} is not a string'
                minimum: int = 3
                maximum: int = 20
                assert minimum <= len(x) <= maximum, f'string is not between {minimum} to {maximum} characters'
        return v


# ---FIELDS---
# Field() can customise the pydantic JSON schema
# Used here to add a description to each field
# Defined separately to avoid repetition in similar classes below
primary_category: str = Field(
    description="The primary category identified in the comment"
)

top_5_categories: list = Field(
    description="The top 1 to 5 sub-categories identified in the comment in order of relevance"
)

categories_1_to_30: list = Field(
    description="sub-categories identified in the comment in order of relevance"
)

sentiment_score: float = \
    Field(
        description="Decimal value that represents sentiment of a comment. Ranges from -1 (negative sentiment) to 1 "
                    "(positive sentiment), with 0 indicating neutral sentiment"
    )

# ---CLASSES---
# Classes use the common fields above
# Validated according to @field_validator methods defined in the BaseValidatorModel if the field name matches
# Type annotations need to be repeated, but use the above as a reference.


class SentimentScore(BaseValidatorModel):
    sentiment_score: float = sentiment_score


class PrimaryCategory(BaseValidatorModel):
    primary_category: str = primary_category


class Top5Categories(BaseValidatorModel):
    top_categories: list = top_5_categories


class SubCategories(BaseValidatorModel):
    sub_categories: list = categories_1_to_30


class PrimaryCategoryAndSentiment(BaseValidatorModel):
    primary_category: str = primary_category
    sentiment_score: float = sentiment_score


class SubCategoriesAndSentiment(BaseValidatorModel):
    sub_categories: list = categories_1_to_30
    sentiment_score: float = sentiment_score


class TopCategoriesAndSentiment(BaseValidatorModel):
    top_categories: list = top_5_categories
    sentiment_score: float = sentiment_score


class CategoryHierarchyAndSentiment(BaseValidatorModel):
    primary_category: str = primary_category
    sub_categories: list = categories_1_to_30
    sentiment_score: float = sentiment_score


if __name__ == "__main__":
    """Only runs below if script is run directly, not on import, so this is for testing purposes"""
    from pydantic import ValidationError
    import pprint

    # ------------------------------------
    # Test cases written by ChatGPT
    # Examples of passes and failures for each
    # ------------------------------------

    def test_validation(cls, args, should_pass):
        try:
            instance = cls(**args)
        except ValidationError as error:  # All Validation errors
            if should_pass:
                print(f"FAILURE | Unexpected ValidationError for {cls.__name__} with args:")
                pprint.pprint(args, indent=4)
                print(f'Error Description:\n{error}')
            else:
                print(f"SUCCESS | Expected ValidationError for {cls.__name__} with args:")
                pprint.pprint(args, indent=4)
                print(f'Error Description:\n{error}')
        else:  # All validation non-errors (i.e the data validated)
            if should_pass:
                print(f"SUCCESS | Expected Validation: {cls.__name__} with args:")
                pprint.pprint(args, indent=4)
            else:
                print(f"FAILURE | Unexpected Validation: {cls.__name__} with args:")
                pprint.pprint(args, indent=4)
        print('\n')


    def print_sub_title(title, prefix=f'{"-" * 20}\n'):
        print(f'{prefix}\nCLASS | {title}\n')

    really_long_string = "A really long string that is over 20 characters ok"

    print(f'{"-"*30}\nVALIDATION TEST RESULTS\n{"-"*30}\n')

    # Now you can use this function to perform your tests
    print('CLASS | SentimentScore\n')
    test_validation(SentimentScore, {'sentiment_score': 0.5}, True)
    test_validation(SentimentScore, {'sentiment_score': 1.5}, False)
    test_validation(SentimentScore, {'sentiment_score': 0.555}, False)

    print_sub_title('PrimaryCategory')
    test_validation(PrimaryCategory, {'primary_category': "Test"}, True)
    test_validation(PrimaryCategory, {'primary_category': "T"}, False)
    test_validation(PrimaryCategory, {'primary_category': really_long_string}, False)

    print_sub_title('Top5Categories')
    test_validation(Top5Categories, {'top_categories': ["Test1", "Test2"]}, True)
    test_validation(Top5Categories, {'top_categories': ["T", "Test2"]}, False)
    test_validation(Top5Categories, {'top_categories': ["Test1", "Test2", really_long_string]}, False)

    print_sub_title('SubCategories')
    test_validation(SubCategories, {'sub_categories': ["Test1"] * 10}, True)
    test_validation(SubCategories, {'sub_categories': ["T"] * 31}, False)
    test_validation(SubCategories, {'sub_categories': ["Test1"] * 10 + [really_long_string]}, True)

    print_sub_title('PrimaryCategoryAndSentiment')
    test_validation(PrimaryCategoryAndSentiment, {'primary_category': "Test", 'sentiment_score': 0.5}, True)
    test_validation(PrimaryCategoryAndSentiment, {'primary_category': "T", 'sentiment_score': 1.5}, False)
    test_validation(PrimaryCategoryAndSentiment, {'primary_category': really_long_string, 'sentiment_score': 0.5}, True)

    print_sub_title('SubCategoriesAndSentiment')
    test_validation(SubCategoriesAndSentiment, {'sub_categories': ["Test1"] * 10, 'sentiment_score': 0.5}, True)
    test_validation(SubCategoriesAndSentiment, {'sub_categories': ["T"] * 31, 'sentiment_score': 1.5}, False)
    test_validation(SubCategoriesAndSentiment, {'sub_categories': ["Test1"] * 10 + [really_long_string],
                                                'sentiment_score': 0.5}, True)

    print_sub_title('TopCategoriesAndSentiment')
    test_validation(TopCategoriesAndSentiment, {'top_categories': ["Test1", "Test2"], 'sentiment_score': 0.5}, True)
    test_validation(TopCategoriesAndSentiment, {'top_categories': ["T", "Test2"], 'sentiment_score': 1.5}, False)
    test_validation(TopCategoriesAndSentiment, {'top_categories': ["Test1", "Test2", really_long_string],
                                                'sentiment_score': 0.5}, True)

    print_sub_title('CategoryHierarchyAndSentiment')
    test_validation(CategoryHierarchyAndSentiment,
                    {'primary_category': "Test", 'sub_categories': ["Test1"] * 10, 'sentiment_score': 0.5}, True)
    test_validation(CategoryHierarchyAndSentiment,
                    {'primary_category': "T", 'sub_categories': ["T"] * 31, 'sentiment_score': 1.5}, False)
    test_validation(CategoryHierarchyAndSentiment,
                    {'primary_category': "Test", 'sub_categories': ["Test1"] * 10 + [really_long_string],
                     'sentiment_score': 0.5}, True)
