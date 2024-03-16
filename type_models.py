from pydantic import BaseModel, Field, field_validator

# Pydantic Models, Types, Fields and Classes for import and use elsewhere in the program
# Used to assert and advise the expected output from provider calls

# Global variables which define the min and max characters allowed in various fields
char_min = 3
char_max = 30
sentiment_min = -1.00
sentiment_max = 1.00


# ---BASE MODELS---
# Add field names to the field_validator arguments if you want them to be validated by a method
class BaseValidatorModel(BaseModel):
    """Adds field validators to the resulting BaseValidatorModel class, which are used if columns match the arguments.
    Multiple validators can apply to a single field if the column name is in multiple validators."""

    @field_validator('sentiment_score', check_fields=False)  # Check fields uses since the item_model inherits from base
    def check_decimal_places(cls, v):
        if isinstance(v, float) or v in (-1, 0, 1):  # Allows some integers if inside the range
            assert round(v, 2) == v, 'value has more than 2 decimal places'
        return v

    @field_validator('sentiment_score', check_fields=False)
    def sentiment_float_in_range(cls, v):
        global sentiment_min, sentiment_max
        if isinstance(v, float) or v in (-1, 0, 1):
            minimum: float = sentiment_min
            maximum: float = sentiment_max
            assert minimum <= v <= maximum, f'sentiment float is not between {minimum} to {maximum}'
        return v

    @field_validator('per_sub_category_sentiment_scores', check_fields=False)
    def list_of_sentiment_floats_in_range(cls, v):
        global sentiment_min, sentiment_max
        if isinstance(v, list):
            for x in v:
                assert type(x) == float or x in (-1, 0, 1), f'{x} is not a float'
                assert round(x, 2) == x, f'value {x} has more than 2 decimal places'
                # Assert sentiment is in the allowed range as assigned in the global variables
                minimum: float = sentiment_min
                maximum: float = sentiment_max
                assert minimum <= x <= maximum, f'sentiment float {x} is not between {minimum} to {maximum}'
        return v

    @field_validator('top_categories', 'top_categories_with_char_limit', check_fields=False)
    def list_1_to_5(cls, v):
        if isinstance(v, list):
            minimum: int = 1
            maximum: int = 5
            assert minimum <= len(v) <= maximum, f'list does not contain between {minimum} to {maximum} entries'
        return v

    @field_validator('sub_categories', 'sub_categories_with_char_limit', check_fields=False)
    def list_1_to_30(cls, v):
        if isinstance(v, list):
            minimum: int = 1
            maximum: int = 30
            assert minimum <= len(v) <= maximum, f'list does not contain between {minimum} to {maximum} entries'
        return v

    @field_validator('primary_category_with_char_limit', check_fields=False)
    def string_with_char_limit(cls, v):
        global char_min, char_max
        if isinstance(v, str):
            minimum: int = char_min
            maximum: int = char_max
            assert minimum <= len(v) <= maximum, f'string is not between {minimum} to {maximum} characters'
        return v

    @field_validator('top_categories_with_char_limit', 'sub_categories_with_char_limit', check_fields=False)
    def string_with_char_limit_in_list(cls, v):
        global char_min, char_max
        if isinstance(v, list):
            for x in v:
                assert type(x) == str, f'{x} is not a string'
                minimum: int = char_min
                maximum: int = char_max
                assert minimum <= len(x) <= maximum, f'string "{x}" is not between {minimum} to {maximum} characters'
        return v


# ---FIELDS---
# Field() can customise the pydantic JSON schema
# Used here to add a description to each field
# Defined separately to avoid repetition in similar classes below
sentiment_score: float = \
    Field(
        description="A 2 decimal value that represents the overall sentiment of a comment. Ranges from -1.00 "
                    "(max negative sentiment) to 1.00 (max positive sentiment), with 0.00 indicating neutral "
                    "sentiment. It must be between -1.00 and 1.00"
    )

per_sub_category_sentiment_scores: list = \
    Field(
        description="A list of sentiment scores corresponding to the list of identified sub-categories. Each score is "
                    "a 2 decimal value that represents sentiment of the corresponding sub-categories as it was used in "
                    "the comment. Each score ranges from -1.00 (max negative sentiment) to 1.00 (max positive "
                    "sentiment), with 0.00 indicating neutral sentiment. It must be between -1.00 and 1.00. The list "
                    "should be of equal length to the list of corresponding sub-categories, and in the same order."
    )

primary_category: str = Field(
    description=f"The primary category identified in the comment. Each category name should be concise."
)

top_5_categories: list = Field(
    description=f"The top 1 to 5 sub-categories identified in the comment in order of relevance. "
                f"Each category name should be concise."
)

categories_1_to_30: list = Field(
    description=f"All sub-categories identified in the comment in order of relevance, making sure to capture all the "
                f"topics, with least 1 and no more than 30 categories. Each category name should be concise."
)


primary_category_with_char_limit: str = Field(
    description=f"The primary category identified in the comment. Each category name should be concise and be "
                f"between {char_min} - {char_max} characters long"
)

top_5_categories_with_char_limit: list = Field(
    description=f"The top 1 to 5 sub-categories identified in the comment in order of relevance. Each category name "
                f"should be concise and be between {char_min} - {char_max} characters long"
)

categories_1_to_30_with_char_limit: list = Field(
    description=f"All sub-categories identified in the comment in order of relevance, making sure to capture all the "
                f"topics, with least 1 and no more than 30 categories. Each category name "
                f"should be concise and be between {char_min} - {char_max} characters long"
)

# Additional information on the fields above
# Cannot sit in the classes or fields due to the way they are accessed by Pydantic, and would increase prompt size
# Access this later by using the '__name__' attribute of the data classes
field_schema: dict = {
    'sentiment_score': {'structure': 'single_value', 'type': 'float'},
    'primary_category': {'structure': 'single_value', 'type': 'str'},
    'top_categories': {'structure': 'list', 'type': 'str'},
    'sub_categories': {'structure': 'list', 'type': 'str'},
    'primary_category_with_char_limit': {'structure': 'single_value', 'type': 'str'},
    'top_categories_with_char_limit': {'structure': 'list', 'type': 'str'},
    'sub_categories_with_char_limit': {'structure': 'list', 'type': 'str'}
}


# ---CLASSES---
# Classes use the common fields above
# Validated according to @field_validator methods defined in the BaseValidatorModel if the field name matches
# Type annotations need to be repeated, but use the above as a reference.
# Pydantic excludes attributes that start with an underscore from validation actions

# ---Sentiment Classes---
class SentimentScore(BaseValidatorModel):
    _field_count: int = 1
    sentiment_score: float = sentiment_score


# ---Categorising Classes---
class PrimaryCategory(BaseValidatorModel):
    _field_count: int = 1
    primary_category: str = primary_category


class Top5Categories(BaseValidatorModel):
    _field_count: int = 1
    top_categories: list = top_5_categories


class SubCategories(BaseValidatorModel):
    _field_count: int = 1
    sub_categories: list = categories_1_to_30


class PrimaryCategoryAndSentiment(BaseValidatorModel):
    _field_count: int = 2
    primary_category: str = primary_category
    sentiment_score: float = sentiment_score


class PrimaryCategoryAndSubCategory(BaseValidatorModel):
    _field_count: int = 2
    primary_category: str = primary_category
    sub_categories: list = categories_1_to_30


class SubCategoriesAndSentiment(BaseValidatorModel):
    _field_count: int = 2
    sub_categories: list = categories_1_to_30
    sentiment_score: float = sentiment_score


class SubCategoriesWithPerItemSentiment(BaseValidatorModel):
    _field_count: int = 2
    sub_categories: list = categories_1_to_30
    per_sub_category_sentiment_scores: list = per_sub_category_sentiment_scores


class SubCategoriesWithPerItemSentimentAndOverallSentiment(BaseValidatorModel):
    _field_count: int = 3
    sub_categories: list = categories_1_to_30
    per_sub_category_sentiment_scores: list = per_sub_category_sentiment_scores
    sentiment_score: float = sentiment_score


class TopCategoriesAndSentiment(BaseValidatorModel):
    _field_count: int = 2
    top_categories: list = top_5_categories
    sentiment_score: float = sentiment_score


class CategoryHierarchyAndSentiment(BaseValidatorModel):
    _field_count: int = 3
    primary_category: str = primary_category
    sub_categories: list = categories_1_to_30
    sentiment_score: float = sentiment_score


# ---Categorising Classes with Character limiting---

class PrimaryCategoryWithCharLimit(BaseValidatorModel):
    _field_count: int = 1
    primary_category_with_char_limit: str = primary_category_with_char_limit


class Top5CategoriesWithCharLimit(BaseValidatorModel):
    _field_count: int = 1
    top_categories_with_char_limit: list = top_5_categories_with_char_limit


class SubCategoriesWithCharLimit(BaseValidatorModel):
    _field_count: int = 1
    sub_categories_with_char_limit: list = categories_1_to_30_with_char_limit


class PrimaryCategoryWithCharLimitAndSentiment(BaseValidatorModel):
    _field_count: int = 2
    primary_category_with_char_limit: str = primary_category_with_char_limit
    sentiment_score: float = sentiment_score


class PrimaryCategoryAndSubCategoryWithCharLimit(BaseValidatorModel):
    _field_count: int = 2
    primary_category_with_char_limit: str = primary_category_with_char_limit
    sub_categories_with_char_limit: list = categories_1_to_30_with_char_limit


class SubCategoriesWithCharLimitAndSentiment(BaseValidatorModel):
    _field_count: int = 2
    sub_categories_with_char_limit: list = categories_1_to_30_with_char_limit
    sentiment_score: float = sentiment_score


class TopCategoriesWithCharLimitAndSentiment(BaseValidatorModel):
    _field_count: int = 2
    top_categories_with_char_limit: list = top_5_categories_with_char_limit
    sentiment_score: float = sentiment_score


class CategoryHierarchyWithCharLimitAndSentiment(BaseValidatorModel):
    _field_count: int = 3
    primary_category_with_char_limit: str = primary_category_with_char_limit
    sub_categories_with_char_limit: list = categories_1_to_30_with_char_limit
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
        else:  # All validation non-errors (i.e the variable validated)
            if should_pass:
                print(f"SUCCESS | Expected Validation: {cls.__name__} with args:")
                pprint.pprint(args, indent=4)
            else:
                print(f"FAILURE | Unexpected Validation: {cls.__name__} with args:")
                pprint.pprint(args, indent=4)
        print('\n')


    def print_sub_title(title, prefix=f'{"-" * 20}\n'):
        print(f'{prefix}\nCLASS | {title}\n')


    really_long_string = "A really long string that is over 20 characters"

    print(f'{"-" * 30}\nVALIDATION TEST RESULTS\n{"-" * 30}\n')

    # Now you can use this function to perform your tests
    print('CLASS | SentimentScore\n')
    test_validation(SentimentScore, {'sentiment_score': 0.5}, True)
    test_validation(SentimentScore, {'sentiment_score': 1.5}, False)
    test_validation(SentimentScore, {'sentiment_score': 0.555}, False)
    test_validation(SentimentScore, {'sentiment_score': 'not a float'}, False)

    print_sub_title('PrimaryCategoryWithCharLimit')
    test_validation(PrimaryCategoryWithCharLimit, {'primary_category_with_char_limit': "Test"}, True)
    test_validation(PrimaryCategoryWithCharLimit, {'primary_category_with_char_limit': "T"}, False)
    test_validation(PrimaryCategoryWithCharLimit, {'primary_category_with_char_limit': really_long_string}, False)
    test_validation(PrimaryCategoryWithCharLimit, {'primary_category_with_char_limit': 123}, False)

    print_sub_title('Top5CategoriesWithCharLimit')
    test_validation(Top5CategoriesWithCharLimit, {'top_categories': ["Test1", "Test2"]}, True)
    test_validation(Top5CategoriesWithCharLimit, {'top_categories': ["T", "Test2"]}, False)
    test_validation(Top5CategoriesWithCharLimit, {'top_categories': ["Test1", "Test2", really_long_string]}, False)
    test_validation(Top5CategoriesWithCharLimit, {'top_categories': 'not a list'}, False)

    print_sub_title('SubCategoriesWithCharLimit')
    test_validation(SubCategoriesWithCharLimit, {'sub_categories': ["Test1"] * 10}, True)
    test_validation(SubCategoriesWithCharLimit, {'sub_categories': ["T"] * 31}, False)
    test_validation(SubCategoriesWithCharLimit, {'sub_categories': ["Test1"] * 10 + [really_long_string]}, False)
    test_validation(SubCategoriesWithCharLimit, {'sub_categories': 0.55}, False)

    print_sub_title('PrimaryCategoryWithCharLimitAndSentiment')
    test_validation(PrimaryCategoryWithCharLimitAndSentiment,
                    {'primary_category_with_char_limit': "Test", 'sentiment_score': 0.5}, True)
    test_validation(PrimaryCategoryWithCharLimitAndSentiment,
                    {'primary_category_with_char_limit': "T", 'sentiment_score': 0.5}, False)
    test_validation(PrimaryCategoryWithCharLimitAndSentiment,
                    {'primary_category_with_char_limit': "Test", 'sentiment_score': 0.555}, False)
    test_validation(PrimaryCategoryWithCharLimitAndSentiment, {'primary_category_with_char_limit': really_long_string,
                                                               'sentiment_score': 0.5}, False)

    print_sub_title('SubCategoriesWithCharLimitAndSentiment')
    test_validation(SubCategoriesWithCharLimitAndSentiment, {'sub_categories': ["Test1"] * 10, 'sentiment_score': 0.5},
                    True)
    test_validation(SubCategoriesWithCharLimitAndSentiment, {'sub_categories': ["T"] * 31, 'sentiment_score': 0.5},
                    False)
    test_validation(SubCategoriesWithCharLimitAndSentiment, {'sub_categories': ["T"] * 10, 'sentiment_score': -1.5},
                    False)
    test_validation(SubCategoriesWithCharLimitAndSentiment, {'sub_categories': ["Test1"] * 10 + [really_long_string],
                                                             'sentiment_score': 0.5}, False)

    print_sub_title('TopCategoriesWithCharLimitAndSentiment')
    test_validation(TopCategoriesWithCharLimitAndSentiment,
                    {'top_categories': ["Test1", "Test2"], 'sentiment_score': 0.5}, True)
    test_validation(TopCategoriesWithCharLimitAndSentiment, {'top_categories': ["T", "Test2"], 'sentiment_score': 1.5},
                    False)
    test_validation(TopCategoriesWithCharLimitAndSentiment, {'top_categories': ["Test1", "Test2", really_long_string],
                                                             'sentiment_score': 0.5}, False)

    print_sub_title('CategoryHierarchyWithCharLimitAndSentiment')
    test_validation(CategoryHierarchyWithCharLimitAndSentiment,
                    {'primary_category_with_char_limit': "Test", 'sub_categories': ["Test1"] * 10,
                     'sentiment_score': 0.5}, True)
    test_validation(CategoryHierarchyWithCharLimitAndSentiment,
                    {'primary_category_with_char_limit': "T", 'sub_categories': ["T"] * 31, 'sentiment_score': 0.5},
                    False)
    test_validation(CategoryHierarchyWithCharLimitAndSentiment,
                    {'primary_category_with_char_limit': "T", 'sub_categories': ["T"] * 10, 'sentiment_score': 0.555},
                    False)
    test_validation(CategoryHierarchyWithCharLimitAndSentiment,
                    {'primary_category_with_char_limit': "Test",
                     'sub_categories': ["Test1"] * 10 + [really_long_string],
                     'sentiment_score': 0.5}, False)
