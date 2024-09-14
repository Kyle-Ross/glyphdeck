from pydantic import ValidationError
import pprint

from CategoriGen.validation.validators import (
    SentimentScore,
    PrimaryCategory,
    Top5Categories,
    SubCategories,
    PrimaryCategoryAndSentiment,
    SubCategoriesAndSentiment,
    TopCategoriesAndSentiment,
    CategoryHierarchyAndSentiment,
)


def test_validation(cls, args, should_pass):
    try:
        cls(
            **args
        )  # Initialising the validator class to test for unexpected and expected errors
    except ValidationError as error:  # All Validation errors
        if should_pass:
            print(f"FAILURE | Unexpected ValidationError for {cls.__name__} with args:")
            pprint.pprint(args, indent=4)
            print(f"Error Description:\n{error}")
        else:
            print(f"SUCCESS | Expected ValidationError for {cls.__name__} with args:")
            pprint.pprint(args, indent=4)
            print(f"Error Description:\n{error}")
    else:  # All validation non-errors (i.e the variable validated)
        if should_pass:
            print(f"SUCCESS | Expected Validation: {cls.__name__} with args:")
            pprint.pprint(args, indent=4)
        else:
            print(f"FAILURE | Unexpected Validation: {cls.__name__} with args:")
            pprint.pprint(args, indent=4)
    print("\n")


def print_sub_title(title, prefix=f'{"-" * 20}\n'):
    print(f"{prefix}\nCLASS | {title}\n")


print(f'{"-" * 30}\nVALIDATION TEST RESULTS\n{"-" * 30}\n')

# Now you can use this function to perform your tests
print("CLASS | SentimentScore\n")
test_validation(SentimentScore, {"sentiment_score": 0.5}, True)
test_validation(SentimentScore, {"sentiment_score": 1.5}, False)
test_validation(SentimentScore, {"sentiment_score": 0.555}, False)
test_validation(SentimentScore, {"sentiment_score": "not a float"}, False)

print_sub_title("PrimaryCategory")
test_validation(PrimaryCategory, {"primary_category": "Test"}, True)
test_validation(PrimaryCategory, {"primary_category": 123}, False)

print_sub_title("Top5Categories")
test_validation(Top5Categories, {"top_categories": ["Test1", "Test2"]}, True)
test_validation(Top5Categories, {"top_categories": "not a list"}, False)

print_sub_title("SubCategories")
test_validation(SubCategories, {"sub_categories": ["Test1"] * 10}, True)
test_validation(SubCategories, {"sub_categories": ["T"] * 31}, False)
test_validation(SubCategories, {"sub_categories": 0.55}, False)

print_sub_title("PrimaryCategoryAndSentiment")
test_validation(
    PrimaryCategoryAndSentiment,
    {"primary_category": "Test", "sentiment_score": 0.5},
    True,
)
test_validation(
    PrimaryCategoryAndSentiment,
    {"primary_category": "Test", "sentiment_score": 0.555},
    False,
)

print_sub_title("SubCategoriesAndSentiment")
test_validation(
    SubCategoriesAndSentiment,
    {"sub_categories": ["Test1"] * 10, "sentiment_score": 0.5},
    True,
)
test_validation(
    SubCategoriesAndSentiment,
    {"sub_categories": ["T"] * 31, "sentiment_score": 0.5},
    False,
)
test_validation(
    SubCategoriesAndSentiment,
    {"sub_categories": ["T"] * 10, "sentiment_score": -1.5},
    False,
)

print_sub_title("TopCategoriesAndSentiment")
test_validation(
    TopCategoriesAndSentiment,
    {"top_categories": ["Test1", "Test2"], "sentiment_score": 0.5},
    True,
)
test_validation(
    TopCategoriesAndSentiment,
    {"top_categories": ["T", "Test2"], "sentiment_score": 1.5},
    False,
)

print_sub_title("CategoryHierarchyAndSentiment")
test_validation(
    CategoryHierarchyAndSentiment,
    {
        "primary_category": "Test",
        "sub_categories": ["Test1"] * 10,
        "sentiment_score": 0.5,
    },
    True,
)
test_validation(
    CategoryHierarchyAndSentiment,
    {"primary_category": "T", "sub_categories": ["T"] * 31, "sentiment_score": 0.5},
    False,
)
test_validation(
    CategoryHierarchyAndSentiment,
    {
        "primary_category": "T",
        "sub_categories": ["T"] * 10,
        "sentiment_score": 0.555,
    },
    False,
)
