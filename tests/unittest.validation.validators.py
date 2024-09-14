import logging

# Disable logging for duration
logging.disable(logging.CRITICAL)


import unittest  # noqa: E402
from pydantic import ValidationError  # noqa: E402

from CategoriGen.validation.validators import (  # noqa: E402
    SentimentScore,
    PrimaryCategory,
    Top5Categories,
    SubCategories,
    PrimaryCategoryAndSentiment,
    SubCategoriesAndSentiment,
    TopCategoriesAndSentiment,
    CategoryHierarchyAndSentiment,
)


class TestValidators(unittest.TestCase):
    def test_sentiment_score(self):
        self.assertIsInstance(SentimentScore(sentiment_score=0.5), SentimentScore)
        with self.assertRaises(ValidationError):
            SentimentScore(sentiment_score=1.5)
        with self.assertRaises(ValidationError):
            SentimentScore(sentiment_score=0.555)
        with self.assertRaises(ValidationError):
            SentimentScore(sentiment_score="not a float")

    def test_primary_category(self):
        self.assertIsInstance(PrimaryCategory(primary_category="Test"), PrimaryCategory)
        with self.assertRaises(ValidationError):
            PrimaryCategory(primary_category=123)

    def test_top5_categories(self):
        self.assertIsInstance(
            Top5Categories(top_categories=["Test1", "Test2"]), Top5Categories
        )
        with self.assertRaises(ValidationError):
            Top5Categories(top_categories=["str1", "str2", "str3", "str4", "str5", "str6"])
        with self.assertRaises(ValidationError):
            Top5Categories(top_categories="not a list")

    def test_sub_categories(self):
        self.assertIsInstance(
            SubCategories(sub_categories=["Test1"] * 10), SubCategories
        )
        with self.assertRaises(ValidationError):
            SubCategories(sub_categories=["T"] * 31)
        with self.assertRaises(ValidationError):
            SubCategories(sub_categories=0.55)

    def test_primary_category_and_sentiment(self):
        self.assertIsInstance(
            PrimaryCategoryAndSentiment(primary_category="Test", sentiment_score=0.5),
            PrimaryCategoryAndSentiment,
        )
        with self.assertRaises(ValidationError):
            PrimaryCategoryAndSentiment(primary_category="Test", sentiment_score=0.555)

    def test_sub_categories_and_sentiment(self):
        self.assertIsInstance(
            SubCategoriesAndSentiment(
                sub_categories=["Test1"] * 10, sentiment_score=0.5
            ),
            SubCategoriesAndSentiment,
        )
        with self.assertRaises(ValidationError):
            SubCategoriesAndSentiment(sub_categories=["T"] * 31, sentiment_score=0.5)
        with self.assertRaises(ValidationError):
            SubCategoriesAndSentiment(sub_categories=["T"] * 10, sentiment_score=-1.5)

    def test_top_categories_and_sentiment(self):
        self.assertIsInstance(
            TopCategoriesAndSentiment(
                top_categories=["Test1", "Test2"], sentiment_score=0.5
            ),
            TopCategoriesAndSentiment,
        )
        with self.assertRaises(ValidationError):
            TopCategoriesAndSentiment(
                top_categories=["T", "Test2"], sentiment_score=1.5
            )

    def test_category_hierarchy_and_sentiment(self):
        self.assertIsInstance(
            CategoryHierarchyAndSentiment(
                primary_category="Test",
                sub_categories=["Test1"] * 10,
                sentiment_score=0.5,
            ),
            CategoryHierarchyAndSentiment,
        )
        with self.assertRaises(ValidationError):
            CategoryHierarchyAndSentiment(
                primary_category="T", sub_categories=["T"] * 31, sentiment_score=0.5
            )
        with self.assertRaises(ValidationError):
            CategoryHierarchyAndSentiment(
                primary_category="T", sub_categories=["T"] * 10, sentiment_score=0.555
            )


if __name__ == "__main__":
    unittest.main()

# Re-enable logging
logging.disable(logging.NOTSET)
