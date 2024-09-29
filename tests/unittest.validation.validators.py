import logging

# Disable logging for duration
logging.disable(logging.CRITICAL)


import unittest  # noqa: E402
from pydantic import ValidationError  # noqa: E402

from glyphdeck.validation.validators import (  # noqa: E402
    Sentiment,
    PrimaryCat,
    Top5Cats,
    SubCats,
    PrimaryCatSentiment,
    SubCatsSentiment,
    TopCatsSentiment,
    CatHierarchySentiment,
)


class TestValidators(unittest.TestCase):
    def test_sentiment_score(self):
        self.assertIsInstance(Sentiment(sentiment_score=0.5), Sentiment)
        with self.assertRaises(ValidationError):
            Sentiment(sentiment_score=1.5)
        with self.assertRaises(ValidationError):
            Sentiment(sentiment_score=0.555)
        with self.assertRaises(ValidationError):
            Sentiment(sentiment_score="not a float")

    def test_primary_category(self):
        self.assertIsInstance(PrimaryCat(primary_category="Test"), PrimaryCat)
        with self.assertRaises(ValidationError):
            PrimaryCat(primary_category=123)

    def test_top5_categories(self):
        self.assertIsInstance(Top5Cats(top_categories=["Test1", "Test2"]), Top5Cats)
        with self.assertRaises(ValidationError):
            Top5Cats(top_categories=["str1", "str2", "str3", "str4", "str5", "str6"])
        with self.assertRaises(ValidationError):
            Top5Cats(top_categories="not a list")

    def test_sub_categories(self):
        self.assertIsInstance(SubCats(sub_categories=["Test1"] * 10), SubCats)
        with self.assertRaises(ValidationError):
            SubCats(sub_categories=["T"] * 31)
        with self.assertRaises(ValidationError):
            SubCats(sub_categories=0.55)

    def test_primary_category_and_sentiment(self):
        self.assertIsInstance(
            PrimaryCatSentiment(primary_category="Test", sentiment_score=0.5),
            PrimaryCatSentiment,
        )
        with self.assertRaises(ValidationError):
            PrimaryCatSentiment(primary_category="Test", sentiment_score=0.555)

    def test_sub_categories_and_sentiment(self):
        self.assertIsInstance(
            SubCatsSentiment(sub_categories=["Test1"] * 10, sentiment_score=0.5),
            SubCatsSentiment,
        )
        with self.assertRaises(ValidationError):
            SubCatsSentiment(sub_categories=["T"] * 31, sentiment_score=0.5)
        with self.assertRaises(ValidationError):
            SubCatsSentiment(sub_categories=["T"] * 10, sentiment_score=-1.5)

    def test_top_categories_and_sentiment(self):
        self.assertIsInstance(
            TopCatsSentiment(top_categories=["Test1", "Test2"], sentiment_score=0.5),
            TopCatsSentiment,
        )
        with self.assertRaises(ValidationError):
            TopCatsSentiment(top_categories=["T", "Test2"], sentiment_score=1.5)

    def test_category_hierarchy_and_sentiment(self):
        self.assertIsInstance(
            CatHierarchySentiment(
                primary_category="Test",
                sub_categories=["Test1"] * 10,
                sentiment_score=0.5,
            ),
            CatHierarchySentiment,
        )
        with self.assertRaises(ValidationError):
            CatHierarchySentiment(
                primary_category="T", sub_categories=["T"] * 31, sentiment_score=0.5
            )
        with self.assertRaises(ValidationError):
            CatHierarchySentiment(
                primary_category="T", sub_categories=["T"] * 10, sentiment_score=0.555
            )


if __name__ == "__main__":
    unittest.main()

# Re-enable logging
logging.disable(logging.NOTSET)
