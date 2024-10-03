import logging

# Disable logging for duration
logging.disable(logging.CRITICAL)


import unittest  # noqa: E402
from pydantic import ValidationError  # noqa: E402

import glyphdeck as gd  # noqa: E402


class TestValidators(unittest.TestCase):
    def test_sentiment_score(self):
        self.assertIsInstance(
            gd.validators.Sentiment(sentiment_score=0.5), gd.validators.Sentiment
        )
        with self.assertRaises(ValidationError):
            gd.validators.Sentiment(sentiment_score=1.5)
        with self.assertRaises(ValidationError):
            gd.validators.Sentiment(sentiment_score=0.555)
        with self.assertRaises(ValidationError):
            gd.validators.Sentiment(sentiment_score="not a float")

    def test_primary_category(self):
        self.assertIsInstance(
            gd.validators.PrimaryCat(primary_category="Test"), gd.validators.PrimaryCat
        )
        with self.assertRaises(ValidationError):
            gd.validators.PrimaryCat(primary_category=123)

    def test_top5_categories(self):
        self.assertIsInstance(
            gd.validators.Top5Cats(top_categories=["Test1", "Test2"]),
            gd.validators.Top5Cats,
        )
        with self.assertRaises(ValidationError):
            gd.validators.Top5Cats(
                top_categories=["str1", "str2", "str3", "str4", "str5", "str6"]
            )
        with self.assertRaises(ValidationError):
            gd.validators.Top5Cats(top_categories="not a list")

    def test_sub_categories(self):
        self.assertIsInstance(
            gd.validators.SubCats(sub_categories=["Test1"] * 10), gd.validators.SubCats
        )
        with self.assertRaises(ValidationError):
            gd.validators.SubCats(sub_categories=["T"] * 31)
        with self.assertRaises(ValidationError):
            gd.validators.SubCats(sub_categories=0.55)

    def test_primary_category_and_sentiment(self):
        self.assertIsInstance(
            gd.validators.PrimaryCatSentiment(
                primary_category="Test", sentiment_score=0.5
            ),
            gd.validators.PrimaryCatSentiment,
        )
        with self.assertRaises(ValidationError):
            gd.validators.PrimaryCatSentiment(
                primary_category="Test", sentiment_score=0.555
            )

    def test_sub_categories_and_sentiment(self):
        self.assertIsInstance(
            gd.validators.SubCatsSentiment(
                sub_categories=["Test1"] * 10, sentiment_score=0.5
            ),
            gd.validators.SubCatsSentiment,
        )
        with self.assertRaises(ValidationError):
            gd.validators.SubCatsSentiment(
                sub_categories=["T"] * 31, sentiment_score=0.5
            )
        with self.assertRaises(ValidationError):
            gd.validators.SubCatsSentiment(
                sub_categories=["T"] * 10, sentiment_score=-1.5
            )

    def test_top_categories_and_sentiment(self):
        self.assertIsInstance(
            gd.validators.TopCatsSentiment(
                top_categories=["Test1", "Test2"], sentiment_score=0.5
            ),
            gd.validators.TopCatsSentiment,
        )
        with self.assertRaises(ValidationError):
            gd.validators.TopCatsSentiment(
                top_categories=["T", "Test2"], sentiment_score=1.5
            )

    def test_category_hierarchy_and_sentiment(self):
        self.assertIsInstance(
            gd.validators.CatHierarchySentiment(
                primary_category="Test",
                sub_categories=["Test1"] * 10,
                sentiment_score=0.5,
            ),
            gd.validators.CatHierarchySentiment,
        )
        with self.assertRaises(ValidationError):
            gd.validators.CatHierarchySentiment(
                primary_category="T", sub_categories=["T"] * 31, sentiment_score=0.5
            )
        with self.assertRaises(ValidationError):
            gd.validators.CatHierarchySentiment(
                primary_category="T", sub_categories=["T"] * 10, sentiment_score=0.555
            )


if __name__ == "__main__":
    unittest.main()

# Re-enable logging
logging.disable(logging.NOTSET)
