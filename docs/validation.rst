.validation
============================

.. automodule:: glyphdeck.validation.validators
   :members:
   :exclude-members: sentiment_min, sentiment_max,
                     Sentiment, PrimaryCat, Top5Cats, SubCats, 
                     PrimaryCatSentiment, PrimarySubCat, SubCatsSentiment,
                     SubCatsPerItemSentiment, SubCatsPerItemOverallSentiment,
                     TopCatsSentiment, CatHierarchySentiment

.. autoclass:: glyphdeck.validation.validators.Sentiment
   :members:
   :no-index: sentiment_score

.. autoclass:: glyphdeck.validation.validators.PrimaryCat
   :members:
   :no-index: primary_category

.. autoclass:: glyphdeck.validation.validators.Top5Cats
   :members:
   :no-index: top_categories

.. autoclass:: glyphdeck.validation.validators.SubCats
   :members:
   :no-index: sub_categories

.. autoclass:: glyphdeck.validation.validators.PrimaryCatSentiment
   :members:
   :no-index: primary_category, sentiment_score

.. autoclass:: glyphdeck.validation.validators.PrimarySubCat
   :members:
   :no-index: primary_category, sub_categories

.. autoclass:: glyphdeck.validation.validators.SubCatsSentiment
   :members:
   :no-index: sub_categories, sentiment_score

.. autoclass:: glyphdeck.validation.validators.SubCatsPerItemSentiment
   :members:
   :no-index: sub_categories, per_sub_category_sentiment_scores

.. autoclass:: glyphdeck.validation.validators.SubCatsPerItemOverallSentiment
   :members:
   :no-index: sub_categories, per_sub_category_sentiment_scores, 
              sentiment_score

.. autoclass:: glyphdeck.validation.validators.TopCatsSentiment
   :members:
   :no-index: top_categories, sentiment_score

.. autoclass:: glyphdeck.validation.validators.CatHierarchySentiment
   :members:
   :no-index: primary_category, sub_categories, sentiment_score