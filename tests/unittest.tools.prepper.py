import logging

# Disable logging for duration
logging.disable(logging.CRITICAL)

import unittest  # noqa: E402

import pandas as pd  # noqa: E402

from CategoriGen.tools.prepper import prepare_df, prepare_xlsx, prepare_csv  # noqa: E402
from CategoriGen.validation.data_types import assert_and_log_type_is_data  # noqa: E402


class TestPrepper(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "data1": ["apple", "banana", "cherry", "date", "elderberry"],
                "data2": ["fig", "grape", "honeydew", "kiwi", "lemon"],
                "revenue1": [150.50, 200.75, 300.25, 450.10, 500.60],
                "revenue2": [250.30, 300.45, 400.60, 550.20, 600.70],
                "revenue3": [350.70, 400.85, 500.90, 650.30, 700.80],
            }
        )

    def test_prepare_df(self):
        df, data = prepare_df(self.df, "id", ["data1", "data2"])
        self.assertIsInstance(df, pd.DataFrame)
        assert_and_log_type_is_data(data, "data")

    def test_prepare_xlsx(self):
        df, data = prepare_xlsx(
            r"tests\testdata.clothingreviews.xlsx",
            "Row ID",
            ["Review Text"],
            sheet_name="Sheet1",
        )
        self.assertIsInstance(df, pd.DataFrame)
        assert_and_log_type_is_data(data, "data")

    def test_prepare_csv(self):
        df, data = prepare_csv(
            r"tests\testdata.clothingreviews.csv",
            "Row ID",
            "Review Text",
            encoding="ISO-8859-1",
        )
        self.assertIsInstance(df, pd.DataFrame)
        assert_and_log_type_is_data(data, "data")


if __name__ == "__main__":
    unittest.main()

# Re-enable logging
logging.disable(logging.NOTSET)
