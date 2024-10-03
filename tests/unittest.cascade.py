import logging

# Disable logging during the test
logging.disable(logging.CRITICAL)

from datetime import datetime, timedelta  # noqa: E402

import unittest  # noqa: E402
import pandas as pd  # noqa: E402

import glyphdeck as gd  # noqa: E402


class TestCascade(unittest.TestCase):
    def setUp(self):
        # Set up test data
        self.test_data = {
            1: ["potato", "steak", "party"],
            2: ["carrot", "party", "alpha"],
            3: ["carrot", "party", "alpha"],
        }

        # Create the test DataFrame
        test_df = pd.DataFrame.from_dict(self.test_data, orient="index")
        test_df = test_df.reset_index()  # Adds the index as a column
        test_df.columns = ["Word ID", "Word1", "Word2", "Word3"]  # Rename cols

        # Initialise,
        # Creates a blank initialisation (record key 0), then appends the initial record (record key 1)
        self.cascade = gd.Cascade(
            test_df,  # csv and xlsx inputs are tested elsewhere
            "Word ID",
            ["Word1", "Word2", "Word3"],
        )

        # Append two more records
        # (record key 2)
        # The table, id and data_columns only needs to be added in the first step, but can be included again to update them
        # Otherwise each record will refer to the last time each was defined, which is the original record until set otherwise
        self.cascade.append(
            title="Example1",
            data={
                1: ["potato", "steak", "party"],
                2: ["carrot", "party", "alpha"],
                3: ["carrot", "party", "alpha"],
            },
        )
        # (record key 2)
        # Becomes the 'latest'
        self.cascade.append(
            title="Example2",
            data={
                1: ["potatoes", "carrot", "gary"],
                2: ["carrots", "pizza", "pasta"],
                3: ["bananas", "beast", "jeffery"],
            },
        )

    def test_delta(self):
        self.assertIsInstance(self.cascade.delta, timedelta)

    def test_set_expected(self):
        self.cascade.set_expected_len(5)
        self.assertEqual(self.cascade.expected_len, 5)

    def test_good_append(self):
        self.cascade.append(
            title="Example3",
            data={
                1: ["wiggle", "carrot", "gary"],
                2: ["carrots", "pizza", "pasta"],
                3: ["bananas", "beast", "jeffery"],
            },
        )
        self.assertEqual(self.cascade.latest_key, 4)
        self.assertEqual(self.cascade.latest_title, "Example3")

    def test_latest_key(self):
        self.assertEqual(self.cascade.latest_key, 3)

    def test_latest_record(self):
        self.assertEqual(self.cascade.latest_record, self.cascade.records[3])

    def test_latest_title(self):
        self.assertEqual(self.cascade.latest_title, "Example2")

    def test_latest_dt(self):
        self.assertIsInstance(self.cascade.latest_dt, datetime)

    def test_latest_data(self):
        self.assertEqual(self.cascade.latest_data, self.cascade.records[3]["data"])

    def test_latest_df(self):
        self.assertTrue(self.cascade.latest_df.equals(self.cascade.df(3)))

    def test_latest_column_names(self):
        self.assertEqual(
            self.cascade.latest_column_names, self.cascade.records[2]["column_names"]
        )

    def test_sanitiser_data(self):
        self.assertEqual(self.cascade.latest_data, self.cascade.sanitiser.input_data)

    def test_missing_value_append(self):
        with self.assertRaises(ValueError):
            self.cascade.append(
                title="Missing Value",
                data={
                    1: ["potato", "steak", "party"],
                    2: ["carrot", "party", "alpha"],
                    3: ["carrot", "party"],  # With missing value
                },
            )

    def test_new_key_append(self):
        with self.assertRaises(KeyError):
            self.cascade.append(
                title="New Key",
                data={
                    1: ["potato", "steak", "party"],
                    2: ["carrot", "party", "alpha"],
                    3: ["carrot", "party", "jeffery"],
                    4: ["jumanji", "shark", "dog"],  # New key
                },
            )

    def test_missing_key_append(self):
        with self.assertRaises(KeyError):
            self.cascade.append(
                title="New Key",
                data={
                    1: ["potato", "steak", "party"],
                    2: ["carrot", "party", "alpha"],
                    # Missing key
                },
            )

    def test_create_dataframes(self):
        examples_titles = ["Example1", "Example2"]
        examples_keys = [self.cascade.title_key(x) for x in examples_titles]
        count_of_examples = len(examples_keys)
        self.cascade.create_dataframes(examples_keys, use_suffix=False, recreate=True)
        counter = 0
        for k, v in self.cascade.records.items():
            if k in examples_keys:
                if "df" in v:
                    counter += 1
        self.assertEqual(count_of_examples, counter)

    def test_combiner(self):
        example1 = "Example1"
        example2 = "Example2"
        example1_df = self.cascade.df(example1)
        example2_df = self.cascade.df(example2)
        combined_df = self.cascade.get_combined([example1, example2])
        example1_col_count = example1_df.shape[1]
        example2_col_count = example2_df.shape[1]
        combined_col_count = combined_df.shape[1]
        combined_cols_count_eg1_suffixes = len(
            [col for col in combined_df.columns if example1 in col]
        )
        combined_cols_count_eg2_suffixes = len(
            [col for col in combined_df.columns if example2 in col]
        )
        self.assertEqual(combined_col_count, example1_col_count + example2_col_count)
        self.assertEqual(combined_cols_count_eg1_suffixes, example1_col_count)
        self.assertEqual(combined_cols_count_eg2_suffixes, example2_col_count)

    def test_output(self):
        # This test assumes that the output function correctly writes files to the specified directory
        self.cascade.write_output(
            file_type="csv",
            file_name_prefix="Test",
            record_identifiers=["Example1"],
            rebase=True,
            xlsx_use_sheets=False,
        )
        # Add assertions to check if the file was created and contains the expected data


if __name__ == "__main__":
    unittest.main()

# Re-enable logging
logging.disable(logging.NOTSET)
