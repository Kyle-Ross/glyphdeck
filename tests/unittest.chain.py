import logging

# Disable logging during the test
logging.disable(logging.CRITICAL)

from datetime import datetime, timedelta  # noqa: E402

import unittest  # noqa: E402
import pandas as pd  # noqa: E402

from CategoriGen.processors.chain import Chain  # noqa: E402


class TestChain(unittest.TestCase):
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
        self.chain = Chain(
            test_df,  # csv and xlsx inputs are tested elsewhere
            "Word ID",
            ["Word1", "Word2", "Word3"],
        )

        # Append two more records
        # (record key 2)
        # The table, id and data_columns only needs to be added in the first step, but can be included again to update them
        # Otherwise each record will refer to the last time each was defined, which is the original record until set otherwise
        self.chain.append(
            title="Example1",
            data={
                1: ["potato", "steak", "party"],
                2: ["carrot", "party", "alpha"],
                3: ["carrot", "party", "alpha"],
            },
        )
        # (record key 2)
        # Becomes the 'latest'
        self.chain.append(
            title="Example2",
            data={
                1: ["potatoes", "carrot", "gary"],
                2: ["carrots", "pizza", "pasta"],
                3: ["bananas", "beast", "jeffery"],
            },
        )

    def test_delta(self):
        self.assertIsInstance(self.chain.delta, timedelta)

    def test_set_expected(self):
        self.chain.set_expected_len(5)
        self.assertEqual(self.chain.expected_len, 5)

    def test_good_append(self):
        self.chain.append(
            title="Example3",
            data={
                1: ["wiggle", "carrot", "gary"],
                2: ["carrots", "pizza", "pasta"],
                3: ["bananas", "beast", "jeffery"],
            },
        )
        self.assertEqual(self.chain.latest_key, 4)
        self.assertEqual(self.chain.latest_title, "Example3")

    def test_initial_key(self):
        self.assertEqual(self.chain.initial_key, 1)

    def test_initial_record(self):
        self.assertEqual(self.chain.initial_record, self.chain.records[1])

    def test_initial_title(self):
        self.assertEqual(self.chain.initial_title, "prepared")

    def test_initial_dt(self):
        self.assertIsInstance(self.chain.initial_dt, datetime)

    def test_initial_data(self):
        self.assertEqual(self.chain.initial_data, self.chain.records[1]["data"])

    def test_initial_table(self):
        self.assertTrue(self.chain.initial_table.equals(self.chain.records[1]["table"]))

    def test_initial_table_id_column(self):
        self.assertEqual(
            self.chain.initial_table_id_column, self.chain.records[1]["table_id_column"]
        )

    def test_initial_column_names(self):
        self.assertEqual(
            self.chain.initial_column_names, self.chain.records[1]["column_names"]
        )

    def test_latest_key(self):
        self.assertEqual(self.chain.latest_key, 3)

    def test_latest_record(self):
        self.assertEqual(self.chain.latest_record, self.chain.records[3])

    def test_latest_title(self):
        self.assertEqual(self.chain.latest_title, "Example2")

    def test_latest_dt(self):
        self.assertIsInstance(self.chain.latest_dt, datetime)

    def test_latest_data(self):
        self.assertEqual(self.chain.latest_data, self.chain.records[3]["data"])

    def test_latest_table(self):
        self.assertTrue(self.chain.latest_df.equals(self.chain.records[2]["table"]))

    def test_latest_table_id_column(self):
        self.assertEqual(
            self.chain.latest_table_id_column, self.chain.records[2]["table_id_column"]
        )

    def test_latest_column_names(self):
        self.assertEqual(
            self.chain.latest_column_names, self.chain.records[2]["column_names"]
        )

    def test_sanitiser_data(self):
        self.assertEqual(self.chain.latest_data, self.chain.sanitiser.input_data)

    def test_missing_value_append(self):
        with self.assertRaises(ValueError):
            self.chain.append(
                title="Missing Value",
                data={
                    1: ["potato", "steak", "party"],
                    2: ["carrot", "party", "alpha"],
                    3: ["carrot", "party"],  # With missing value
                },
            )

    def test_new_key_append(self):
        with self.assertRaises(KeyError):
            self.chain.append(
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
            self.chain.append(
                title="New Key",
                data={
                    1: ["potato", "steak", "party"],
                    2: ["carrot", "party", "alpha"],
                    # Missing key
                },
            )

    def test_selector(self):
        selected_records = self.chain.create_dataframes(["Example1"], use_suffix=False)
        self.assertEqual(len(selected_records), 1)
        self.assertEqual(selected_records[0]["title"], "Example1")

    def test_combiner(self):
        combined_records = self.chain.get_combined(["Example1"])
        self.assertEqual(len(combined_records), 1)
        self.assertEqual(combined_records[0]["title"], "combined")

    def test_output(self):
        # This test assumes that the output function correctly writes files to the specified directory
        self.chain.write_output(
            file_type="csv",
            file_name_prefix="Test",
            record_keys=["Example1"],
            rebase=True,
            xlsx_use_sheets=False,
        )
        # Add assertions to check if the file was created and contains the expected data


if __name__ == "__main__":
    unittest.main()

# Re-enable logging
logging.disable(logging.NOTSET)
