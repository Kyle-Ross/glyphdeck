import logging

# Disable logging during the test
logging.disable(logging.CRITICAL)

import unittest  # noqa: E402
from glyphdeck.tools.logging_ import SanitiserLogger  # noqa: E402
from glyphdeck.processors.sanitiser import Sanitiser  # noqa: E402
from glyphdeck.validation.data_types import DataDict, assert_and_log_type_is_data  # noqa: E402

logger = SanitiserLogger().setup

# Example data with targets for removal
test_data: DataDict = {
    1: [
        r"Record One! - I like apple bottom jeans 156.a19878, 11/10/2020, jimbo@gmail.com",
        "My birthday is 11/10/2021",
        "Product info: https://t.co/KNkANrdypk \r\r\nTo order",
    ],
    2: [
        "Nothing wrong with this",
        "My email is jeff@babe.com, my ip address is 192.158.1.38",
        "Go to this website: www.website.com.au",
    ],
    3: [
        r"Big number is 1896987, I store my files in C:\Users\username\Documents\GitHub",
        "I like blue jeans, my card number is 2222 4053 4324 8877",
        r"I was born 15/12/1990, a file path is C:\Users\username\Pictures\BYG0Djh.png",
    ],
}


class TestSanitiser(unittest.TestCase):
    def setUp(self):
        self.data_example = test_data
        self.santiser_obj = Sanitiser(
            self.data_example, pattern_groups=["number", "date", "email"]
        )

    def test_initialisation(self):
        self.assertEqual(
            sorted(self.santiser_obj.active_groups), sorted(["number", "date", "email"])
        )

    def test_select_groups(self):
        self.santiser_obj.select_groups(["number", "date", "email", "path", "url"])
        self.assertEqual(
            sorted(self.santiser_obj.active_groups),
            sorted(["number", "date", "email", "path", "url"]),
        )

    def test_set_placeholders(self):
        self.santiser_obj.set_placeholders({"email": "EMAILS>>", "date": "<DA>TES>"})
        # Should clean up reserved characters
        self.assertEqual(
            self.santiser_obj.patterns["email"]["placeholder"],
            "<EMAILS>",
        )
        self.assertEqual(self.santiser_obj.patterns["date1"]["placeholder"], "<DATES>")

    def test_add_pattern(self):
        self.santiser_obj.add_pattern(
            pattern_name="custom",
            group="custom_group",
            placeholder="Cust",
            rank=0.5,
            regex=r"jeans",
        )
        self.assertIn("custom", self.santiser_obj.patterns)

    def test_sanitise(self):
        self.santiser_obj.sanitise()
        self.assertTrue(self.santiser_obj.overall_run_state)

    def test_output(self):
        self.santiser_obj.sanitise()
        assert_and_log_type_is_data(
            self.santiser_obj.output_data, "self.santiser_obj.output_data"
        )

    def test_email_sanitisation(self):
        self.santiser_obj.select_groups(["email"])
        self.santiser_obj.sanitise()
        self.assertNotIn("jimbo@gmail.com", self.santiser_obj.output_data[1][0])
        self.assertNotIn("jeff@babe.com", self.santiser_obj.output_data[2][1])

    def test_date_sanitisation(self):
        self.santiser_obj.select_groups(["date"])
        self.santiser_obj.sanitise()
        self.assertNotIn("11/10/2020", self.santiser_obj.output_data[1][0])
        self.assertNotIn("11/10/2021", self.santiser_obj.output_data[1][1])
        self.assertNotIn("15/12/1990", self.santiser_obj.output_data[3][2])

    def test_number_sanitisation(self):
        self.santiser_obj.select_groups(["number"])
        self.santiser_obj.sanitise()
        self.assertNotIn("156.a19878", self.santiser_obj.output_data[1][0])
        self.assertNotIn("1896987", self.santiser_obj.output_data[3][0])
        self.assertNotIn("2222 4053 4324 8877", self.santiser_obj.output_data[3][1])

    def test_url_sanitisation(self):
        self.santiser_obj.select_groups(["url"])
        self.santiser_obj.sanitise()
        self.assertNotIn("https://t.co/KNkANrdypk", self.santiser_obj.output_data[1][2])
        self.assertNotIn("www.website.com.au", self.santiser_obj.output_data[2][2])

    def test_path_sanitisation(self):
        self.santiser_obj.select_groups(["path"])
        self.santiser_obj.sanitise()
        self.assertNotIn(
            r"C:\Users\username\Documents\GitHub", self.santiser_obj.output_data[3][0]
        )
        self.assertNotIn(
            r"C:\Users\username\Pictures\BYG0Djh.png",
            self.santiser_obj.output_data[3][2],
        )


if __name__ == "__main__":
    unittest.main()

# Re-enable logging
logging.disable(logging.NOTSET)
