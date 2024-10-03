from typing import Union, Tuple, List, Dict, Any, Self
import copy
import re


import pandas as pd

from glyphdeck.tools.logging_ import (
    SanitiserLogger,
    log_and_raise_error,
    log_decorator,
)
from glyphdeck.validation.data_types import DataDict

logger = SanitiserLogger().setup()


class Sanitiser:
    """Sanitises strings by replacing private information with placeholders.

    Attributes:
        email_regex: A regex pattern string for matching email addresses.
        email_pattern: A compiled regex pattern for matching email addresses.
        folder_path_regex: A regex pattern string for matching folder paths.
        folder_path_pattern: A compiled regex pattern for matching folder paths.
        file_path_regex: A regex pattern string for matching full file paths.
        file_path_pattern: A compiled regex pattern for matching full file paths.
        url_regex: A regex pattern string for matching URLs.
        url_pattern: A compiled regex pattern for matching URLs.
        date_regex1: A regex pattern string for matching dates in the form dd-mm-yyyy.
        date_pattern1: A compiled regex pattern for matching dates in the form dd-mm-yyyy.
        date_regex2: A regex pattern string for matching dates like 1 Jan 22 and variations.
        date_pattern2: A compiled regex pattern for matching dates like 1 Jan 22 and variations.
        date_regex3: A regex pattern string for matching dates like 1-mar-2022 and variations.
        date_pattern3: A compiled regex pattern for matching dates like 1-mar-2022 and variations.
        number_regex: A regex pattern string for matching words that contain one or more digits.
        number_pattern: A compiled regex pattern for matching words that contain one or more digits.
        PatternsDict: Typing alias for a dictionary of patterns.
        patterns: A dictionary containing regex patterns and their associated metadata.
        input_data: The data to be sanitized.
        output_data: A deepcopy of input_data which will be modified.
        overall_run_state: A boolean indicating if any sanitisation has been run.
        all_groups: A list of all group names from the patterns dictionary.
        active_groups: A list of active group names from the patterns dictionary.
        inactive_groups: A list of inactive group names from the patterns dictionary.
        group_matches: A dictionary recording the number of matches per group.
        total_matches: The total number of matches for all patterns.
    """

    # Takes a string and uses selected patterns to replace private information with placeholders.

    # Email addresses
    _email_regex: str = (
        r"(([\w-]+(?:\.[\w-]+)*)@((?:[\w-]+\.)*\w[\w-]{0,66})\."
        r"([a-z]{2,6}(?:\.[a-z]{2})?))(?![^<]*>)"
    )
    _email_pattern: re.Pattern[str] = re.compile(_email_regex)

    # Folder Paths
    # Gets any folder path, but doesn't work when the file name has a space
    _folder_path_regex: str = (
        r"(?:[a-zA-Z]:|\\\\[\w\.]+\\[\w.$]+)\\(?:[\s\w-]+\\)*([\w.-])*"
    )
    _folder_path_pattern: re.Pattern[str] = re.compile(_folder_path_regex)

    # Full File Paths
    # Anything starting with C:\ and ending in .filetype, works with file names that have spaces
    _file_path_regex: str = r"(?:[a-zA-Z]:|\\\\[\w\.]+\\[\w.$]+).*[\w](?=[.])[.\w]*"
    _file_path_pattern: re.Pattern[str] = re.compile(_file_path_regex)

    # URLs
    _url_regex: str = (
        r"(?i)\b((?:[a-z][\w-]+:(?:\/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)"
        r"(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]"
        r"+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
    )
    _url_pattern: re.Pattern[str] = re.compile(_url_regex)

    # Dates in the form dd-mm-yyyy
    _date_regex1: str = r"\d{2}[- /.]\d{2}[- /.]\d{,4}"
    _date_pattern1: re.Pattern[str] = re.compile(_date_regex1)
    # Dates like 1 Jan 22 and variations
    _date_regex2 = (
        r"(\d{1,2}[^\w]{,2}(january|february|march|april|may|june|july|august|september|october|november|december)"
        r"([- /.]{,2}(\d{4}|\d{2})){,1})(?P<n>\D)(?![^<]*>)"
    )
    _date_pattern2: re.Pattern[str] = re.compile(_date_regex2)
    # Dates like 1-mar-2022 and variations
    _date_regex3: str = (
        r"(\d{1,2}[^\w]{,2}(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"
        r"([- /.]{,2}(\d{4}|\d{2})){,1})(?P<n>\D)(?![^<]*>)"
    )
    _date_pattern3: re.Pattern[str] = re.compile(_date_regex3)

    # Lastly as a catch-all, replace all words that contain one or more digits
    # The 'word' can contain full stops and still be detected
    _number_regex: str = r"[.\w]*\d[.\w]*"
    _number_pattern: re.Pattern[str] = re.compile(_number_regex)

    # Storing that all as a dict
    PatternsDict = Dict[str, Dict[str, Union[str, float, re.Pattern[str]]]]
    patterns: PatternsDict = {
        "date1": {
            "group": "date",
            "placeholder": "<DATE>",
            "rank": 1,
            "pattern": _date_pattern1,
        },
        "date2": {
            "group": "date",
            "placeholder": "<DATE>",
            "rank": 2,
            "pattern": _date_pattern2,
        },
        "date3": {
            "group": "date",
            "placeholder": "<DATE>",
            "rank": 3,
            "pattern": _date_pattern3,
        },
        "email": {
            "group": "email",
            "placeholder": "<EMAIL>",
            "rank": 4,
            "pattern": _email_pattern,
        },
        "url": {
            "group": "url",
            "placeholder": "<URL>",
            "rank": 5,
            "pattern": _url_pattern,
        },
        "file_path": {
            "group": "path",
            "placeholder": "<PATH>",
            "rank": 6,
            "pattern": _file_path_pattern,
        },
        "folder_path": {
            "group": "path",
            "placeholder": "<PATH>",
            "rank": 7,
            "pattern": _folder_path_pattern,
        },
        "number": {
            "group": "number",
            "placeholder": "<NUM>",
            "rank": 8,
            "pattern": _number_pattern,
        },
    }

    # Add some keys with default values to each entry in the pattern dict to avoid repeating above
    # Setting all to active here is what makes all patterns on by default
    for key, values in patterns.items():
        values["active"] = True
        values["run_state"] = False
        values["matches"] = 0

    @staticmethod
    @log_decorator(logger, is_static_method=True)
    def _placeholder_check(patterns_dict: PatternsDict):
        """Checks that all placeholders in the patterns dictionary contain only allowed characters.

        Args:
            patterns_dict: A dictionary containing regex patterns and their associated metadata.

        Raises:
            TypeError: If any placeholder contains non-alphabet characters excluding '<' and '>'.
        """
        for key, value in patterns_dict.items():
            if not all(char.isalpha() or char in "<>" for char in value["placeholder"]):
                error_message = (
                    f"Placeholder {value['placeholder']}, "
                    f"in pattern group '{value['group']}', "
                    f"for pattern '{key}', "
                    f"contains non-alphabet characters. This is not allowed in placeholders."
                )
                log_and_raise_error(logger, "error", TypeError, error_message)

    # Run a check on the default values
    _placeholder_check(patterns)

    @staticmethod
    @log_decorator(logger, is_static_method=True)
    def _order_patterns(patterns_dict: PatternsDict) -> PatternsDict:
        """Re-sorts the pattern dictionary by rank.

        Args:
            patterns_dict: A dictionary containing regex patterns and their associated metadata.

        Returns:
            Dict: The sorted dictionary of patterns.
        """
        # Change the dict to a list with nested tuples and sort it by ascending rank
        sorted_items: List[Tuple[Any, Any]] = sorted(
            patterns_dict.items(), key=lambda item: item[1]["rank"]
        )
        # Turn the sorted data back into a dictionary and write to the instance
        patterns_dict: Dict[Any, Any] = dict(sorted_items)
        return patterns_dict

    # Run a sort on the default values in case they were not already ordered by rank
    patterns = _order_patterns(patterns)

    @staticmethod
    @log_decorator(logger, is_static_method=True)
    def _remove_arrows(input_string: str) -> str:
        """Removes angle brackets from a string.

        Args:
            input_string: The string from which to remove '<' and '>'.

        Returns:
            str: The string without angle brackets.
        """
        input_string = input_string.replace("<", "").replace(">", "")
        return input_string

    @staticmethod
    @log_decorator(logger, is_static_method=True)
    def _groups_where(
        patterns_dict: PatternsDict, active_type: Union[List, Tuple] = (True, False)
    ) -> List[str]:
        """Returns a list of pattern groups based on their 'active' status.

        Args:
            patterns_dict: A dictionary containing regex patterns and their associated metadata.
            active_type: A list or set defining the 'active' statuses to filter by. Defaults to (True, False).

        Returns:
            List[str]: A list of group names with the desired 'active' statuses.
        """
        groups: List = [
            value["group"]
            for key, value in patterns_dict.items()
            if value["active"] in active_type
        ]
        groups = list(set(groups))  # Remove duplicates
        return groups

    @log_decorator(logger, "info", suffix_message="Initialise Sanitiser object")
    def __init__(self, input_data: DataDict, pattern_groups: List = None) -> None:
        """Initializes a Sanitiser object with input data and optionally selected pattern groups.

        Args:
            input_data: The data to be sanitized.
            pattern_groups: A list of pattern groups to activate. Defaults to Non, which means all will be activated.

        Returns:
            None
        """
        self.input_data: DataDict = input_data
        # Will be changed by processes below
        self.output_data: DataDict = copy.deepcopy(input_data)
        self.overall_run_state = False
        self.all_groups: List = self._groups_where(self.patterns)
        # Sets the patterns dict only if selection was made
        if pattern_groups is not None:
            self.select_groups(pattern_groups)
        # Sets self.all_groups, self.active_groups & self.inactive_groups using patterns dict
        self._update_groups()
        self.group_matches: Dict[str, int] = {}
        self.total_matches: int = 0

    @log_decorator(logger)
    def _update_groups(self) -> Self:
        """Updates the lists of all, active, and inactive pattern groups based on the current patterns dictionary.

        Returns:
            Self: The updated instance of the Sanitiser class.
        """
        # Uses update_group() to update all group references
        # Storing all the available pattern groups in a distinct lists for reference
        # All groups
        self.all_groups: List = self._groups_where(self.patterns)
        # Active groups
        self.active_groups: List = self._groups_where(self.patterns, [True])
        # Inactive groups
        self.inactive_groups: List = self._groups_where(self.patterns, [False])

        return Self

    @log_decorator(logger, "off")  # Runs for every row, logs off by default
    def _update_match_counts(self) -> Self:
        """Updates the match count dictionary and the overall match count.

        Returns:
            Self: The updated instance of the Sanitiser class.
        """
        # Updates the per group match count dictionary and the overall count variable.
        # Based on the per regex counts in the 'patterns' dictionary.
        # Clear existing counts
        self.group_matches: Dict[str, int] = {}
        self.total_matches: int = 0
        # Prepare record_identifier for each active group with zero
        for group in self.active_groups:
            self.group_matches[group] = 0
        # Add each result to that record_identifier in the dictionary
        for key, value in self.patterns.items():
            if value["group"] in self.active_groups:
                self.group_matches[value["group"]] += value["matches"]
                self.total_matches += value["matches"]
        return self

    @log_decorator(logger)
    def set_placeholders(self, placeholder_dict: Dict[str, str]) -> Self:
        """Sets custom placeholders for the patterns.

        Args:
            placeholder_dict: A dictionary with group names as keys and custom placeholders as values.

        Returns:
            Self: The updated instance of the Sanitiser class.

        Raises:
            KeyError: If a provided key does not exist in the available patterns.
        """
        # Function to change the placeholders from their defaults
        # Check supplied patterns all exist
        for x in placeholder_dict:
            if x not in self.all_groups:
                log_and_raise_error(
                    logger,
                    "error",
                    KeyError,
                    f"Key {x} does not exist in available patterns: {self.all_groups}",
                )
        # Add the placeholders to the patterns dictionary
        for key, value in self.patterns.items():
            if value["group"] in placeholder_dict:
                value["placeholder"] = (
                    "<"
                    + self._remove_arrows(str(placeholder_dict[value["group"]]).upper())
                    + ">"
                )
        # Run a check on the new placeholders
        self._placeholder_check(self.patterns)
        return self

    @log_decorator(logger)
    def select_groups(self, pattern_groups: List[str]) -> Self:
        """Activates or deactivates pattern groups.

        Args:
            pattern_groups: A list of pattern groups to activate. All others are deactivated.

        Returns:
            Self: The updated instance of the Sanitiser class.

        Raises:
            KeyError: If a provided group does not exist in the available patterns.
        """
        # Function to select groups of patterns to run, updating the 'active' attribute in the instance.
        # Check that each pattern exists
        for x in pattern_groups:
            if x not in self.all_groups:
                log_and_raise_error(
                    logger,
                    "error",
                    KeyError,
                    f"Pattern {x} does not exist in available patterns: {self.all_groups}",
                )
        # Update 'active' in the patterns dict based on that list
        for key, value in self.patterns.items():
            if value["group"] in pattern_groups:
                value["active"] = True
            else:
                value["active"] = False
        # Update the reference lists for group selections
        self._update_groups()
        return self

    @log_decorator(logger)
    def _sort_patterns(self) -> Self:
        """Ensures patterns are sorted by their rank in ascending order.

        Returns:
            Self: The updated instance of the Sanitiser class.
        """
        # Just runs the static method 'order_patterns' on the instance, applying the result to the instance.
        # Making sure the patterns are run in order of rank regardless of other actions.
        self.patterns = self._order_patterns(self.patterns)
        return self

    @log_decorator(logger)
    def add_pattern(
        self, pattern_name: str, group: str, placeholder: str, rank: float, regex: str
    ):
        """Adds a new pattern to the sanitiser.

        Args:
            pattern_name: The unique name for the pattern.
            group: The group to which the new pattern belongs.
            placeholder: The placeholder to substitute matches with.
            rank: The rank indicating the order in which to process this pattern.
            regex: The regex string to compile and use for matching.

        Returns:
            None

        Raises:
            TypeError: If the placeholder contains invalid characters.
        """
        # Adds a new pattern to the 'patterns' dictionary, that will be run during the sanitise method
        # in addition to the existing patterns.
        # Build the inner dictionary
        new_pattern: Dict[str, Union[str, float, re.Pattern[str], bool]] = {
            "group": group,
            "placeholder": "<" + self._remove_arrows(str(placeholder).upper()) + ">",
            "rank": rank,
            "pattern": re.compile(regex),
            "active": True,
            "run_state": False,
            "matches": 0,
        }
        # Save the new pattern to the instance
        self.patterns[pattern_name] = new_pattern
        # Save the group to the 'all_groups' reference list, avoiding duplication
        self.all_groups = list(set(self.all_groups + [group]))
        # Post-processing
        self._placeholder_check(self.patterns)  # Check placeholders
        self._sort_patterns()  # Sort patterns by rank
        self._update_groups()  # Update selected groups lists

    @log_decorator(logger, "info", suffix_message="Sanitise data")
    def sanitise(self) -> Self:
        """Sanitises the input data using active patterns.

        Returns:
            Self: The updated instance of the Sanitiser class.
        """
        # Run all selected patterns in order, updating the 'raw_output_data'.
        # Run every selected regex pattern for every item, in every list, in every key, in the self.raw_output_data dict.
        # Successive regex patterns recursively act on the output of the previous regex,
        # in the order defined at the class level.
        self._placeholder_check(self.patterns)  # Check placeholders
        for pattern_key, pattern_dict in self.patterns.items():
            pattern_dict["matches"] = 0  # Resetting the per pattern count
            for data_key, data_list in self.output_data.items():
                for index, list_item in enumerate(data_list):
                    if pattern_dict["active"]:
                        if pd.notna(
                            self.output_data[data_key][index]
                        ):  # Check that the data is not NaN
                            result: Tuple[str, int] = re.subn(
                                pattern_dict["pattern"],
                                pattern_dict["placeholder"],
                                self.output_data[data_key][index],
                            )
                            self.output_data[data_key][index] = result[
                                0
                            ]  # Replacing the item in the output
                            pattern_dict["matches"] += result[1]  # Number of matches
                self.overall_run_state = True  # Shows regex has been run at least once
                self._update_match_counts()  # Updates match counts
        return self
