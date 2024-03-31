from typing import Union, Tuple, List, Dict, Any
from functions.logs import SanitiserLogger, log_and_raise_error
from data_types import Data
from icecream import ic
import pandas as pd
import re

logger = SanitiserLogger().setup()


class RegexSanitiser:
    """Takes a string and uses selected patterns to replace private information with placeholders."""
    # Email addresses
    email_regex: str = \
        r"(([\w-]+(?:\.[\w-]+)*)@((?:[\w-]+\.)*\w[\w-]{0,66})\." \
        r"([a-z]{2,6}(?:\.[a-z]{2})?))(?![^<]*>)"
    email_pattern: re.Pattern[str] = re.compile(email_regex)

    # Folder Paths
    # Gets any folder path, but doesn't work when the file name has a space
    folder_path_regex: str = r"(?:[a-zA-Z]:|\\\\[\w\.]+\\[\w.$]+)\\(?:[\s\w-]+\\)*([\w.-])*"
    folder_path_pattern: re.Pattern[str] = re.compile(folder_path_regex)

    # Full File Paths
    # Anything starting with C:\ and ending in .filetype, works with file names that have spaces
    file_path_regex: str = r"(?:[a-zA-Z]:|\\\\[\w\.]+\\[\w.$]+).*[\w](?=[.])[.\w]*"
    file_path_pattern: re.Pattern[str] = re.compile(file_path_regex)

    # URLs
    url_regex: str = r"(?i)\b((?:[a-z][\w-]+:(?:\/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)" \
                     r"(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]" \
                     r"+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
    url_pattern: re.Pattern[str] = re.compile(url_regex)

    # Dates in the form dd-mm-yyyy
    date_regex1: str = r"\d{2}[- /.]\d{2}[- /.]\d{,4}"
    date_pattern1: re.Pattern[str] = re.compile(date_regex1)
    # Dates like 1 Jan 22 and variations
    date_regex2 = \
        r"(\d{1,2}[^\w]{,2}(january|february|march|april|may|june|july|august|september|october|november|december)" \
        r"([- /.]{,2}(\d{4}|\d{2})){,1})(?P<n>\D)(?![^<]*>)"
    date_pattern2: re.Pattern[str] = re.compile(date_regex2)
    # Dates like 1-mar-2022 and variations
    date_regex3: str = \
        r"(\d{1,2}[^\w]{,2}(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)" \
        r"([- /.]{,2}(\d{4}|\d{2})){,1})(?P<n>\D)(?![^<]*>)"
    date_pattern3: re.Pattern[str] = re.compile(date_regex3)

    # Lastly as a catch-all, replace all words that contain one or more digits
    # The 'word' can contain full stops and still be detected
    number_regex: str = r'[.\w]*\d[.\w]*'
    number_pattern: re.Pattern[str] = re.compile(number_regex)

    # Storing that all as a dict
    patterns: Dict[str, Dict[str, Union[str, float, re.Pattern[str]]]] = {
        'date1': {
            'group': 'date',
            'placeholder': '<DATE>',
            'rank': 1,
            'pattern': date_pattern1
        },
        'date2': {
            'group': 'date',
            'placeholder': '<DATE>',
            'rank': 2,
            'pattern': date_pattern2
        },
        'date3': {
            'group': 'date',
            'placeholder': '<DATE>',
            'rank': 3,
            'pattern': date_pattern3
        },
        'email': {
            'group': 'email',
            'placeholder': '<EMAIL>',
            'rank': 4,
            'pattern': email_pattern
        },
        'url': {
            'group': 'url',
            'placeholder': '<URL>',
            'rank': 5,
            'pattern': url_pattern
        },
        'file_path': {
            'group': 'path',
            'placeholder': '<PATH>',
            'rank': 6,
            'pattern': file_path_pattern
        },
        'folder_path': {
            'group': 'path',
            'placeholder': '<PATH>',
            'rank': 7,
            'pattern': folder_path_pattern
        },
        'number': {
            'group': 'number',
            'placeholder': '<NUM>',
            'rank': 8,
            'pattern': number_pattern
        }
    }

    # Add some keys with default values to each entry in the pattern dict to avoid repeating above
    for key, values in patterns.items():
        values['active'] = True
        values['run_state'] = False
        values['matches'] = 0

    @staticmethod
    def placeholder_check(patterns_dict: 'patterns'):
        """Raises an error if any of the current placeholders contain non-alphabet characters excluding '<' and '>'."""
        for key, value in patterns_dict.items():
            if not all(char.isalpha() or char in '<>' for char in value['placeholder']):
                error_message = f"Placeholder {value['placeholder']}, "\
                                f"in pattern group '{value['group']}', "\
                                f"for pattern '{key}', "\
                                f"contains non-alphabet characters. This is not allowed in placeholders."
                log_and_raise_error(logger, 'error', TypeError, error_message)

    # Run a check on the default values
    placeholder_check(patterns)

    @staticmethod
    def order_patterns(patterns_dict: 'patterns'):
        """Re-sorts the pattern dictionary by rank, for use if you have added a new pattern to the patterns attribute"""
        # Change the dict to a list with nested tuples and sort it by ascending rank
        sorted_items: List[Tuple[Any, Any]] = sorted(patterns_dict.items(), key=lambda item: item[1]['rank'])
        # Turn the sorted data back into a dictionary and write to the instance
        patterns_dict: Dict[Any, Any] = dict(sorted_items)
        return patterns_dict

    # Run a sort on the default values in case they were not already ordered by rank
    patterns = order_patterns(patterns)

    @staticmethod
    def remove_arrows(input_string: str):
        """Removes '<' and '>' from a string"""
        input_string = input_string.replace('<', '').replace('>', '')
        return input_string

    @staticmethod
    def update_group(patterns_dict: 'patterns', active_type: Union[list, set] = (True, False)):
        """Returns a list of groups, with particular bool values in the 'active' record_identifier. Used to update
        reference attributes"""
        groups: List = [value['group'] for key, value in patterns_dict.items() if value['active'] in active_type]
        groups = list(set(groups))  # Remove duplicates
        return groups

    def __init__(self,
                 input_data: Data) -> None:
        self.input_data: Data = input_data
        self.output_data: Data = input_data  # Will be changed by processes below
        self.overall_run_state = False
        self.all_groups: List = self.update_group(self.patterns)
        self.active_groups: List = self.update_group(self.patterns, [True])
        self.inactive_groups: List = self.update_group(self.patterns, [False])
        self.group_matches: Dict[str, int] = {}
        self.total_matches: int = 0

    def update_groups(self):
        """Uses update_group() to update all group references"""
        # Storing all the available pattern groups in a distinct list for reference
        self.all_groups: List = self.update_group(self.patterns)
        self.active_groups: List = self.update_group(self.patterns, [True])
        self.inactive_groups: List = self.update_group(self.patterns, [False])

    def update_match_counts(self):
        """Updates the per group match count dictionary and the overall count variable.
        Based on the per regex counts in the 'patterns' dictionary."""
        # Clear existing counts
        self.group_matches: Dict[str, int] = {}
        self.total_matches: int = 0
        # Prepare record_identifier for each active group with zero
        for group in self.active_groups:
            self.group_matches[group] = 0
        # Add each result to that record_identifier in the dictionary
        for key, value in self.patterns.items():
            if value['group'] in self.active_groups:
                self.group_matches[value['group']] += value['matches']
                self.total_matches += value['matches']

    def set_placeholders(self, placeholder_dict: Dict[str, str]) -> 'RegexSanitiser':
        """Function to change the placeholders from their defaults
        Accepts a dict with {'group_name': 'placeholder',...}"""
        # Check supplied patterns all exist
        for x in placeholder_dict:
            if x not in self.all_groups:
                log_and_raise_error(logger, 'error', KeyError,
                                    f"Key {x} does not exist in available patterns: {self.all_groups}")
        # Add the placeholders to the patterns dictionary
        for key, value in self.patterns.items():
            if value['group'] in placeholder_dict:
                value['placeholder'] = \
                    '<' + self.remove_arrows(str(placeholder_dict[value['group']]).upper()) + '>'
        # Run a check on the new placeholders
        self.placeholder_check(self.patterns)
        return self

    def select_groups(self, selection: List) -> 'RegexSanitiser':
        """Function to select groups of patterns to run, updating the 'active' attribute in the instance."""
        # Check that each pattern exists
        for x in selection:
            if x not in self.all_groups:
                log_and_raise_error(logger, 'error', KeyError,
                                    f"Pattern {x} does not exist in available patterns: {self.all_groups}")
        # Update 'active' in the patterns dict based on that list
        for key, value in self.patterns.items():
            if value['group'] in selection:
                value['active'] = True
            else:
                value['active'] = False
        # Update the reference lists for group selections
        self.update_groups()
        return self

    def sort_patterns(self):
        """Just runs the static method 'order_patterns' on the instance, applying the result to the instance.
        Making sure the patterns are run in order of rank regardless of other actions."""
        self.patterns = self.order_patterns(self.patterns)

    def add_pattern(self,
                    pattern_name: str,
                    group: str,
                    placeholder: str,
                    rank: float,
                    regex: str):
        """Adds a new pattern to the 'patterns' dictionary, that will be run during the sanitise method
         in addition to the existing patterns."""
        # Build the inner dictionary
        new_pattern: Dict[str, Union[str, float, re.Pattern[str], bool]] = {
            'group': group,
            'placeholder': '<' + self.remove_arrows(str(placeholder).upper()) + '>',
            'rank': rank,
            'pattern': re.compile(regex),
            'active': True,
            'run_state': False,
            'matches': 0
        }
        # Save the new pattern to the instance
        self.patterns[pattern_name] = new_pattern
        # Save the group to the 'all_groups' reference list, avoiding duplication
        self.all_groups = list(set(self.all_groups + [group]))
        # Post-processing
        self.placeholder_check(self.patterns)  # Check placeholders
        self.sort_patterns()  # Sort patterns by rank
        self.update_groups()  # Update selected groups lists

    def sanitise(self):
        """Run all selected patterns in order, updating the 'raw_output_data'.

        Run every selected regex pattern for every item, in every list, in every key, in the self.raw_output_data dict.

        Successive regex patterns recursively act on the output of the previous regex,
        in the order defined at the class level.
        """
        self.placeholder_check(self.patterns)  # Check placeholders
        for pattern_key, pattern_dict in self.patterns.items():
            pattern_dict['matches'] = 0  # Resetting the per pattern count
            for data_key, data_list in self.output_data.items():
                for index, list_item in enumerate(data_list):
                    if pattern_dict['active']:
                        if pd.notna(self.output_data[data_key][index]):  # Check that the data is not NaN
                            result: Tuple[str, int] = re.subn(pattern_dict['pattern'],
                                                              pattern_dict['placeholder'],
                                                              self.output_data[data_key][index])
                            self.output_data[data_key][index] = result[0]  # Replacing the item in the output
                            pattern_dict['matches'] += result[1]  # Number of matches
                self.overall_run_state = True  # Shows regex has been run at least once
                self.update_match_counts()  # Updates match counts
        return self


if __name__ == "__main__":
    """Only runs below if script is run directly, not on import, so this is for testing purposes"""
    data_example: Data = \
        {
            1: [
                r"I like apple bottom jeans 156.a19878, 11/10/2020, jimbo@gmail.com",
                'My birthday is 11/10/2021',
                'Product info: https://t.co/KNkANrdypk \r\r\nTo order'],
            2: [
                'Nothing wrong with this',
                'My email is jeff@babe.com, my ip address is 192.158.1.38',
                'Go to this website: www.website.com.au'],
            3: [
                r'Big number is 1896987, I store my files in C:\Users\username\Documents\GitHub',
                'I like blue jeans, my card number is 2222 4053 4324 8877',
                r'I was born 15/12/1990, a file path is C:\Users\username\Pictures\BYG0Djh.png'
            ]
        }
    santiser_obj = RegexSanitiser(data_example)
    # Select to only run certain groups
    santiser_obj.select_groups(selection=['number', 'date', 'email', 'path', 'url'])
    # Replace placeholders
    santiser_obj.set_placeholders(placeholder_dict={'email': 'EMAILS>>', 'date': '<DA>TES>'})
    # Add a new pattern
    santiser_obj.add_pattern(
        pattern_name='custom',
        group='custom_group',
        placeholder='Cust',
        rank=.5,
        regex=r"jeans")
    # Attributes showing group selections - shows that the new pattern group was added
    ic(santiser_obj.all_groups)
    ic(santiser_obj.active_groups)
    ic(santiser_obj.inactive_groups)
    # Runs the patterns
    santiser_obj.sanitise()
    # Show match counts
    ic(santiser_obj.group_matches)
    ic(santiser_obj.total_matches)
    # 'raw_output_data' contains the result after 'sanitise()' is run
    ic(santiser_obj.output_data)
