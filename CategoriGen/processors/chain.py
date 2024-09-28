from datetime import datetime, timedelta
from functools import reduce
from typing import Self, Union, Optional
import copy
import re
import os

import pandas as pd

from CategoriGen.validation.data_types import (
    assert_and_log_type_is_data,
    assert_and_log_is_type_or_list_of,
    Data,
    IntList,
    IntStrList,
    Optional_StrList,
    Optional_dFrame,
    Optional_Data,
    Optional_Str,
    Optional_IntStrList,
    Record,
    Records,
    StrList,
    Str_or_StrList,
    Str_or_dFrame,
    dFrameObjListDict,
    dFrame_and_Data_Tuple,
)
from CategoriGen.tools.loggers import (
    ChainLogger,
    assert_and_log_error,
    log_and_raise_error,
    log_decorator,
)
from CategoriGen.processors.sanitiser import Sanitiser
from CategoriGen.processors.llm_handler import LLMHandler
from CategoriGen.tools.prepper import type_conditional_prepare
from CategoriGen.tools.directory_creators import create_files_directory
from CategoriGen.path_constants import OUTPUT_FILES_DIR

logger = ChainLogger().setup()


class Chain:
    @log_decorator(logger, "info", suffix_message="Initialise Chain object")
    def __init__(
        self,
        data_source: Str_or_dFrame,
        id_column: str,
        data_columns: Str_or_StrList,
        encoding: str = "utf-8",
        sheet_name: Union[int, str] = 0,
    ):
        """Common object for storing and passing the chained results of data processing."""

        # Initialise the records and expected_len variables
        self.expected_len = 0
        self.records: Records = {
            0: {
                "title": "initialisation",
                "dt": datetime.now(),
                "delta": None,
                "data": {},
                "column_names": None,
            }
        }

        # Prepare the data depending on the data_source type
        prep_results: dFrame_and_Data_Tuple = type_conditional_prepare(
            data_source, id_column, data_columns, encoding, sheet_name
        )

        # Unpack the returned tuple
        prepared_df = prep_results[0]
        prepared_data: Data = prep_results[1]

        # Before proceeding, set the 'protected' base variables (indicated by the leading underscore)
        self._base_dataframe: pd.DataFrame = copy.deepcopy(prepared_df)
        self._base_id_column = id_column

        # Finally, save this as the first record, while updating the expected length
        self.append(
            title="prepared",
            data=prepared_data,
            column_names=data_columns,
            update_expected_len=True,
        )

        # Inherit the sanitiser class and add new run method which writes records and uses the latest_data by default
        class Sanitise(Sanitiser):
            @log_decorator(logger, "info", suffix_message="chain.sanitiser object")
            def __init__(self, outer_chain: Chain, **kwargs):
                # Pass all arguments to superclass
                super(Sanitise, self).__init__(**kwargs)
                self.outer_chain: Chain = outer_chain
                self.use_selected: bool = False

                # Data to use if use_selected = True, otherwise use latest
                self.selected_data: Optional_Data = None

            @log_decorator(logger, "info", suffix_message="Use chain.sanitiser.run()")
            def run(self, title: str = "sanitised"):
                """Runs the sanitiser and appends the result to the chain."""
                # Check argument type
                assert_and_log_error(
                    logger,
                    "error",
                    type(title) is str,
                    f"Provided title argument '{title}' is not a string",
                )
                # Check new title is unique
                self.outer_chain.title_validator(title)
                # Sanitise the data
                self.sanitise()
                # Append and return self
                self.outer_chain.append(title=title, data=self.output_data)
                return self

        # Initialise the sanitiser - in __init__ 'latest_data' is set to it's initialised value - which won't update
        # So a wrapper property will access and update this with new data
        # Call this using the self.sanitiser property so input_data is updated with the default latest data
        self.base_sanitiser = Sanitise(outer_chain=self, input_data=self.latest_data)

    # Inherit the LLMHandler class and add new run method which writes records and uses the latest_data by default
    # Abstractions required intricate juggling of args and kwargs to pass the context of the current chain instance...
    # ... as well as implement a reference to that instance's self.latest_data property
    class Handler(LLMHandler):
        @log_decorator(logger, "info", suffix_message="chain.llm_handler object")
        def __init__(self, *args, **kwargs):
            # Take the outer chain reference
            outer_chain = kwargs["outer_chain"]
            # And remove it before using it in the super().__init__
            kwargs.pop("outer_chain")

            # Pass all arguments to superclass
            super().__init__(*args, **kwargs)

            # Save the outer_chain reference to self
            self.outer_chain: Chain = outer_chain

            # Values to be set post-initialisation, containing the result of methods setting custom targets

            # Activates or disables the use of manually 'selected' chain parts
            self.use_selected: bool = False
            # Actives all selections to be based on selected record key
            self.use_selected_of_record: bool = False
            # The key or title of the record to be accessed for use
            self.selected_record_key: Optional[Union[int, str]] = None
            # Input data which is used by llm_handler.run_async()
            self.selected_input_data: Optional_Data = None
            # Contains selected column names to be used by flatten_output_data() when generating for any multiplicative per-column outputs
            self.selected_column_names: Optional_StrList = None
            # Contains the name of the selected record title, which is appended to the cache identifier to keep it unique
            self.selected_record_title: Optional_Str = None

        @property
        @log_decorator(logger, is_property=True)
        def active_record_key(self) -> int:
            """Returns the selected record key when self.use_selected_of_record is True.
            Otherwise returns the key of the latest record"""
            # Use the selected key to access the record
            if self.use_selected_of_record:
                assert_and_log_error(
                    logger,
                    "error",
                    self.selected_record_key is not None,
                    "self.selected_record_key has not been set",
                )
                key = self.selected_record_key
            # Otherwise use the latest key
            else:
                key = self.outer_chain.latest_key
            return key

        @property
        @log_decorator(logger, is_property=True)
        def active_column_names(self) -> StrList:
            """Returns the selected column names when self.use_selected is True.
            Otherwise returns columns names of the active record key"""
            # Use column names stored in self.selected_column_names
            if self.use_selected:
                assert_and_log_error(
                    logger,
                    "error",
                    self.selected_column_names is not None,
                    "self.use_selected is True, but self.selected_column_names has not been set",
                )
                assert_and_log_is_type_or_list_of(
                    self.selected_column_names, "selected_column_names", [str]
                )
                column_names = self.selected_column_names
            # Otherwise use the active record key to access the column names
            else:
                column_names = self.outer_chain.column_names(self.active_record_key)
            # Return the outcome
            return column_names

        @property
        @log_decorator(logger, is_property=True)
        def active_input_data(self) -> Data:
            """Returns the selected data when self.use_selected is True.
            Otherwise returns data from the active record key"""
            # Use data stored in self.selected_column_names
            if self.use_selected:
                assert_and_log_error(
                    logger,
                    "error",
                    self.selected_input_data is not None,
                    "self.use_selected is True, self.selected_input_data has not been set",
                )
                assert_and_log_type_is_data(
                    self.selected_input_data, "selected_input_data"
                )
                input_data = self.selected_input_data
            # Otherwise use the active record key to access the data
            else:
                input_data = self.outer_chain.data(self.active_record_key)
            # Return the outcome
            return input_data

        @property
        @log_decorator(logger, is_property=True)
        def active_record_title(self) -> str:
            """Returns the selected title when self.use_selected is True.
            Otherwise returns the title using the active record key"""
            # Use data stored in self.selected_column_names
            if self.use_selected:
                assert_and_log_error(
                    logger,
                    "error",
                    self.selected_record_title is not None,
                    "self.use_selected is True, self.selected_title has not been set",
                )
                title = self.selected_record_title
            # Otherwise use the title from the active record key
            else:
                title = self.outer_chain.title(self.active_record_key)
            # Return the outcome
            return title

        @log_decorator(
            logger,
            "info",
            suffix_message="Set chain.llm_handler to use the latest record",
        )
        def use_latest(self):
            """Sets chain.llm_handler to use the latest record"""
            # Reset selection state, leading control flow to use latest values for all chain.llm_handler access properties
            self.use_selected: bool = False
            self.use_selected_of_record: bool = False

        @log_decorator(
            logger,
            "info",
            suffix_message="Set chain.llm_handler to use a specified record",
        )
        def use_record(self, record_key: Union[int, str]):
            """Sets chain.llm_handler to use a specified record"""
            # Assert the input type and assign the new record key
            assert_and_log_error(
                logger,
                "error",
                isinstance(record_key, (int, str)),
                "record_key key must be int or str",
            )
            self.selected_record_key: Union[int, str] = record_key
            # Set selection state
            self.use_selected: bool = False
            self.use_selected_of_record: bool = True
            # Assign values
            self.selected_column_names = self.outer_chain.column_names(
                self.active_record_key
            )
            self.selected_input_data = self.outer_chain.data(self.active_record_key)

        @log_decorator(
            logger,
            "info",
            suffix_message="Sets chain.llm_handler to use specified data and columns",
        )
        def use_selection(
            self, data: Data, record_title: str, column_names: Optional_StrList = None
        ):
            """Updates selected data and column_names. Will use the self.latest_column names if column_names is not specified."""
            # Assert argument types, and check record title is unique
            assert_and_log_type_is_data(data, "data")
            assert_and_log_is_type_or_list_of(
                column_names, "column_names", [str], allow_none=True
            )
            self.outer_chain.title_validator(record_title)
            # Assign values
            self.selected_column_names = (
                self.active_column_names if column_names is None else column_names
            )
            # Set selection state
            self.use_selected: bool = True
            self.use_selected_of_record: bool = False
            self.selected_input_data = data
            self.selected_record_title = record_title

        @log_decorator(logger, "info", suffix_message="Use chain.llm_handler.run()")
        def run(self, title):
            """Runs the llm_handler and appends the result to the chain."""
            # Check the new title is unique before proceeding
            self.outer_chain.title_validator(title)
            # Set self.input_data (used by run_async) to the active_input_data
            self.input_data = self.active_input_data
            # Run the llm_handler
            self.run_async()
            # Flatten and pivot the data into the Data format
            self.flatten_output_data(self.active_column_names)
            # Perform the append action and return self
            self.outer_chain.append(
                title=title,
                data=self.output_data,
                # Updating len since the llm validators can produce multiple columns per input
                update_expected_len=True,
            )
            return self

    @log_decorator(logger)
    def title_key(self, title: str) -> int:
        """Returns the record number for a given title"""
        for record_num, record_dict in self.records.items():
            try:
                if record_dict["title"] == title:
                    return record_num
            except TypeError as error:
                log_and_raise_error(
                    logger,
                    "error",
                    type(error),
                    f"Provided title 'f{title}' does not exist.",
                )

    @log_decorator(logger)
    def record(self, record_identifier: Union[int, str]) -> Record:
        """Returns the record corresponding to the provided record number or record title."""
        if isinstance(record_identifier, int):
            return self.records[record_identifier]
        elif isinstance(record_identifier, str):
            return self.records[self.title_key(record_identifier)]
        else:
            log_and_raise_error(
                logger,
                "error",
                TypeError,
                f"self.record() accepts either record keys (int) or record titles (str), not {type(record_identifier)}",
            )

    @log_decorator(logger)
    def title(self, key: Union[int, str]) -> str:
        """Returns the title corresponding to the provided record_identifier number."""
        return self.record(key)["title"]

    @log_decorator(logger)
    def dt(self, key: Union[int, str]) -> datetime:
        """Returns the datetime corresponding to the provided record_identifier number."""
        return self.record(key)["dt"]

    @log_decorator(logger)
    def data(self, key: Union[int, str]) -> Data:
        """Returns the data dictionary corresponding to the provided record_identifier number."""
        return self.record(key)["data"]

    @log_decorator(logger)
    def df(self, key: Union[int, str], recreate=False) -> pd.DataFrame:
        """Returns the dataframe corresponding to the provided record_identifier number.
        If recreate is True, the dataframe will be re-created from whatever data is in the record instead."""
        # Create the record's dataframe if it didn't exist yet, and return it
        self.create_dataframes(key, recreate=recreate)
        return self.record(key)["df"]

    @log_decorator(logger)
    def record_delta(self, key: Union[int, str]) -> timedelta:
        """Returns the timedelta corresponding to the provided record_identifier number."""
        return self.record(key)["delta"]

    @log_decorator(logger)
    def column_names(self, key: Union[int, str]) -> StrList:
        """Returns the list of column names corresponding to the provided record_identifier number."""
        return self.record(key)["column_names"]

    @property
    @log_decorator(logger, is_property=True)
    def sanitiser(self):
        """Alias for the sanitiser object, which also updates with provided or latest data, then returns sanitiser"""

        # Define the data to used based on settings
        new_data = (
            self.base_sanitiser.selected_data
            if self.base_sanitiser.use_selected
            else self.latest_data
        )
        self.base_sanitiser.input_data = new_data

        # Since sanitise runs on the output_data attribute, make deepcopy
        self.base_sanitiser.output_data = copy.deepcopy(new_data)

        # Return the changed sanitiser
        return self.base_sanitiser

    @log_decorator(logger)
    # WARNING!!! ON CHANGES - Manually syncronise these arguments, type hints and defaults with LLMHandler
    # Until such time you figure out how to have a function and a class share the same signature
    def set_llm_handler(
        self,
        provider: str,
        model: str,
        system_message: str,
        validation_model,
        cache_identifier: str,
        use_cache: bool = True,
        temperature: float = 0.2,
        max_validation_retries: int = 2,
        max_preprepared_coroutines: int = 10,
        max_awaiting_coroutines: int = 100,
    ):
        """Creates the LLMHandler / Handler object as self.llm_handler, passing in self.llm_handler.latest_data as the input.

        Differences to parent class LLMHandler:
        - Rather than taking a input_data argument, it inserts self.latest_data at the front of the args
            - The handler will then always run on the latest data
            - This can be changed with self.llm_handler.use_selection() or use_record(), and back with self.llm_handler.use_latest()
        - Passes a reference to the current chain instance up through the kwargs
            - This is removed from the kwargs before reaching LLMHandler"""

        # Grab the latest_data and put it in front of the args (which won't include it)
        args = (
            self.latest_data,
            provider,
            model,
            system_message,
            validation_model,
            cache_identifier,
        )
        kwargs = {
            "use_cache": use_cache,
            "temperature": temperature,
            "max_validation_retries": max_validation_retries,
            "max_preprepared_coroutines": max_preprepared_coroutines,
            "max_awaiting_coroutines": max_awaiting_coroutines,
        }

        # Adding self (aka the current chain into the kwargs)
        kwargs["outer_chain"] = self  # noqa: E402

        # Set the llm_handler using the adapted arguments
        self.llm_handler = self.Handler(*args, **kwargs)

    @property
    @log_decorator(logger, is_property=True)
    def latest_key(self) -> int:
        """Returns the max record_identifier from 'records'."""
        return max(self.records.keys())

    @property
    @log_decorator(logger, is_property=True)
    def latest_record(self) -> Record:
        """Returns the latest 'record' from 'records'."""
        return self.record(self.latest_key)

    @property
    @log_decorator(logger, is_property=True)
    def latest_title(self) -> str:
        """Returns the latest 'title' from 'records'."""
        return self.title(self.latest_key)

    @property
    @log_decorator(logger, is_property=True)
    def latest_dt(self) -> datetime:
        """Returns the latest 'dt' from 'records'."""
        return self.dt(self.latest_key)

    @property
    @log_decorator(logger, is_property=True)
    def latest_data(self) -> Data:
        """Returns the latest 'data' from the latest 'record' in 'records'."""
        return self.data(self.latest_key)

    @property
    @log_decorator(logger, is_property=True)
    def latest_df(self, recreate=False) -> Optional_dFrame:
        """Returns the latest 'df' from the latest 'record' in 'records'.
        If recreate is True, the dataframe will be re-created from whatever data is in the record instead."""
        return self.df(self.latest_key, recreate=recreate)

    @property
    @log_decorator(logger, is_property=True)
    def latest_record_delta(self) -> timedelta:
        """Returns the latest 'record_delta' from the latest 'record' in 'records'."""
        return self.record_delta(self.latest_key)

    @property
    @log_decorator(logger, is_property=True)
    def latest_column_names(self) -> StrList:
        """Returns the latest 'column_names' from 'records'."""
        return self.column_names(self.latest_key)

    @property
    @log_decorator(logger, is_property=True)
    def delta(self) -> timedelta:
        """Returns the overall timedelta."""
        return self.latest_dt - self.dt(0)

    @log_decorator(logger)
    def set_expected_len(self, value: int):
        """Updates the number of values expected for each list in the records data"""
        self.expected_len = value
        return self

    @log_decorator(logger)
    def key_validator(self, target_key: int):
        """Validates that records have identical keys, and no new or missing keys."""

        def key_list(key) -> list:
            return [x for x, y in dict.items(self.data(key))]

        target_title: str = self.title(target_key)
        initial_title: str = self.title(1)
        initial_key_list: list = key_list(1)
        target_key_list = key_list(target_key)
        initial_not_target = [x for x in initial_key_list if x not in target_key_list]
        initial_not_target_len = len(initial_not_target)
        target_not_initial = [x for x in target_key_list if x not in initial_key_list]
        target_not_initial_len = len(target_not_initial)
        total_differences = initial_not_target_len + target_not_initial_len

        if total_differences > 0:
            key_validator_message = ""
            if initial_not_target_len > 0:
                key_validator_message = (
                    f"{initial_not_target_len} keys were in the initial record 1"
                    f"'{initial_title}"
                    f"', but not in the appended record {target_key} '{target_title}'. "
                    f"These were the following keys: {initial_not_target}"
                )
            if target_not_initial_len > 0:
                key_validator_message = (
                    f"{target_not_initial_len} keys were in the appended record {target_key} "
                    f"'{target_title}"
                    f"', but not in the initial record 1 '{initial_title}'. "
                    f"These were the following keys: {target_not_initial}"
                )
            key_validator_message += (
                " | Records cannot be missing keys or add new keys that are not "
                "already in the initial record."
            )
            log_and_raise_error(logger, "error", KeyError, key_validator_message)

    @log_decorator(logger)
    def data_validator(self, target_key: int):
        """Checks that each list in the data of the target record has the expected length."""
        target_data: Data = self.data(target_key)
        target_title: str = self.title(target_key)
        bad_keys: IntList = []
        good_keys: IntList = []
        for key, value in target_data.items():
            if len(value) != self.expected_len:
                bad_keys.append(key)
            else:
                good_keys.append(key)
        bad_len: int = len(bad_keys)
        good_len: int = len(good_keys)
        data_validator_message = ""
        if good_len == 0:
            data_validator_message = (
                f"All keys in record '{target_title}' "
                f"contained lists of unexpected length / column count, "
                f"where expected length was '{self.expected_len}.'"
            )
        elif bad_len != 0:
            data_validator_message = (
                f"Some keys in record '{target_title}' "
                f"contained lists of unexpected length / column count, "
                f"where expected length was '{self.expected_len}'. "
                f"These keys were: {bad_keys}"
            )
        if bad_len != 0:
            log_and_raise_error(logger, "error", ValueError, data_validator_message)

    @log_decorator(logger)
    def title_validator(self, potential_title: str):
        """Checks that a given title str does not already exist in the records,
        which is necessary to maintain the uniqueness of the cache per-accessed record
        across the lifetime of a chain instance."""
        # Check argument type
        assert_and_log_error(
            logger,
            "error",
            isinstance(potential_title, str),
            "record titles must be str",
        )
        # Check provided title against all existing titles
        for key, record in self.records.items():
            assert_and_log_error(
                logger,
                "error",
                record["title"] != potential_title,
                f"record title '{potential_title} already exists'",
            )

    @log_decorator(logger)
    def append(
        self,
        title: str,
        data: Data,
        column_names: Optional_StrList = None,
        update_expected_len: bool = False,
    ):
        """Adds a new record to the 'records' dictionary."""
        if self.latest_key == 0 or update_expected_len:
            # Set expected len if this is the first entry or update_expected_len = True
            # Sets the len using the first list in the data dict
            self.set_expected_len(len(data[next(iter(data))]))
        else:
            # Check if the list of column names is of the correct length
            # Uses the latest if none were set
            if column_names is None:
                column_names_len = len(self.latest_column_names)
            else:
                # Otherwise, get the length from the provided list
                column_names_len = len(column_names)
            assert_and_log_error(
                logger,
                "error",
                column_names_len == self.expected_len,
                f"{self.expected_len} columns expected, but 'column_names' contains "
                f"{column_names_len} entries. If this is expected, set "
                f"self.append(update_expected_len=True), otherwise review your data.",
            )

        # Check that the provided title doesn't exist yet
        self.title_validator(title)

        # Build the record and validate the entry
        now: datetime = datetime.now()
        delta: timedelta = now - self.latest_dt
        new_key = self.latest_key + 1
        self.records[new_key] = {
            "title": title,
            "dt": now,
            "delta": delta,
            "data": data,
            # References previous values if none
            "column_names": self.latest_column_names
            if column_names is None
            else column_names,
        }
        self.key_validator(new_key)
        self.data_validator(new_key)
        return self

    @log_decorator(logger)
    def create_dataframes(
        self, records: IntStrList, use_suffix: bool = False, recreate=False
    ) -> Self:
        """Creates Dataframes in the selected records, and adds column names back on with optional suffixes.
        Returns the provided record keys afterwards.
        While recreate is False, only creates the dataframes if they didn't already exist in the record."""

        # Type assertions
        assert_and_log_error(
            logger,
            "info",
            isinstance(use_suffix, bool),
            f"Expected bool for use_suffix argument, instead got '{type(use_suffix)}'",
        )

        # Conditionally handling input based on argument type
        # Converts all record identifers to keys
        arg_type = type(records)
        if arg_type is int:
            records = [records]
        elif arg_type is str:
            records = [self.title_key(records)]
        elif arg_type is list:
            records = [self.title_key(x) if isinstance(x, str) else x for x in records]
        else:
            log_and_raise_error(
                logger,
                "info",
                TypeError,
                f"records argument must be a str, int, or list, it was {arg_type}",
            )

        # Looping over selected records and creating the dataframes if necessary
        for record_key in records:
            # Only create if recreate is True or the df didn't exist yet
            if recreate or "df" not in self.records[record_key]:
                # Get the needed items from the record
                data = self.data(record_key)

                # Creating a dataframe from the record data, treating the index as the row_id
                df = pd.DataFrame.from_dict(data, orient="index")

                # Record the new "df" in the dictionary of the specified record
                self.records[record_key]["df"] = df

            # Set the column names
            title = self.title(record_key)
            column_names = self.column_names(record_key)

            # Renaming columns
            # Includes suffixes if specified
            # This helps with concat errors when multiple outputs are generated per column
            self.records[record_key]["df"].columns = [
                name + "_" + title if use_suffix else name for name in column_names
            ]

        return self

    @log_decorator(logger)
    def get_combined(self, target_records: list, recreate=False) -> pd.DataFrame:
        """Combines a list of records and returns them as a dataframe.
        Does not append anything to the chain."""

        # Create dataframes in the selected records
        # Use suffix ensures that all columns are suffixed with the record title, preventing duplicate columns
        self.create_dataframes(target_records, use_suffix=True, recreate=recreate)

        # Create a list of dataframes from the record keys
        dataframes = []
        for record_key in target_records:
            dataframes.append(self.record(record_key)["df"])

        # Using reduce to merge all Dataframes on their indices
        # Skip if there is one record
        if len(dataframes) > 1:
            combined_df = reduce(
                lambda x, y: pd.merge(x, y, left_index=True, right_index=True), dataframes
            )
        else:
            combined_df = dataframes[0]

        return combined_df

    @log_decorator(logger)
    def get_rebase(self, records: IntStrList, recreate=False) -> pd.DataFrame:
        """Returns the specified records joined onto the base dataframe.
        If multiple records are provided they will be combined first.
        Does not append anything to the chain and is intended as an easy way to get your final output."""

        # Check the arguments
        assert_and_log_is_type_or_list_of(records, "records", [str, int])

        # Suffix added to column of non-base table fields if a duplicate exists in the merge
        # Blank by default, and is only set when individual records are specified
        suffix_on_duplicate = ""

        # Access a single df if the argument is str or int
        # Access a single record if the argument is a single item list
        if isinstance(records, list) and len(records) == 1:
            output_df = copy.deepcopy(self.df(records[0]))
            suffix_on_duplicate = self.title(records[0])
        # combine the records before rebasing
        # Handles adding suffixes inside get_combined()
        elif isinstance(records, list):
            output_df = copy.deepcopy(self.get_combined(records, recreate=recreate))
        # Otherwise it is a str or int
        else:
            output_df = copy.deepcopy(self.df(records))
            suffix_on_duplicate = self.title(records)

        # Join the output_df on the base _base_dataframe and return
        return self._base_dataframe.merge(
            output_df,
            how="left",
            left_on=self._base_id_column,
            right_index=True,
            suffixes=("", f"_{suffix_on_duplicate}"),
        )

    @log_decorator(logger)
    def get_output(
        self,
        record_keys: Optional_IntStrList = None,
        output_type: str = "dataframe",
        rebase: bool = True,
        combine: bool = True,
        recreate: bool = False,
    ) -> dFrameObjListDict:
        """
        Get the output of the specified records and return in specified output_type.

        Args:
            record_keys (Optional_IntStrList, optional): List of record keys to be included in the output. If None, the latest record is used. Defaults to None.
            output_type (str, optional): The type of output to be returned. Can be 'dataframe', 'list', 'nested list', or 'dict'. Defaults to "dataframe".
            rebase (bool, optional): If True, the output dataframes are joined onto the base dataframe. Defaults to True.
            combine (bool, optional): If True, the records are combined before joining onto the base dataframe or returning. Defaults to True.
            recreate (bool, optional): If True, the dataframes are recreated from the data in the records instead of using existing dataframes. Defaults to False.

        Returns:
            output_type == 'dataframe' -> dataframe
            output_type == 'list' -> [dataframe,]
            output_type == 'nested list' -> [[title, dataframe],]
            output_type == 'dict' -> {title:dataframe,}
        """

        # Check the provided keys
        assert_and_log_is_type_or_list_of(
            record_keys, "record_keys", [str, int], allow_none=True
        )

        # Check the provided output_type argument
        allowed_output_types = ("dataframe", "list", "nested list", "dict")
        assert_and_log_error(
            logger,
            "error",
            output_type in allowed_output_types,
            f"argument output_type '{output_type}' is not in allowed_output_types '{allowed_output_types}'",
        )

        # Sub in reference to the latest record if None was provided
        if record_keys is None:
            record_keys = [self.latest_key]
        # If there was only 1 record_key provided as a str or int, put it in a list
        else:
            if not isinstance(record_keys, list):
                record_keys = [record_keys]

        # Always set to combine when output_type is "dataframe" and len is > 1
        if output_type == "dataframe" and len(record_keys) > 1:
            combine = True

        # Prepare a list to contain processed titles and dataframes
        title_dataframe_lists = []

        # Then conditionally append to it

        # Append every record individually
        if not combine and not rebase:
            for key in record_keys:
                key_df_pair = [self.title(key), copy.deepcopy(self.df(key))]
                title_dataframe_lists.append(key_df_pair)

        # Rebase, then append each record individually
        if not combine and rebase:
            for key in record_keys:
                key_df_pair = [self.title(key), self.get_rebase(key, recreate=recreate)]
                title_dataframe_lists.append(key_df_pair)

        # Combine all records, then append
        if combine and not rebase:
            title = self.title(record_keys[0]) if len(record_keys) == 1 else "combined"
            key_df_pair = [title, self.get_combined(record_keys, recreate=recreate)]
            title_dataframe_lists.append(key_df_pair)

        # Combine all records, then rebase, then append
        if combine and rebase:
            title = self.title(record_keys[0]) if len(record_keys) == 1 else "combined"
            key_df_pair = [
                title,
                self.get_rebase(record_keys, recreate=recreate),
            ]
            title_dataframe_lists.append(key_df_pair)

        # Actualising the index and sorting the dataframes
        for title, df in title_dataframe_lists:
            # Insert the index as a col at 0, if it doesn't already exist (i.e. you are rebasing)
            if self._base_id_column not in df.columns:
                df.insert(0, self._base_id_column, df.index)
            # Sort by the id column ascending
            df.sort_values(self._base_id_column)

        # Conditionally return result

        # When dataframe is selected the list will only ever contain one dataframe
        if output_type == "dataframe":
            # Access the dataframe of the inner list
            return title_dataframe_lists[0][1]
        # Return only the dataframes in a list
        if output_type == "list":
            return [x[1] for x in title_dataframe_lists]
        # Return the list of lists unchanged
        if output_type == "nested list":
            return title_dataframe_lists
        # Return it as a dictionary with the title as the key
        if output_type == "dict":
            return {item[0]: item[1] for item in title_dataframe_lists}

    @log_decorator(logger)
    def write_output(
        self,
        file_type: str,
        file_name_prefix: str,
        record_keys: Optional_IntStrList = None,
        rebase: bool = True,
        combine: bool = True,
        xlsx_use_sheets: bool = True,
        recreate: bool = False,
    ) -> Self:
        """Writes the output of the selected records to a file or files."""

        # Sub in reference to the latest record if None was provided
        if record_keys is None:
            record_keys = [self.latest_key]
        # If there was only 1 record_key provided as a str or int, put it in a list
        else:
            if not isinstance(record_keys, list):
                record_keys = [record_keys]

        # Get the list of [[title, dataframe],] using self.get_output
        title_dataframe_lists = self.get_output(
            record_keys,
            output_type = "nested list",
            rebase=rebase,
            combine=combine,
            recreate=recreate,
        )

        def make_path(title: str) -> str:
            """Function to generate file paths for records."""
            formatted_time = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
            path_parts = [file_name_prefix, formatted_time]
            # Don't add the title if the string is blank
            if title != "":
                path_parts.insert(1, title)
            file_name = f"{' - '.join(path_parts)}.{file_type}"
            file_path = os.path.join(OUTPUT_FILES_DIR, file_name)
            return file_path

        # Create the files directory if it doesn't exist
        create_files_directory(logger)

        # Output the dataframes

        # csv will always have multiple files for multiple inputs
        if file_type == "csv":
            for title, df in title_dataframe_lists:
                df.to_csv(make_path(title), index=False)

        # xlsx will only have multiple files if xlsx_use_sheets is False
        if file_type == "xlsx" and not xlsx_use_sheets:
            for title, df in title_dataframe_lists:
                with pd.ExcelWriter(make_path(title)) as writer:
                    df.to_excel(writer, sheet_name=title, index=False)

        # xlsx will put the multiple records in the sheets of a single file if xlsx_use_sheets split is True
        if file_type == "xlsx" and xlsx_use_sheets:
            # Set the path of the file containing the multiple sheets
            # If len is 1, use the title of the record for the path, otherwise use an empty string
            file_title = self.title(record_keys[0]) if len(record_keys) == 1 else ""
            path = make_path(file_title)

            # Writing each record to its own sheet in the same xlsx file
            with pd.ExcelWriter(path) as writer:
                for title, df in title_dataframe_lists:
                    invalid_chars = r'[\/:*?"<>|]'
                    sheet_title = re.sub(invalid_chars, "", title)
                    df.to_excel(writer, sheet_name=sheet_title, index=False)

        return self
