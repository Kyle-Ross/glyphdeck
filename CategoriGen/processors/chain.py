from datetime import datetime, timedelta
from functools import reduce
from typing import Self
import copy
import re
import os

import pandas as pd

from CategoriGen.validation.data_types import (
    assert_and_log_type_is_data,
    assert_and_log_type_is_strlist,
    Data,
    IntList,
    IntStr,
    IntStrList,
    Optional_IntStr,
    Optional_StrList,
    Optional_dFrame,
    Optional_Data,
    Optional_Str,
    Optional_IntStrList,
    Record,
    RecordList,
    Records,
    StrList,
    Str_or_StrList,
    Str_or_dFrame,
    dFrame,
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
        sheet_name: IntStr = 0,
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
        self._base_dataframe: dFrame = copy.deepcopy(prepared_df)
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
            self.selected_record_key: Optional_IntStr = None
            # Input data which is used by llm_handler.run_async()
            self.selected_input_data: Optional_Data = None
            # Contains selected column names to be used by flatten_output_data() when generating for any multiplicative per-column outputs
            self.selected_column_names: Optional_StrList = None
            # Contains the name of the selected record title, which is appended to the cache identifier to keep it unique
            self.selected_record_title: Optional_Str = None

        @property
        @log_decorator(logger, is_property=True)
        def active_record_key(self) -> IntStr:
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
                assert_and_log_type_is_strlist(
                    self.selected_column_names, "selected_column_names"
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
        def use_record(self, record_key: IntStr):
            """Sets chain.llm_handler to use a specified record"""
            # Assert the input type and assign the new record key
            assert_and_log_error(
                logger,
                "error",
                isinstance(record_key, (int, str)),
                "record_key key must be int or str",
            )
            self.selected_record_key: IntStr = record_key
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
            assert_and_log_type_is_strlist(column_names, "column_names")
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
    def record(self, record_identifier: IntStr) -> Record:
        """Returns the record corresponding to the provided record number or record title."""
        if isinstance(record_identifier, int):
            return self.records[record_identifier]
        elif isinstance(record_identifier, str):
            return self.records[self.title_key(record_identifier)]
        else:
            log_and_raise_error(
                logger,
                "info",
                TypeError,
                f"self.record() accepts either record keys (int) or record titles (str), not {type(record_identifier)}",
            )

    @log_decorator(logger)
    def title(self, key: IntStr) -> str:
        """Returns the title corresponding to the provided record_identifier number."""
        return self.record(key)["title"]

    @log_decorator(logger)
    def dt(self, key: IntStr) -> datetime:
        """Returns the datetime corresponding to the provided record_identifier number."""
        return self.record(key)["dt"]

    @log_decorator(logger)
    def data(self, key: IntStr) -> Data:
        """Returns the data dictionary corresponding to the provided record_identifier number."""
        return self.record(key)["data"]

    @log_decorator(logger)
    def df(self, key: IntStr, recreate=False) -> dFrame:
        """Returns the dataframe corresponding to the provided record_identifier number.
        If recreate is True, the dataframe will be re-created from whatever data is in the record instead."""
        # Create the record's dataframe if it didn't exist yet, and return it
        self.create_dataframes(key, recreate=recreate)
        return self.record(key)["df"]

    @log_decorator(logger)
    def record_delta(self, key: IntStr) -> timedelta:
        """Returns the timedelta corresponding to the provided record_identifier number."""
        return self.record(key)["delta"]

    @log_decorator(logger)
    def column_names(self, key: IntStr) -> StrList:
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
            # Only if recreate is True or the df didn't exist yet
            if recreate or record_key not in self.records:
                # Get the needed items from the record
                data = self.data(record_key)
                title = self.title(record_key)
                column_names = self.column_names(record_key)

                # Creating a dataframe from the record data, treating the index as the row_id
                df = pd.DataFrame.from_dict(data, orient="index")

                # Renaming columns
                # Includes suffixes if specified
                # This helps with concat errors when multiple outputs are generated per column
                df.columns = [
                    name + "_" + title if use_suffix else name for name in column_names
                ]

                # Record the new "df" in the dictionary of the specified record
                self.records[record_key]["df"] = df

        return self

    @log_decorator(logger)
    def combine_records(
        self, new_record_title: str, target_records: list, recreate=False
    ) -> Self:
        """Creates a new record by combining multiple records, then appends it to the chain.
        While recreate is False, only creates the dataframes it needs if they didn't already exist in the records."""

        # Create dataframes in the selected records
        self.create_dataframes(target_records, use_suffix=True, recreate=recreate)

        # Create a list of dataframes from the record keys
        dataframes = []
        for record_key in target_records:
            dataframes.append(self.records[record_key]["df"])

        # Using reduce to merge all Dataframes on their indices
        combined_df = reduce(
            lambda x, y: pd.merge(x, y, left_index=True, right_index=True), dataframes
        )

        # Generate the required arguments to append the combined_df to the chain

        # Use every column except the id as a data column
        combined_data_columns = [
            col for col in combined_df.columns if col != self._base_id_column
        ]

        # Use the prepare function on the combined_df, and get the data object from the returned tuple
        combined_data = type_conditional_prepare(
            combined_df, self._base_id_column, combined_data_columns
        )[1]

        # Append these to the chain
        self.append(
            title=new_record_title,
            data=combined_data,
            column_names=combined_data_columns,
            update_expected_len=True,
        )

        return self

    @log_decorator(logger)
    def get_rebase(self, key: IntStr) -> pd.DataFrame:
        """Returns the specified record joined onto the base dataframe.
        Does not append anything to the chain and is intended as an easy way to get your final output."""
        return self._base_dataframe.merge(
            self.record(key),
            how="left",
            left_on=self._base_id_column,
            right_index=True,
        )

    @log_decorator(logger)
    def write_output(
        self,
        file_type: str,
        file_name_prefix: str,
        record_keys: Optional_IntStrList = None,
        rebase: bool = True,
        split: bool = False,
    ) -> Self:
        """Writes the output of the selected records to a file."""

        # Checking file_type is in allowed list
        allowed_file_types = ["csv", "xlsx"]
        assert_and_log_error(
            logger,
            "error",
            file_type in allowed_file_types,
            f"'{file_type}' is not in allowed list {allowed_file_types}.",
        )

        # Sub in reference to the latest record if None was provided
        if record_keys is None:
            record_keys = self.latest_key

        # Create the dataframes for each record if they don't exist yet
        self.create_dataframes(record_keys)

        # Set the list of target records conditionally
        if split:
            records_list: RecordList = self.create_dataframes(
                record_keys, use_suffix=False
            )
        # Multiple records must be combined if only one output is desired
        else:
            records_list: RecordList = self.combine_records(record_keys)

        # Use the dataframes as is, or left join each back onto the initial source
        if rebase:
            for record in records_list:
                record["df"] = self._base_dataframe.merge(
                    record["df"],
                    how="left",
                    left_on=self._base_id_column,
                    right_index=True,
                )

        # Final changes to the df
        for record in records_list:
            df = record["df"]
            # Insert the index as a col at 0, if it doesn't already exist (i.e. you are rejoining)
            if self._base_id_column not in df.columns:
                df.insert(0, self._base_id_column, df.index)
            # Sort by the id column ascending
            df.sort_values(self._base_id_column)
            record["df"] = df

        def make_path(source_record: Record) -> str:
            """Function to generate file paths for records."""
            formatted_time = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
            # Conditionally setting the title to be used in the name
            # Each individual file has the title name
            if file_type == "csv" and split:
                title = source_record["title"]
            # The containing excel file has 'split'
            elif file_type == "xlsx" and split:
                title = "split"
            # Should be unreachable
            elif split:
                title = "split"
            else:
                title = "combined"

            file_name = f"{file_name_prefix} - {title} - {formatted_time}.{file_type}"
            file_path = os.path.join(OUTPUT_FILES_DIR, file_name)
            return file_path

        # Create the files directory if it doesn't exist
        create_files_directory(logger)

        # Output the dataframes
        if file_type == "csv":
            for record in records_list:
                df = record["df"]
                path = make_path(record)
                df.to_csv(path, index=False)

        if file_type == "xlsx":
            # Argument may not be used in certain conditions
            path = make_path(self.latest_record)
            with pd.ExcelWriter(path) as writer:
                for record in (
                    records_list
                ):  # Writing each record to its own sheet in the same xlsx file
                    sheet_df = record["df"]
                    invalid_chars = r'[\/:*?"<>|]'
                    sheet_title = re.sub(invalid_chars, "", record["title"])
                    sheet_df.to_excel(writer, sheet_name=sheet_title, index=False)

        return self
