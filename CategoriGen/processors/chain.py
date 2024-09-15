from datetime import datetime, timedelta
from functools import reduce
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
    List_or_Str,
    Optional_IntStr,
    Optional_StrList,
    Optional_dFrame,
    Optional_Data,
    Optional_Str,
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
                "table": None,
                "table_id_column": None,
                "column_names": None,
            }
        }

        # Prepare the data depending on the data_source type
        prep_results: dFrame_and_Data_Tuple = type_conditional_prepare(
            data_source, id_column, data_columns, encoding, sheet_name
        )
        source_table: dFrame = prep_results[0]
        prepared_data: Data = prep_results[1]

        # Finally, save this as the first record, while updating the expected length
        # This is that becomes the initial record
        self.append(
            title="prepared",
            data=prepared_data,
            table=source_table,
            table_id_column=id_column,
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
        self.initial_sanitiser = Sanitise(outer_chain=self, input_data=self.latest_data)

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
        key_type = type(record_identifier)
        if key_type is int:
            return self.records[record_identifier]
        if key_type is str:
            return self.records[self.title_key(record_identifier)]

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
    def table(self, key: IntStr) -> dFrame:
        """Returns the table corresponding to the provided record_identifier number."""
        return self.record(key)["table"]

    @log_decorator(logger)
    def table_id_column(self, key: IntStr) -> Optional_IntStr:
        """Returns the table corresponding to the provided record_identifier number."""
        return self.record(key)["table_id_column"]

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
            self.initial_sanitiser.selected_data
            if self.initial_sanitiser.use_selected
            else self.latest_data
        )
        self.initial_sanitiser.input_data = new_data

        # Since sanitise runs on the output_data attribute, make deepcopy
        self.initial_sanitiser.output_data = copy.deepcopy(new_data)

        # Return the changed sanitiser
        return self.initial_sanitiser

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
    def latest_table(self) -> Optional_dFrame:
        """Returns the latest 'table' from the latest 'record' in 'records'."""
        return self.table(self.latest_key)

    @property
    @log_decorator(logger, is_property=True)
    def latest_table_id_column(self) -> Optional_IntStr:
        """Returns the latest 'table_id_column' from the latest 'record' in 'records'."""
        return self.table_id_column(self.latest_key)

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
    def initial_key(self) -> int:
        """Returns 1, but only if record_identifier 1 exists in the records."""
        if 1 not in self.records:
            log_and_raise_error(
                logger,
                "error",
                KeyError,
                "KeyError: Initial record does not exist yet! Use self.append to get started.",
            )
        return 1

    @property
    @log_decorator(logger, is_property=True)
    def initial_record(self) -> Record:
        """Returns the initial 'record' from 'records'."""
        return self.record(self.initial_key)

    @property
    @log_decorator(logger, is_property=True)
    def initial_title(self) -> str:
        """Returns the initial 'title' from 'records'."""
        return self.title(self.initial_key)

    @property
    @log_decorator(logger, is_property=True)
    def initial_dt(self) -> datetime:
        """Returns the initial 'dt' from 'records'."""
        return self.dt(self.initial_key)

    @property
    @log_decorator(logger, is_property=True)
    def initial_data(self) -> Data:
        """Returns the initial 'data' from 'records'."""
        return self.data(self.initial_key)

    @property
    @log_decorator(logger, is_property=True)
    def initial_table(self) -> Optional_dFrame:
        """Returns the initial 'table' from 'records'."""
        return self.table(self.initial_key)

    @property
    @log_decorator(logger, is_property=True)
    def initial_table_id_column(self) -> Optional_IntStr:
        """Returns the initial 'table_id_column' from 'records'."""
        return self.table_id_column(self.initial_key)

    @property
    @log_decorator(logger, is_property=True)
    def initial_column_names(self) -> StrList:
        """Returns the initial 'column_names' from 'records'."""
        return self.column_names(self.initial_key)

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
        initial_title: str = self.title(self.initial_key)
        initial_key_list: list = key_list(self.initial_key)
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
                    f"{initial_not_target_len} keys were in the initial record {self.initial_key} "
                    f"'{initial_title}"
                    f"', but not in the appended record {target_key} '{target_title}'. "
                    f"These were the following keys: {initial_not_target}"
                )
            if target_not_initial_len > 0:
                key_validator_message = (
                    f"{target_not_initial_len} keys were in the appended record {target_key} "
                    f"'{target_title}"
                    f"', but not in the initial record {self.initial_key} '{initial_title}'. "
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
        table: Optional_dFrame = None,
        table_id_column: Optional_IntStr = None,
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

        # Build the record
        now: datetime = datetime.now()
        delta: timedelta = now - self.latest_dt
        new_key = self.latest_key + 1
        self.records[new_key] = {
            "title": title,
            "dt": now,
            "delta": delta,
            "data": data,
            # References previous values if none
            "table": self.latest_table if table is None else table,
            "table_id_column": self.latest_table_id_column
            if table_id_column is None
            else table_id_column,
            "column_names": self.latest_column_names
            if column_names is None
            else column_names,
        }
        self.key_validator(new_key)
        self.data_validator(new_key)
        return self

    @log_decorator(logger)
    def selector(self, records: List_or_Str, use_suffix: bool) -> RecordList:
        """Returns a list of clean DataFrames from the selected records. Adds column names back on."""
        # Handling str input
        if type(records) is str:
            records = [records]
        # Add only selected records to a list
        selected_records = [self.record(record) for record in records]

        # Looping over selected records and making changes
        for record in selected_records:
            # Creating dataframes and in each of the records, treating the index as the row_id
            df = pd.DataFrame.from_dict(record["data"], orient="index")
            # Renaming columns
            if use_suffix:  # Includes suffixes when there are multiple selections to avoid concatenation errors
                df.columns = [
                    name + "_" + record["title"] for name in record["column_names"]
                ]
            else:
                df.columns = record["column_names"]
            # Record the new df
            record["output_df"] = df

        # Returning the selected records
        return selected_records

    @log_decorator(logger)
    def combiner(self, records: list) -> RecordList:
        """Combines records into a single dataframe."""
        # Grab the dataframes, combining them, and returning them
        selected_records = self.selector(records, use_suffix=True)
        selected_dfs = [record["output_df"] for record in selected_records]
        # Using reduce to merge all DataFrames in selected_dfs on their indices
        combined_df = reduce(
            lambda x, y: pd.merge(x, y, left_index=True, right_index=True), selected_dfs
        )
        # Building into the record format to be used in the flow with the rest
        combined_record: RecordList = [
            {
                "title": "combined",
                "dt": datetime.now(),
                "delta": datetime.now() - self.latest_dt,
                "data": {},
                "table": selected_records[0],
                "output_df": combined_df,
                "table_id_column": self.initial_table_id_column,  # Not really needed
                "column_names": None,  # Not needed here - all different anyway
            }
        ]
        return combined_record

    @log_decorator(logger)
    def output(
        self,
        records: List_or_Str,
        file_type: str,
        name_prefix: str,
        rejoin: bool = True,
        split: bool = False,
    ):
        # Checking file_type is in allowed list
        allowed_file_types = ["csv", "xlsx"]
        assert_and_log_error(
            logger,
            "error",
            file_type in allowed_file_types,
            f"'{file_type}' is not in allowed list {allowed_file_types}.",
        )
        # Use the separate or combined records
        if split:
            records_list: RecordList = self.selector(records, use_suffix=False)
        else:
            records_list: RecordList = self.combiner(records)
        # Use the dataframes as is, or left join each back onto the initial source
        if rejoin:
            for record in records_list:
                record["output_df"] = self.initial_table.merge(
                    record["output_df"],
                    how="left",
                    left_on=self.initial_table_id_column,
                    right_index=True,
                )

        # Final changes to the output_df
        for record in records_list:
            df = record["output_df"]
            # Insert the index as a col at 0, if it doesn't already exist (i.e. you are rejoining)
            if record["table_id_column"] not in df.columns:
                df.insert(0, record["table_id_column"], df.index)
            df.sort_values(record["table_id_column"])  # Sort by the id column ascending
            record["output_df"] = df

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

            file_name = f"{name_prefix} - {title} - {formatted_time}.{file_type}"
            file_path = os.path.join(OUTPUT_FILES_DIR, file_name)
            return file_path

        # Create the files directory if it doesn't exist
        create_files_directory(logger)

        # Output the dataframes
        if file_type == "csv":
            for record in records_list:
                df = record["output_df"]
                path = make_path(record)
                df.to_csv(path, index=False)

        if file_type == "xlsx":
            # Argument may not be used in certain conditions
            path = make_path(self.latest_record)
            with pd.ExcelWriter(path) as writer:
                for record in (
                    records_list
                ):  # Writing each record to its own sheet in the same xlsx file
                    sheet_df = record["output_df"]
                    invalid_chars = r'[\/:*?"<>|]'
                    sheet_title = re.sub(invalid_chars, "", record["title"])
                    sheet_df.to_excel(writer, sheet_name=sheet_title, index=False)
