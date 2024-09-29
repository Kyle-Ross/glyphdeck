from datetime import datetime, timedelta
from functools import reduce
from typing import Self, Union, Optional, List, Dict, Tuple
import copy
import re
import os

import pandas as pd

from glyphdeck.validation.data_types import (
    assert_and_log_type_is_data,
    assert_and_log_is_type_or_list_of,
    DataDict,
    Optional_DataDict,
    RecordDict,
    RecordsDict,
)
from glyphdeck.tools.loggers import (
    ChainLogger,
    assert_and_log_error,
    log_and_raise_error,
    log_decorator,
)
from glyphdeck.processors.sanitiser import Sanitiser
from glyphdeck.processors.llm_handler import LLMHandler
from glyphdeck.tools.prepper import type_conditional_prepare
from glyphdeck.tools.directory_creators import create_files_directory
from glyphdeck.path_constants import OUTPUT_FILES_DIR

logger = ChainLogger().setup()


class Chain:
    """
    Chain class is designed to handle and process a sequence of data records.

    Attributes:
        records (Dict[int, RecordDict]): A dictionary to hold all records.
        expected_len (int): The number of values expected in each list in the records data.
        sanitiser (Sanitiser): An instance of the Sanitiser class to handle data sanitization.
        llm_handler (LLMHandler): An instance to handle operations related to large language models.
    """

    @log_decorator(logger, "info", suffix_message="Initialise Chain object")
    def __init__(
        self,
        data_source: Union[str, pd.DataFrame],
        id_column: str,
        data_columns: Union[str, List[str]],
        encoding: str = "utf-8",
        sheet_name: Union[int, str] = 0,
    ):
        """Initializes the Chain instance with the provided data source and configuration parameters.

        Args:
            data_source: The source of the data, which can be either a file path (str) or a DataFrame.
            id_column: The name of the column to be used as the unique identifier.
            data_columns: The column(s) from the data source to be processed. Can be a single column name (str) or a list of column names (List[str]).
            encoding: The encoding format for reading data if `data_source` is a file. Defaults to "utf-8".
            sheet_name: The sheet name or index for reading data if `data_source` is an Excel file. Defaults to 0.

        Returns:
            None
        """

        # Initialise the records and expected_len variables
        self.expected_len = 0
        self.records: RecordsDict = {
            0: {
                "title": "initialisation",
                "dt": datetime.now(),
                "delta": None,
                "data": {},
                "column_names": None,
            }
        }

        # Prepare the data depending on the data_source type
        prep_results: Tuple[pd.DataFrame, DataDict] = type_conditional_prepare(
            data_source, id_column, data_columns, encoding, sheet_name
        )

        # Unpack the returned tuple
        prepared_df = prep_results[0]
        prepared_data: DataDict = prep_results[1]

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
            """Represents a sanitiser that is part of the Chain class. This class inherits
            from Sanitiser and is used to sanitise and append data to the chain.

            Attributes:
                outer_chain: The Chain instance that includes the Sanitise class.
                use_selected: A boolean indicating whether to use selected data or not.
                selected_data: Optional data dictionary to use when use_selected is True.
            """

            @log_decorator(logger, "info", suffix_message="chain.sanitiser object")
            def __init__(self, outer_chain: Chain, **kwargs):
                """Initializes the Sanitise instance associated with the Chain instance.

                Args:
                    outer_chain: The Chain instance that this Sanitise instance is a part of.
                    **kwargs: Additional keyword arguments to be passed to the Sanitiser superclass.

                Returns:
                    None
                """
                # Pass all arguments to superclass
                super(Sanitise, self).__init__(**kwargs)
                self.outer_chain: Chain = outer_chain
                self.use_selected: bool = False

                # Data to use if use_selected = True, otherwise use latest
                self.selected_data: Optional_DataDict = None

            @log_decorator(logger, "info", suffix_message="Use chain.sanitiser.run()")
            def run(self, title: str = "sanitised"):
                """Runs the sanitiser and appends the result to the chain.

                Args:
                    title: The title to be given to the sanitised record. Defaults to "sanitised".

                Returns:
                    The Sanitiser object, capable of being further used to chain additional operations
                """
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
        """Inherits from LLMHandler, handles the interaction with LLM providers and manages the processing of input data for asynchronous querying.

        Attributes:
            outer_chain: Reference to the Chain instance that this Handler is associated with.
            use_selected: Boolean flag to indicate whether to use manually 'selected' data or the latest data.
            use_selected_of_record: Boolean flag to indicate whether to use a selected record in the chain.
            selected_record_identifier: The identifier (key or title) of the selected record to be accessed.
            selected_input_data: The data dictionary selected for use by the handler.
            selected_column_names: A list of column names to be used by the handler, if specified.
            selected_record_title: The title of the selected record, used to keep the cache identifier unique.
        """

        @log_decorator(logger, "info", suffix_message="chain.llm_handler object")
        def __init__(self, *args, **kwargs):
            """Initializes the Handler class with the provided arguments and Keyword arguments.

            This constructor method passes all input parameters to the superclass (LLMHandler) constructor,
            while also storing a reference to the Chain instance it is associated with.

            Args:
                *args: Variable length argument list passed to the LLMHandler.
                **kwargs: Arbitrary keyword arguments passed to the LLMHandler. One of these keyword arguments should be 'outer_chain',
                           which is a reference to the Chain instance.
            """
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
            self.selected_record_identifier: Optional[Union[int, str]] = None
            # Input data which is used by llm_handler.run_async()
            self.selected_input_data: Optional_DataDict = None
            # Contains selected column names to be used by flatten_output_data() when generating for any multiplicative per-column outputs
            self.selected_column_names: Optional[List[str]] = None
            # Contains the name of the selected record title, which is appended to the cache identifier to keep it unique
            self.selected_record_title: Optional[str] = None

        @property
        @log_decorator(logger, is_property=True)
        def active_record_key(self) -> int:
            """Returns the key of the active record based on current selection state.

            If `self.use_selected_of_record` is True, returns the selected record key.
            Otherwise, returns the key of the latest record.

            Returns:
                int: The key of the active record.
            """

            # Use the selected key to access the record
            if self.use_selected_of_record:
                assert_and_log_error(
                    logger,
                    "error",
                    self.selected_record_identifier is not None,
                    "self.selected_record_key has not been set",
                )
                key = self.selected_record_identifier
            # Otherwise use the latest key
            else:
                key = self.outer_chain.latest_key
            return key

        @property
        @log_decorator(logger, is_property=True)
        def active_column_names(self) -> List[str]:
            """Returns the column names of the active record.

            Depending on the state of `self.use_selected`, this method retrieves the column names from either the selected column names
            or the active record in the chain.

            Returns:
                List[str]: The list of active column names.
            """
            # Use data stored in self.selected_column_names
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
        def active_input_data(self) -> DataDict:
            """
            Returns the input data to be used by the Handler, determined by the current selection state.

            If `self.use_selected` is True, it returns `self.selected_input_data`.
            Otherwise, it returns the data of the active record key.

            Returns:
                DataDict: The input data dictionary to be used by the Handler.
            """
            # Use data stored in self.selected_input_data
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
            """Returns the title of the active record.

            Depending on the state of `self.use_selected`, this method retrieves the title from
            either the selected record or the active record in the chain.

            Returns:
                str: The title of the active record.
            """
            # Use data stored in self.selected_record_title
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
            """
            Sets the LLMHandler to use the latest record in the Chain.

            When invoked, this method ensures that the LLMHandler will operate
            on the latest record in the Chain rather than any manually selected data.

            Args:
                None

            Returns:
                None
            """
            # Reset selection state, leading control flow to use latest values for all chain.llm_handler access properties
            self.use_selected: bool = False
            self.use_selected_of_record: bool = False

        @log_decorator(
            logger,
            "info",
            suffix_message="Set chain.llm_handler to use a specified record",
        )
        def use_record(self, record_identifier: Union[int, str]):
            """Sets chain.llm_handler to use a specified record.

            Args:
                record_identifier: The identifier of the record to be used. Can be an integer representing the record key,
                or a string representing the record title.

            Returns:
                None
            """
            # Assert the input type and assign the new record key
            assert_and_log_error(
                logger,
                "error",
                isinstance(record_identifier, (int, str)),
                "record_identifier key must be int or str",
            )
            self.selected_record_identifier: Union[int, str] = record_identifier
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
            self,
            data: DataDict,
            record_title: str,
            column_names: Optional[List[str]] = None,
        ):
            """Updates selected data and column_names. Will use the self.latest_column names if column_names is not specified.

            When selected through this method, the handler will use the provided data,
            column names (if any), and record title for future processing steps.

            Args:
                data: The data to be utilized.
                record_title: A unique title given to this specific record of data.
                column_names: A list of column names to be used for this data. Defaults to None.

            Returns:
                None
            """
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
            """Runs the LLMHandler and appends the results to the chain.

            The function will process the LLMHandler with the current settings and append
            the resulting output data to the chain as a new record with the specified title.

            Args:
                title (str): The title to be assigned to the new record in the chain.

            Returns:
                Handler: The Handler object, allowing further chained operations.
            """
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
        """
        Returns the record number for a given title.

        Args:
            title: The title of the record to retrieve the key for.

        Returns:
            int: The key of the record associated with the given title.

        Raises:
            TypeError: If the provided title does not exist in the records.
        """
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
    def record(self, record_identifier: Union[int, str]) -> RecordDict:
        """Returns the record corresponding to the provided record number or record title.

        Args:
            record_identifier: The identifier for the record, which can be either an integer (record number) or a string (record title).

        Returns:
            RecordDict: The record dict corresponding to the provided identifier.

        Raises:
            TypeError: If the provided record_identifier is not an integer or string.
        """
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
    def title(self, record_identifier: Union[int, str]) -> str:
        """Returns the title corresponding to the provided record_identifier number.

        Args:
            record_identifier: The record identifier, which can be an integer or a string.

        Returns:
            str: The title of the specified record.
        """
        return self.record(record_identifier)["title"]

    @log_decorator(logger)
    def dt(self, record_identifier: Union[int, str]) -> datetime:
        """
        Returns the datetime corresponding to the provided record_identifier.

        Args:
            record_identifier: The record identifier, which can be an integer or a string.

        Returns:
            datetime: The datetime of the specified record.
        """
        return self.record(record_identifier)["dt"]

    @log_decorator(logger)
    def data(self, record_identifier: Union[int, str]) -> DataDict:
        """Returns the data dictionary corresponding to the provided record_identifier.

        Args:
            record_identifier: The record identifier, which can be an integer or a string.

        Returns:
            DataDict: The data dictionary of the specified record.
        """
        return self.record(record_identifier)["data"]

    @log_decorator(logger)
    def df(self, record_identifier: Union[int, str], recreate=False) -> pd.DataFrame:
        """Returns the dataframe corresponding to the provided record_identifier.

        Args:
            record_identifier: The record identifier, which can be an integer or a string.
            recreate: A boolean indicating whether to recreate the dataframe from the data in the record. Defaults to False.

        Returns:
            pd.DataFrame: The dataframe of the specified record.
        """
        # Create the record's dataframe if it didn't exist yet, and return it
        self.create_dataframes(record_identifier, recreate=recreate)
        return self.record(record_identifier)["df"]

    @log_decorator(logger)
    def record_delta(self, record_identifier: Union[int, str]) -> timedelta:
        """Returns the timedelta corresponding to the provided record_identifier.

        Args:
            record_identifier: The record identifier, which can be an integer or a string.

        Returns:
            timedelta: The timedelta of the specified record.
        """
        return self.record(record_identifier)["delta"]

    @log_decorator(logger)
    def column_names(self, record_identifier: Union[int, str]) -> List[str]:
        """Returns the list of column names corresponding to the provided record_identifier.

        Args:
            record_identifier: The record identifier, which can be an integer or a string.

        Returns:
            List[str]: The list of column names for the specified record.
        """
        return self.record(record_identifier)["column_names"]

    @property
    @log_decorator(logger, is_property=True)
    def sanitiser(self):
        """Returns the sanitiser object, updating it with provided or latest data.

        Returns:
            Sanitiser: The updated sanitiser object.
        """

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
        """Sets up the LLMHandler for the Chain instance.

        Rather than taking a input_data argument, it always uses self.latest_data. This can be changed.
        Also Passes a reference to the current chain instance up through the kwargs.

        Args:
            provider: The name of the LLM provider.
            model: The specific model to be used.
            system_message: The system message to be used by the LLM.
            validation_model: The model used for data validation.
            cache_identifier: The identifier for cache storage.
            use_cache: Whether to use caching. Defaults to True.
            temperature: The sampling temperature for the LLM. Defaults to 0.2.
            max_validation_retries: The maximum number of validation retries. Defaults to 2.
            max_preprepared_coroutines: The maximum number of pre-prepared coroutines. Defaults to 10.
            max_awaiting_coroutines: The maximum number of awaiting coroutines. Defaults to 100.

        Returns:
            None
        """

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
        """Returns the key of the latest record.

        Returns:
            int: The key of the latest record.
        """
        return max(self.records.keys())

    @property
    @log_decorator(logger, is_property=True)
    def latest_record(self) -> RecordDict:
        """Returns the latest record dictionary.

        Returns:
            RecordDict: The latest record data.
        """
        return self.record(self.latest_key)

    @property
    @log_decorator(logger, is_property=True)
    def latest_title(self) -> str:
        """Returns the title of the latest record.

        Returns:
            str: The title of the latest record.
        """
        return self.title(self.latest_key)

    @property
    @log_decorator(logger, is_property=True)
    def latest_dt(self) -> datetime:
        """Returns the datetime of the latest record.

        Returns:
            datetime: The datetime of the latest record.
        """
        return self.dt(self.latest_key)

    @property
    @log_decorator(logger, is_property=True)
    def latest_data(self) -> DataDict:
        """Returns the data of the latest record.

        Returns:
            DataDict: The data dictionary of the latest record.
        """
        return self.data(self.latest_key)

    @property
    @log_decorator(logger, is_property=True)
    def latest_df(self, recreate=False) -> pd.DataFrame:
        """Returns the DataFrame of the latest record.

        Args:
            recreate: Whether to recreate the DataFrame from the data. Defaults to False.

        Returns:
            pd.DataFrame: The DataFrame of the latest record.
        """
        return self.df(self.latest_key, recreate=recreate)

    @property
    @log_decorator(logger, is_property=True)
    def latest_record_delta(self) -> timedelta:
        """Returns the timedelta of the latest record.

        Returns:
            timedelta: The timedelta of the latest record.
        """
        return self.record_delta(self.latest_key)

    @property
    @log_decorator(logger, is_property=True)
    def latest_column_names(self) -> List[str]:
        """Returns the column names of the latest record.

        Returns:
            List[str]: The list of column names of the latest record.
        """
        return self.column_names(self.latest_key)

    @property
    @log_decorator(logger, is_property=True)
    def delta(self) -> timedelta:
        """Returns the overall timedelta of the chain.

        Returns:
            timedelta: The overall timedelta from the initialisation to the latest record.
        """
        return self.latest_dt - self.dt(0)

    @log_decorator(logger)
    def set_expected_len(self, value: int):
        """Sets the expected length of the data lists in records.

        Args:
            value: The expected length for each list in the records data.

        Returns:
            None
        """
        self.expected_len = value
        return self

    @log_decorator(logger)
    def key_validator(self, record_identifier: int):
        """Validates that records have identical keys, without any new or missing keys.

        Args:
            record_identifier: The identifier of the record to be validated.

        Returns:
            None
        """

        def key_list(key) -> list:
            return [x for x, y in dict.items(self.data(key))]

        target_title: str = self.title(record_identifier)
        initial_title: str = self.title(1)
        initial_key_list: list = key_list(1)
        target_key_list = key_list(record_identifier)
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
                    f"', but not in the appended record {record_identifier} '{target_title}'. "
                    f"These were the following keys: {initial_not_target}"
                )
            if target_not_initial_len > 0:
                key_validator_message = (
                    f"{target_not_initial_len} keys were in the appended record {record_identifier} "
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
    def data_validator(self, record_identifier: int):
        """Validates that each list in the data of the target record has the expected length.

        Args:
            record_identifier: The identifier of the record to be validated.

        Returns:
            None
        """
        target_data: DataDict = self.data(record_identifier)
        target_title: str = self.title(record_identifier)
        bad_keys: List[int] = []
        good_keys: List[int] = []
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
        """Validates that a given title does not already exist in the records.

        Args:
            potential_title: The title to be validated.

        Returns:
            None
        """
        # This is necessary to maintain the uniqueness of the cache per-accessed record across the lifetime of a chain instance.

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
        data: DataDict,
        column_names: Optional[Union[str, List[str]]] = None,
        update_expected_len: bool = False,
    ):
        """Adds a new record to the 'records' dictionary.

        Args:
            title: The title of the new record.
            data: The data dictionary containing the new record's data.
            column_names: The list of column names. Defaults to None.
            update_expected_len: Boolean flag to update the expected length of data lists. Defaults to False.

        Returns:
            None
        """
        # Adds a new record to the 'records' dictionary.
        if self.latest_key == 0 or update_expected_len:
            # Set expected len if this is the first entry or update_expected_len = True
            # Sets the len using the first list in the data dict
            self.set_expected_len(len(data[next(iter(data))]))
        else:
            # Check if the list of column names is of the correct length
            # Uses the latest if none were set
            if column_names is None:
                column_names_len = len(self.latest_column_names)
            # If its a string, put it in a list
            # Set len to 1
            elif isinstance(column_names, str):
                column_names = [column_names]
                column_names_len = 1
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
        self,
        record_identifiers: Union[List[Union[int, str]], Union[int, str]],
        use_suffix: bool = False,
        recreate=False,
    ) -> Self:
        """Creates dataframes for the specified records and optionally adds column name suffixes.

        Args:
            record_identifiers: A list of record identifiers (keys or titles) or a single identifier.
            use_suffix: Boolean flag to append the record title as a suffix to column names. Defaults to False.
            recreate: Boolean flag to recreate dataframes from record data even if they already exist. Defaults to False.

        Returns:
            Self: The Chain instance.
        """

        # Type assertions
        assert_and_log_error(
            logger,
            "info",
            isinstance(use_suffix, bool),
            f"Expected bool for use_suffix argument, instead got '{type(use_suffix)}'",
        )

        # Conditionally handling input based on argument type
        # Converts all record identifers to keys
        arg_type = type(record_identifiers)
        if arg_type is int:
            record_identifiers = [record_identifiers]
        elif arg_type is str:
            record_identifiers = [self.title_key(record_identifiers)]
        elif arg_type is list:
            record_identifiers = [
                self.title_key(x) if isinstance(x, str) else x
                for x in record_identifiers
            ]
        else:
            log_and_raise_error(
                logger,
                "info",
                TypeError,
                f"records argument must be a str, int, or list, it was {arg_type}",
            )

        # Looping over selected records and creating the dataframes if necessary
        for record_key in record_identifiers:
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
    def get_combined(self, record_identifiers: list, recreate=False) -> pd.DataFrame:
        """Combines the specified records into a single dataframe.

        Args:
            record_identifiers: A list of record identifiers (keys or titles).
            recreate: Boolean flag to recreate dataframes from record data if they already exist. Defaults to False.

        Returns:
            pd.DataFrame: The combined dataframe.
        """

        # Create dataframes in the selected records
        # Use suffix ensures that all columns are suffixed with the record title, preventing duplicate columns
        self.create_dataframes(record_identifiers, use_suffix=True, recreate=recreate)

        # Create a list of dataframes from the record keys
        dataframes = []
        for record_key in record_identifiers:
            dataframes.append(self.record(record_key)["df"])

        # Using reduce to merge all Dataframes on their indices
        # Skip if there is one record
        if len(dataframes) > 1:
            combined_df = reduce(
                lambda x, y: pd.merge(x, y, left_index=True, right_index=True),
                dataframes,
            )
        else:
            combined_df = dataframes[0]

        return combined_df

    @log_decorator(logger)
    def get_rebase(
        self,
        record_identifiers: Union[List[Union[int, str]], Union[int, str]],
        recreate=False,
    ) -> pd.DataFrame:
        """Combines the specified records and joins them to the base dataframe.

        Args:
            record_identifiers: A list of record identifiers (keys or titles), or a single identifier.
            recreate: Boolean flag to recreate dataframes from record data if they already exist. Defaults to False.

        Returns:
            pd.DataFrame: The rebased dataframe.
        """
        # Does not append anything to the chain and is intended as an easy way to get your final output.

        # Check the arguments
        assert_and_log_is_type_or_list_of(record_identifiers, "records", [str, int])

        # Suffix added to column of non-base table fields if a duplicate exists in the merge
        # Blank by default, and is only set when individual records are specified
        suffix_on_duplicate = ""

        # Access a single df if the argument is str or int
        # Access a single record if the argument is a single item list
        if isinstance(record_identifiers, list) and len(record_identifiers) == 1:
            output_df = copy.deepcopy(self.df(record_identifiers[0]))
            suffix_on_duplicate = self.title(record_identifiers[0])
        # combine the records before rebasing
        # Handles adding suffixes inside get_combined()
        elif isinstance(record_identifiers, list):
            output_df = copy.deepcopy(
                self.get_combined(record_identifiers, recreate=recreate)
            )
        # Otherwise it is a str or int
        else:
            output_df = copy.deepcopy(self.df(record_identifiers))
            suffix_on_duplicate = self.title(record_identifiers)

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
        record_identifiers: Optional[
            Union[List[Union[int, str]], Union[int, str]]
        ] = None,
        output_type: str = "dataframe",
        rebase: bool = True,
        combine: bool = True,
        recreate: bool = False,
    ) -> Union[pd.DataFrame, List[pd.DataFrame], Dict[Union[int, str], pd.DataFrame]]:
        """Retrieves the specified records in the requested output format.

        Args:
            record_identifiers: Optional list of record identifiers (keys or titles). If None, the latest record is used. Defaults to None.
            output_type: The type of output to be returned ('dataframe', 'list', 'nested list', or 'dict'). Defaults to "dataframe".
            rebase: Boolean flag to join the records onto the base dataframe. Defaults to True.
            combine: Boolean flag to combine the records before joining onto the base dataframe. Defaults to True.
            recreate: Boolean flag to recreate dataframes from record data instead of using existing dataframes. Defaults to False.

        Returns:
            Union[pd.DataFrame, List[pd.DataFrame], Dict[Union[int, str], pd.DataFrame]]: The output in the specified format.
        """

        # Check the provided keys
        assert_and_log_is_type_or_list_of(
            record_identifiers, "record_identifiers", [str, int], allow_none=True
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
        if record_identifiers is None:
            record_identifiers = [self.latest_key]
        # If there was only 1 record_key provided as a str or int, put it in a list
        else:
            if not isinstance(record_identifiers, list):
                record_identifiers = [record_identifiers]

        # Always set to combine when output_type is "dataframe" and len is > 1
        if output_type == "dataframe" and len(record_identifiers) > 1:
            combine = True

        # Prepare a list to contain processed titles and dataframes
        title_dataframe_lists = []

        # Then conditionally append to it

        # Append every record individually
        if not combine and not rebase:
            for identifier in record_identifiers:
                identifier_df_pair = [
                    self.title(identifier),
                    copy.deepcopy(self.df(identifier)),
                ]
                title_dataframe_lists.append(identifier_df_pair)

        # Rebase, then append each record individually
        if not combine and rebase:
            for identifier in record_identifiers:
                identifier_df_pair = [
                    self.title(identifier),
                    self.get_rebase(identifier, recreate=recreate),
                ]
                title_dataframe_lists.append(identifier_df_pair)

        # Combine all records, then append
        if combine and not rebase:
            title = (
                self.title(record_identifiers[0])
                if len(record_identifiers) == 1
                else "combined"
            )
            identifier_df_pair = [
                title,
                self.get_combined(record_identifiers, recreate=recreate),
            ]
            title_dataframe_lists.append(identifier_df_pair)

        # Combine all records, then rebase, then append
        if combine and rebase:
            title = (
                self.title(record_identifiers[0])
                if len(record_identifiers) == 1
                else "combined"
            )
            identifier_df_pair = [
                title,
                self.get_rebase(record_identifiers, recreate=recreate),
            ]
            title_dataframe_lists.append(identifier_df_pair)

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
        record_identifiers: Optional[
            Union[List[Union[int, str]], Union[int, str]]
        ] = None,
        rebase: bool = True,
        combine: bool = True,
        xlsx_use_sheets: bool = True,
        recreate: bool = False,
    ) -> Self:
        """Writes the output of the selected records to a file or files.

        Args:
            file_type: The type of file to write the output to. Can be 'csv' or 'xlsx'.
            file_name_prefix: The prefix to be used for the output file name.
            record_identifiers: The identifiers for the records to be included in the output. Can be a single identifier or a list of identifiers.
                                Defaults to None, which means the latest record is used.
            rebase: If True, the output dataframes are joined onto the base dataframe. Defaults to True.
            combine: If True, the records are combined before joining onto the base dataframe or returning. Defaults to True.
            xlsx_use_sheets: If True and file_type is 'xlsx', writes each record to its own sheet in the same file. Defaults to True.
            recreate: If True, the dataframes are recreated from the data in the records instead of using existing dataframes. Defaults to False.

        Returns:
            Self: The Chain object, allowing further chained operations.
        """

        # Sub in reference to the latest record if None was provided
        if record_identifiers is None:
            record_identifiers = [self.latest_key]
        # If there was only 1 record_key provided as a str or int, put it in a list
        else:
            if not isinstance(record_identifiers, list):
                record_identifiers = [record_identifiers]

        # Get the list of [[title, dataframe],] using self.get_output
        title_dataframe_lists = self.get_output(
            record_identifiers,
            output_type="nested list",
            rebase=rebase,
            combine=combine,
            recreate=recreate,
        )

        def make_path(title: str) -> str:
            """
            Generates the file path for a record based on the title.

            Args:
                title: The title of the record.

            Returns:
                str: The generated file path.
            """

            # unction to generate file paths for records.
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
            file_title = (
                self.title(record_identifiers[0])
                if len(record_identifiers) == 1
                else ""
            )
            path = make_path(file_title)

            # Writing each record to its own sheet in the same xlsx file
            with pd.ExcelWriter(path) as writer:
                for title, df in title_dataframe_lists:
                    invalid_chars = r'[\/:*?"<>|]'
                    sheet_title = re.sub(invalid_chars, "", title)
                    df.to_excel(writer, sheet_name=sheet_title, index=False)

        return self
