from datetime import datetime, timedelta
from functools import reduce
import re
import os

import pandas as pd

from CategoriGen.validation.data_types import (Record, Records, Data, IntStr, dFrame, dFrame_or_None, IntList,
                                               StrList, StrList_or_None, List_or_Str, IntStrNone, RecordList)
from CategoriGen.tools.loggers import ChainLogger, assert_and_log_error, log_and_raise_error, log_decorator
from CategoriGen.tools.directory_creators import create_files_directory
from CategoriGen.path_constants import OUTPUT_FILES_DIR

logger = ChainLogger().setup()


class Chain:
    @log_decorator(logger, start="Initialising Chain object", finish="Initialised Chain object")
    def __init__(self):
        """Common object for storing and passing the chained results of data processing."""
        self.expected_len = 0
        self.records: Records = {
            0: {
                'title': 'initialisation',
                'dt': datetime.now(),
                'delta': None,
                'data': {},
                'table': None,
                'table_id_column': None,
                'column_names': None  # Names of the columns, in order
            }
        }
        logger.debug("Function - __init__() - Finish - Initialised Chain object")

    @log_decorator(logger)
    def title_key(self, title: str) -> int:
        """Returns the record number for a given title"""
        for record_num, record_dict in self.records.items():
            try:
                if record_dict['title'] == title:
                    return record_num
            except TypeError as error:
                log_and_raise_error(logger, 'error', type(error), f"Provided title 'f{title}' does not exist.")

    @log_decorator(logger)
    def record(self, record_identifier: IntStr) -> Record:
        """Returns the record corresponding to the provided record number or record title."""
        key_type = type(record_identifier)
        if key_type == int:
            return self.records[record_identifier]
        if key_type == str:
            return self.records[self.title_key(record_identifier)]

    @log_decorator(logger)
    def title(self, key: IntStr) -> str:
        """Returns the title corresponding to the provided record_identifier number."""
        return self.record(key)['title']

    @log_decorator(logger)
    def dt(self, key: IntStr) -> datetime:
        """Returns the datetime corresponding to the provided record_identifier number."""
        return self.record(key)['dt']

    @log_decorator(logger)
    def data(self, key: IntStr) -> Data:
        """Returns the data dictionary corresponding to the provided record_identifier number."""
        return self.record(key)['data']

    @log_decorator(logger)
    def table(self, key: IntStr) -> dFrame:
        """Returns the table corresponding to the provided record_identifier number."""
        return self.record(key)['table']

    @log_decorator(logger)
    def table_id_column(self, key: IntStr) -> IntStrNone:
        """Returns the table corresponding to the provided record_identifier number."""
        return self.record(key)['table_id_column']

    @log_decorator(logger)
    def record_delta(self, key: IntStr) -> timedelta:
        """Returns the timedelta corresponding to the provided record_identifier number."""
        return self.record(key)['delta']

    @log_decorator(logger)
    def column_names(self, key: IntStr) -> StrList:
        """Returns the list of column names corresponding to the provided record_identifier number."""
        return self.record(key)['column_names']

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
    def latest_table(self) -> dFrame_or_None:
        """Returns the latest 'table' from the latest 'record' in 'records'."""
        return self.table(self.latest_key)

    @property
    @log_decorator(logger, is_property=True)
    def latest_table_id_column(self) -> IntStrNone:
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
            log_and_raise_error(logger, 'error', KeyError,
                                "KeyError: Initial record does not exist yet! Use self.append to get started.")
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
        """Returns the initial 'data' from the latest 'record' in 'records'."""
        return self.data(self.initial_key)

    @property
    @log_decorator(logger, is_property=True)
    def initial_table(self) -> dFrame_or_None:
        """Returns the initial 'table' from the latest 'record' in 'records'."""
        return self.table(self.initial_key)

    @property
    @log_decorator(logger, is_property=True)
    def initial_table_id_column(self) -> IntStrNone:
        """Returns the initial 'table_id_column' from the latest 'record' in 'records'."""
        return self.table_id_column(self.initial_key)

    @property
    @log_decorator(logger, is_property=True)
    def initial_column_names(self) -> StrList:
        """Returns the initial 'column_names' from the latest 'record' in 'records'."""
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
                key_validator_message = f"{initial_not_target_len} keys were in the initial record {self.initial_key} " \
                                        f"'{initial_title}" \
                                        f"', but not in the appended record {target_key} '{target_title}'. " \
                                        f"These were the following keys: {initial_not_target}"
            if target_not_initial_len > 0:
                key_validator_message = f"{target_not_initial_len} keys were in the appended record {target_key} " \
                                        f"'{target_title}" \
                                        f"', but not in the initial record {self.initial_key} '{initial_title}'. " \
                                        f"These were the following keys: {target_not_initial}"
            key_validator_message += " | Records cannot be missing keys or add new keys that are not " \
                                     "already in the initial record."
            log_and_raise_error(logger, 'error', KeyError, key_validator_message)

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
        data_validator_message = ''
        if good_len == 0:
            data_validator_message = f"All keys in record '{target_title}' " \
                                     f"contained lists of unexpected length / column count, " \
                                     f"where expected length was '{self.expected_len}.'"
        elif bad_len != 0:
            data_validator_message = f"Some keys in record '{target_title}' " \
                                     f"contained lists of unexpected length / column count, " \
                                     f"where expected length was '{self.expected_len}'. " \
                                     f"These keys were: {bad_keys}"
        if bad_len != 0:
            log_and_raise_error(logger, 'error', ValueError, data_validator_message)

    @log_decorator(logger)
    def append(self,
               title: str,
               data: Data,
               table: dFrame_or_None = None,
               table_id_column: IntStrNone = None,
               column_names: StrList_or_None = None,
               update_expected_len: bool = False
               ):
        """Adds a new record to the 'records' dictionary."""
        if self.latest_key == 0 or update_expected_len:
            # Set expected len if this is the first entry or update_expected_len = True
            # Sets the len of the first list in the data dict
            self.set_expected_len(len(data[next(iter(data))]))
        else:
            # Check if the list of column names is of the correct length
            if column_names is None:
                column_names_len = len(self.latest_column_names)  # Uses the latest if none were set
            else:
                column_names_len = len(column_names)  # Otherwise, get the length from the provided list
            assert_and_log_error(logger, 'error', column_names_len == self.expected_len,
                                 f"{self.expected_len} columns expected, but 'column_names' contains "
                                 f"{column_names_len} entries. If this is expected, set "
                                 f"self.append(update_expected_len=True), otherwise review your data.")

        now: datetime = datetime.now()
        delta: timedelta = now - self.latest_dt
        new_key = self.latest_key + 1
        self.records[new_key] = {
            'title': title,
            'dt': now,
            'delta': delta,
            'data': data,
            'table': self.latest_table if table is None else table,  # References previous values if none
            'table_id_column': self.latest_table_id_column if table_id_column is None else table_id_column,
            'column_names': self.latest_column_names if column_names is None else column_names
        }
        self.key_validator(new_key)
        self.data_validator(new_key)
        return self

    @log_decorator(logger)
    def selector(self, records: List_or_Str, use_suffix: bool) -> RecordList:
        """Returns a list of clean DataFrames from the selected records. Adds column names back on."""
        # Handling str input
        if type(records) == str:
            records = [records]
        # Add only selected records to a list
        selected_records = [self.record(record) for record in records]

        # Looping over selected records and making changes
        for record in selected_records:
            # Creating dataframes and in each of the records
            df = pd.DataFrame.from_dict(record['data'], orient='index')  # Treats the index as the row_id
            # Renaming columns
            if use_suffix:  # Includes suffixes when there are multiple selections to avoid concatenation errors
                df.columns = [name + "_" + record['title'] for name in record['column_names']]
            else:
                df.columns = record['column_names']
            # Record the new df
            record['output_df'] = df

        # Returning the selected records
        return selected_records

    @log_decorator(logger)
    def combiner(self, records: list) -> RecordList:
        """Combines records into a single dataframe."""
        # Grab the dataframes, combining them, and returning them
        selected_records = self.selector(records, use_suffix=True)
        selected_dfs = [record['output_df'] for record in selected_records]
        # Using reduce to merge all DataFrames in selected_dfs on their indices
        combined_df = reduce(lambda x, y: pd.merge(x, y, left_index=True, right_index=True), selected_dfs)
        # Building into the record format to be used in the flow with the rest
        combined_record: RecordList = [{
            'title': 'combined',
            'dt': datetime.now(),
            'delta': datetime.now() - self.latest_dt,
            'data': {},
            'table': selected_records[0],
            'output_df': combined_df,
            'table_id_column': self.initial_table_id_column,  # Not really needed
            'column_names': None  # Not needed here - all different anyway
        }]
        return combined_record

    @log_decorator(logger)
    def output(self,
               records: List_or_Str,
               file_type: str,
               name_prefix: str,
               rejoin: bool = True,
               split: bool = False):
        # Checking file_type is in allowed list
        allowed_file_types = ['csv', 'xlsx']
        assert_and_log_error(logger, 'error', file_type in allowed_file_types,
                             f"'{file_type}' is not in allowed list {allowed_file_types}.")
        # Use the separate or combined records
        if split:
            records_list: RecordList = self.selector(records, use_suffix=False)
        else:
            records_list: RecordList = self.combiner(records)
        # Use the dataframes as is, or left join each back onto the initial source
        if rejoin:
            for record in records_list:
                record['output_df'] = self.initial_table.merge(record['output_df'],
                                                               how='left',
                                                               left_on=self.initial_table_id_column,
                                                               right_index=True)

        # Final changes to the output_df
        for record in records_list:
            df = record['output_df']
            # Insert the index as a col at 0, if it doesn't already exist (i.e. you are rejoining)
            if record['table_id_column'] not in df.columns:
                df.insert(0, record['table_id_column'], df.index)
            df.sort_values(record['table_id_column'])  # Sort by the id column ascending
            record['output_df'] = df

        def make_path(source_record: Record) -> str:
            """Function to generate file paths for records."""
            formatted_time = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
            # Conditionally setting the title to be used in the logger_name
            if file_type == 'csv' and split:
                title = source_record['title']  # Each individual file has the title logger_name
            elif file_type == 'xlsx' and split:
                title = "split"  # The containing excel file has 'split'
            elif split:
                title = "split"  # Shouldn't be possible but just in case
            else:
                title = "combined"

            file_name = f"{name_prefix} - {title} - {formatted_time}.{file_type}"
            file_path = os.path.join(OUTPUT_FILES_DIR, file_name)
            return file_path

        # Create the files directory if it doesn't exist
        create_files_directory(logger)

        # Output the dataframes
        if file_type == 'csv':
            for record in records_list:
                df = record['output_df']
                path = make_path(record)
                df.to_csv(path, index=False)

        if file_type == 'xlsx':
            path = make_path(self.latest_record)  # Argument may not be used in certain conditions
            with pd.ExcelWriter(path) as writer:
                for record in records_list:  # Writing each record to its own sheet in the same xlsx file
                    sheet_df = record['output_df']
                    invalid_chars = r'[\/:*?"<>|]'
                    sheet_title = re.sub(invalid_chars, '', record['title'])
                    sheet_df.to_excel(writer, sheet_name=sheet_title, index=False)


if __name__ == "__main__":
    """Only runs below if script is run directly, not on import."""
    from time import sleep

    test_data = {1: ['door', 'champ', 'slam'],
                 2: ['blam', 'clam', 'sam'],
                 3: ['tim', 'tam', 'fam']}

    # Create the DataFrame
    test_df = pd.DataFrame.from_dict(test_data, orient='index')
    test_df = test_df.reset_index()  # Adds the index as a column
    test_df.columns = ['Word ID', 'Word1', 'Word2', 'Word3']  # Rename cols

    chain = Chain()
    sleep(0.75)
    # The table only needs to be added in the first step, but can be included again to add a new one
    # Otherwise the table will be the last time 'table' was assigned
    chain.append(
        title='Example1',
        data={1: ['potato', 'steak', 'party'],
              2: ['carrot', 'party', 'alpha'],
              3: ['carrot', 'party', 'alpha']},
        table=test_df,
        table_id_column="Word ID",
        column_names=['Food1', 'Food2', 'Food3']
    )
    sleep(0.5)
    # Since table is not assigned table will just be the last table
    chain.append(
        title='Example2',
        data={1: ['potatoes', 'carrot', 'gary'],
              2: ['carrots', 'pizza', 'pasta'],
              3: ['bananas', 'beast', 'jeffery']}
    )
    sleep(0.6)
    chain.append(
        title='Example3',
        data={1: ['keys', 'mud', 'salt'],
              2: ['carrot cake', 'car', 'bike'],
              3: ['banana sundae', 'icecream', 'intel']},
        table_id_column="Word ID2",
        column_names=['Food 1', 'Food 2', 'Food 3']
    )
    sleep(0.2)
    print("chain.records")
    print(chain.records)
    print("chain.initial_key")
    print(chain.initial_key)
    print("chain.initial_record")
    print(chain.initial_record)
    print("chain.initial_title")
    print(chain.initial_title)
    print("chain.initial_dt")
    print(chain.initial_dt)
    print("chain.initial_data")
    print(chain.initial_data)
    print("chain.latest_key")
    print(chain.latest_key)
    print("chain.latest_record")
    print(chain.latest_record)
    print("chain.latest_title")
    print(chain.latest_title)
    print("chain.latest_dt")
    print(chain.latest_dt)
    print("chain.latest_data")
    print(chain.latest_data)
    print("chain.latest_column_names")
    print(chain.latest_column_names)
    print("chain.title(2)")
    print(chain.title(2))
    print("chain.record_delta(1)")
    print(chain.record_delta(1))
    print("chain.record_delta(2)")
    print(chain.record_delta(2))
    print("chain.record_delta(3)")
    print(chain.record_delta(3))
    print("chain.delta")
    print(chain.delta)
    print("chain.record(2)")
    print(chain.record(2))
    print("chain.record("'Example2'")")
    print(chain.record('Example2'))
    print("chain.data("'Example2'")")
    print(chain.data('Example2'))
    print("chain.title_key("'Example2'")")
    print(chain.title_key('Example2'))
    print("chain.initial_table")
    print(chain.initial_table)
    print("chain.initial_table_id_column")
    print(chain.initial_table_id_column)
    chain.output(
        records=['Example1', 'Example2', 'Example3'],
        file_type='xlsx',
        name_prefix='Chain Test',
        rejoin=True,
        split=False)
