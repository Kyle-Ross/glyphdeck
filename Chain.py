from custom_types import Record, Records, Data, IntStr, dFrame, dFrame_or_None, IntList
from datetime import datetime, timedelta


class Chain:
    def __init__(self):
        """Common object for storing and passing the chained results of data processing."""
        self.expected_len = 0
        self.records: Records = {
            0: {
                'title': 'initialisation',
                'dt': datetime.now(),
                'delta': None,
                'data': {},
                'table': None
            }
        }

    def title_key(self, title: str) -> int:
        """Returns the record number for a given title"""
        for record_num, record_dict in self.records.items():
            try:
                if record_dict['title'] == title:
                    return record_num
            except TypeError:
                print(f"Provided title 'f{title}' does not exist.")

    def record(self, record_identifier: IntStr) -> Record:
        """Returns the record corresponding to the provided record number or record title."""
        key_type = type(record_identifier)
        if key_type == int:
            return self.records[record_identifier]
        if key_type == str:
            return self.records[self.title_key(record_identifier)]

    def title(self, key: IntStr) -> str:
        """Returns the title corresponding to the provided record_identifier number."""
        return self.record(key)['title']

    def dt(self, key: IntStr) -> datetime:
        """Returns the datetime corresponding to the provided record_identifier number."""
        return self.record(key)['dt']

    def data(self, key: IntStr) -> Data:
        """Returns the data dictionary corresponding to the provided record_identifier number."""
        return self.record(key)['data']

    def table(self, key: IntStr) -> dFrame:
        """Returns the data dictionary corresponding to the provided record_identifier number."""
        return self.record(key)['table']

    def record_delta(self, key: IntStr) -> timedelta:
        """Returns the timedelta corresponding to the provided record_identifier number."""
        return self.record(key)['delta']

    @property
    def latest_key(self) -> int:
        """Returns the max record_identifier from 'records'."""
        return max(self.records.keys())

    @property
    def latest_record(self) -> Record:
        """Returns the latest 'record' from 'records'."""
        return self.record(self.latest_key)

    @property
    def latest_title(self) -> str:
        """Returns the latest 'title' from 'records'."""
        return self.title(self.latest_key)

    @property
    def latest_dt(self) -> datetime:
        """Returns the latest 'dt' from 'records'."""
        return self.dt(self.latest_key)

    @property
    def latest_data(self) -> Data:
        """Returns the latest 'data' from the latest 'record' in 'records'."""
        return self.data(self.latest_key)

    @property
    def latest_table(self) -> dFrame_or_None:
        """Returns the latest 'table' from the latest 'record' in 'records'."""
        return self.table(self.latest_key)

    @property
    def initial_key(self) -> int:
        """Returns 1, but only if record_identifier 1 exists in the records."""
        if 1 not in self.records:
            raise KeyError("Initial record does not exist yet! Use self.append to get started.")
        return 1

    @property
    def initial_record(self) -> Record:
        """Returns the initial 'record' from 'records'."""
        return self.record(self.initial_key)

    @property
    def initial_title(self) -> str:
        """Returns the initial 'title' from 'records'."""
        return self.title(self.initial_key)

    @property
    def initial_dt(self) -> datetime:
        """Returns the initial 'dt' from 'records'."""
        return self.dt(self.initial_key)

    @property
    def initial_data(self) -> Data:
        """Returns the initial 'data' from the latest 'record' in 'records'."""
        return self.data(self.initial_key)

    @property
    def initial_table(self) -> dFrame_or_None:
        """Returns the initial 'table' from the latest 'record' in 'records'."""
        return self.table(self.initial_key)

    @property
    def delta(self) -> timedelta:
        """Returns the overall timedelta."""
        return self.latest_dt - self.dt(0)

    def set_expected_len(self, value: int):
        """Updates the number of values expected for each list in the records data"""
        self.expected_len = value
        return self

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
                key_validator_message = f"\n" \
                                        f"{initial_not_target_len} keys were in the initial record {self.initial_key} " \
                                        f"'{initial_title}" \
                                        f"', but not in the appended record {target_key} '{target_title}'.\n" \
                                        f"These were the following keys:\n" \
                                        f"{initial_not_target}"
            if target_not_initial_len > 0:
                key_validator_message = f"\n" \
                                        f"{target_not_initial_len} keys were in the appended record {target_key} " \
                                        f"'{target_title}" \
                                        f"', but not in the initial record {self.initial_key} '{initial_title}'.\n" \
                                        f"These were the following keys:\n" \
                                        f"{target_not_initial}"
            key_validator_message += "\n\nRecords cannot be missing keys or add new keys that are not " \
                                     "already in the initial record."
            raise ValueError(key_validator_message)

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
                                      f"where expected length was '{self.expected_len}'.\n\n" \
                                      f"These keys were: \n" \
                                      f"{bad_keys}"
        if bad_len != 0:
            raise ValueError(data_validator_message)

    def append(self,
               title: str,
               data: Data,
               table: dFrame_or_None = None,
               update_expected_len: bool = False
               ):
        """Adds a new record to the 'records' dictionary."""
        if self.latest_key == 0:
            # Set expected len if this is the first entry, using the len of the first list in the data dict
            self.set_expected_len(len(data[next(iter(data))]))
        if update_expected_len:
            # Set expected len if update_expected_len is True
            # Uses the len of the first data list from the most recent record
            self.set_expected_len(len(self.latest_data[next(iter(self.latest_data))]))

        now: datetime = datetime.now()
        delta: timedelta = now - self.latest_dt
        new_key = self.latest_key + 1
        self.records[new_key] = {
            'title': title,
            'dt': now,
            'delta': delta,
            'data': data,
            'table': self.latest_table if table is None else table  # References previous table if no table provided
        }
        self.key_validator(new_key)
        self.data_validator(new_key)
        return self


if __name__ == "__main__":
    """Only runs below if script is run directly, not on import."""
    from icecream import ic
    from time import sleep
    import pandas as pd

    test_data = {
        'A': [1, 2, 3, 4, 5],
        'B': ['a', 'b', 'c', 'd', 'e'],
        'C': [1.1, 2.2, 3.3, 4.4, 5.5]
    }

    # Create the DataFrame
    test_df = pd.DataFrame(test_data)

    chain = Chain()
    sleep(0.75)
    # The table only needs to be added in the first step, but can be included again to add a new one
    # Otherwise the table will be the last time 'table' was assigned
    chain.append(
        'Example1',
        {1: ['potato', 'steak', 'party'], 2: ['carrot', 'party', 'alpha'], 3: ['carrot', 'party', 'alpha']},
        test_df
    )
    sleep(0.5)
    # Since table is not assigned table will just be the last table
    chain.append(
        'Example2',
        {1: ['potatoes', 'carrot', 'gary'], 2: ['carrots', 'pizza', 'pasta'], 3: ['bananas', 'beast', 'jeffery']}
    )
    sleep(0.6)
    chain.append(
        'Example3',
        {1: ['keys', 'mud', 'salt'], 2: ['carrot cake', 'car', 'bike'], 3: ['banana sundae', 'icecream', 'intel']}
    )
    sleep(0.2)
    ic(chain.records)
    ic(chain.initial_key)
    ic(chain.initial_record)
    ic(chain.initial_title)
    ic(chain.initial_dt)
    ic(chain.initial_data)
    ic(chain.latest_key)
    ic(chain.latest_record)
    ic(chain.latest_title)
    ic(chain.latest_dt)
    ic(chain.latest_data)
    ic(chain.title(2))
    ic(chain.record_delta(1))
    ic(chain.record_delta(2))
    ic(chain.record_delta(3))
    ic(chain.delta)
    ic(chain.record(2))
    ic(chain.record('Example2'))
    ic(chain.data('Example2'))
    ic(chain.title_key('Example2'))
