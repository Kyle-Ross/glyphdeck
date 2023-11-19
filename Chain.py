from custom_types import Record, Records, Data
from datetime import datetime, timedelta
from icecream import ic
from time import sleep


class Chain:
    def __init__(self):
        """Common object for storing and passing the chained results of data processing"""
        self.records: Records = {
            0: {
                'title': 'initialisation',
                'dt': datetime.now(),
                'delta': None,
                'data': {}
            }
        }

    def record(self, key: int) -> Record:
        """Returns the record corresponding to the provided key number"""
        return self.records[key]

    def title(self, key: int) -> str:
        """Returns the title corresponding to the provided key number"""
        return self.record(key)['title']

    def dt(self, key: int) -> datetime:
        """Returns the datetime corresponding to the provided key number"""
        return self.record(key)['dt']

    def data(self, key: int) -> Data:
        """Returns the data dictionary corresponding to the provided key number"""
        return self.record(key)['data']

    def record_delta(self, key: int) -> timedelta:
        """Returns the timedelta corresponding to the provided key number"""
        return self.record(key)['delta']

    @property
    def latest_key(self) -> int:
        """Returns the max key from 'records'"""
        return max(self.records.keys())

    @property
    def latest_record(self) -> Record:
        """Returns the latest 'record' from 'records'"""
        return self.record(self.latest_key)

    @property
    def latest_title(self) -> str:
        """Returns the latest 'title' from 'records'"""
        return self.title(self.latest_key)

    @property
    def latest_dt(self) -> datetime:
        """Returns the latest 'dt' from 'records'"""
        return self.dt(self.latest_key)

    @property
    def latest_data(self) -> Data:
        """Returns the latest 'data' from the latest 'record' in 'records'"""
        return self.data(self.latest_key)

    @property
    def initial_key(self) -> int:
        """Returns 1, but only if key 1 exists in the records"""
        if 1 not in self.records:
            raise KeyError("Initial record does not exist yet! Use self.append to get started.")
        return 1

    @property
    def initial_record(self) -> Record:
        """Returns the initial 'record' from 'records'"""
        return self.record(self.initial_key)

    @property
    def initial_title(self) -> str:
        """Returns the initial 'title' from 'records'"""
        return self.title(self.initial_key)

    @property
    def initial_dt(self) -> datetime:
        """Returns the initial 'dt' from 'records'"""
        return self.dt(self.initial_key)

    @property
    def initial_data(self) -> Data:
        """Returns the initial 'data' from the latest 'record' in 'records'"""
        return self.data(self.initial_key)

    @property
    def delta(self) -> timedelta:
        """Returns the overall timedelta"""
        return self.latest_dt - self.dt(0)

    def key_validator(self, target_record: int):
        """Validates that records have identical keys, and no new or missing keys."""
        def key_list(key) -> list:
            return [x for x, y in dict.items(self.data(key))]
        initial_key_list: list = key_list(self.initial_key)
        target_key_list = key_list(target_record)
        initial_not_target = [x for x in initial_key_list if x not in target_key_list]
        initial_not_target_len = len(initial_not_target)
        target_not_initial = [x for x in target_key_list if x not in initial_key_list]
        target_not_initial_len = len(target_not_initial)
        total_differences = initial_not_target_len + target_not_initial_len

        if total_differences > 0:
            if initial_not_target_len > 0:
                print("")
                print(f"{initial_not_target_len} keys are in the initial record, but not in the record {target_record} "
                      f"({self.title(target_record)}).")
                print("These were records:")
                for x in initial_not_target:
                    print(f"{x}: {self.initial_data[x]}")
            if target_not_initial_len > 0:
                print("")
                print(f"{target_not_initial_len} keys from the appended record {target_record} "
                      f"({self.title(target_record)}) were not in the initial record.")
                print("These were records:")
                for x in target_not_initial:
                    print(f"{x}: {self.data(target_record)[x]}")
            print("")
            print(f"Records cannot be missing keys or add new keys that are not already in the initial record.")
            raise ValueError()

    def append(self,
               title: str,
               data: Data
               ):
        """Adds a new record to the records dictionary"""
        now: datetime = datetime.now()
        delta: timedelta = now - self.latest_dt
        new_key = self.latest_key + 1
        self.records[new_key] = {
            'title': title,
            'dt': now,
            'delta': delta,
            'data': data
        }
        self.key_validator(new_key)
        return self


if __name__ == "__main__":
    """Only runs below if script is run directly, not on import"""
    chain = Chain()
    sleep(0.75)
    chain.append('Example1', {1: 'potato', 2: 'carrot', 3: 'banana'})
    sleep(0.5)
    chain.append('Example2', {1: 'potatoes', 2: 'carrots', 3: 'bananas'})
    sleep(0.6)
    chain.append('Example3', {1: 'potato cake', 2: 'carrot cake', 3: 'banana sundae'})
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
