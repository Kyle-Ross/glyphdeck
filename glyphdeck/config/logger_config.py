from typing import Optional, Tuple
import oyaml as yaml
import copy

# Path to store and access logger config yaml file from
_config_path = r"glyphdeck\config\logger_config.yaml"


class LoggerConfig:
    """Context manager for modifying a YAML configuration file.
    Opens the config, makes your changes, and then saves it.
    """

    def __init__(self):
        """Initializes the LoggerConfigManager with the path to the YAML config file."""
        self.path = _config_path

    def __enter__(self):
        """Loads the YAML data from the file, store it in self.data and returns self

        Returns:
            LoggerConfig: The LoggerConfig instance.
        """
        with open(self.path, "r") as f:
            self.data = yaml.safe_load(f)
            self.original_data = copy.deepcopy(self.data)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Writes the modified data back to the file."""
        # Only write if changes occurred
        if self.data != self.original_data:
            with open(self.path, "w") as f:
                yaml.dump(self.data, f)


def logging_config(
    # Bool toggles for private data logging
    log_input_data: Optional[bool] = None,
    log_output_data: Optional[bool] = None,
    # Parent level setting directing logger level references
    setting_type: Optional[str] = None,
    # Used if setting_type == "set_all"
    set_all_levels: Optional[Tuple[int]] = None,
    # Used if setting_type == "granular"
    data_types_levels: Optional[Tuple[int]] = None,
    prepper_levels: Optional[Tuple[int]] = None,
    cascade_levels: Optional[Tuple[int]] = None,
    sanitiser_levels: Optional[Tuple[int]] = None,
    validators_levels: Optional[Tuple[int]] = None,
    llm_handler_levels: Optional[Tuple[int]] = None,
    cache_levels: Optional[Tuple[int]] = None,
    workflows_levels: Optional[Tuple[int]] = None,
    strings_levels: Optional[Tuple[int]] = None,
    time_levels: Optional[Tuple[int]] = None,
    file_importers_levels: Optional[Tuple[int]] = None,
    unhandled_errors_levels: Optional[Tuple[int]] = None,
):
    """Configure the logging settings by changing the configuration yaml file.

    This function allows you to modify various logging settings such as whether to log input and output data,
    the source of logging levels, and the logging levels for different components of the module.

    Changes here affect the module globally for current and future use.

    Privacy:
    --------
    - log_input_data: Optional[bool]
    - log_output_data: Optional[bool]

    Setting_type:
    ----------------
    Can be "default", "set_all" or "granular"
    - setting_type: Optional[str]

    If "default":
    -----------------------------
    Uses the default values:
    - file = 90 (no logs)
    - console = 20 (info)
    \n
    For all loggers.

    If "set_all":
    -----------------------------
    (file_level, console_level)
    - set_all_levels: Optional[Tuple[int]]

    If "Granular":
    ------------------------------
    (file_level, console_level)
    - data_types_levels: Optional[Tuple[int]]
    - prepper_levels: Optional[Tuple[int]]
    - cascade_levels: Optional[Tuple[int]]
    - sanitiser_levels: Optional[Tuple[int]]
    - validators_levels: Optional[Tuple[int]]
    - llm_handler_levels: Optional[Tuple[int]]
    - cache_levels: Optional[Tuple[int]]
    - workflows_levels: Optional[Tuple[int]]
    - strings_levels: Optional[Tuple[int]]
    - time_levels: Optional[Tuple[int]]
    - file_importers_levels: Optional[Tuple[int]]
    - unhandled_errors_levels: Optional[Tuple[int]]
    """

    def _set_levels(d: dict, key: str, levels: Tuple[int]):
        """Set the logging levels for a given key in a dictionary representing an opened yaml file.

        Args:
            d (dict): The dictionary to modify.
            key (str): The key in the dictionary to modify.
            levels (Tuple[int]): A tuple of two integers representing the logging levels for the file and console.
        """
        # Assert provided arguments
        assert isinstance(d, dict), f"d is type '{type(d)}', expected dict"
        assert isinstance(key, str), f"key is type '{type(key)}', expected str"
        assert key in d, f"Key '{key}' does not exist in the logger config"
        assert isinstance(
            levels, tuple
        ), f"levels is type '{type(levels)}', expected tuple"
        for level in levels:
            assert isinstance(
                level, int
            ), f"value '{level}' in levels is type '{type(level)}', expected int"
        # Set the levels to the dictionary
        d[key]["file"] = levels[0]
        d[key]["console"] = levels[1]

    # Use the context manager to address the yaml changes
    with LoggerConfig() as config:
        # Context manager opens the config yaml as a dict called config
        # Me make changes to that dict

        if log_input_data is not None:
            assert isinstance(
                log_input_data, bool
            ), f"log_input_data is type '{type(log_input_data)}', expected bool"
            config.data["private_data"]["log_input"] = log_input_data

        if log_output_data is not None:
            assert isinstance(
                log_output_data, bool
            ), f"log_input_data is type '{type(log_output_data)}', expected bool"
            config.data["private_data"]["log_output"] = log_output_data

        if setting_type is not None:
            assert isinstance(
                setting_type, str
            ), f"level_source is type '{type(setting_type)}', expected str"
            allowed_sources = ("default", "set_all", "granular")
            assert (
                setting_type in allowed_sources
            ), f"level_source '{setting_type}' is not in allowed sources '{allowed_sources}'"
            config.data["source"] = setting_type

        if set_all_levels is not None:
            _set_levels(config.data, "set_all", set_all_levels)

        if data_types_levels is not None:
            _set_levels(config.data, "data_types", data_types_levels)

        if prepper_levels is not None:
            _set_levels(config.data, "prepper", prepper_levels)

        if cascade_levels is not None:
            _set_levels(config.data, "cascade", cascade_levels)

        if sanitiser_levels is not None:
            _set_levels(config.data, "sanitiser", sanitiser_levels)

        if validators_levels is not None:
            _set_levels(config.data, "validators", validators_levels)

        if llm_handler_levels is not None:
            _set_levels(config.data, "llm_handler", llm_handler_levels)

        if cache_levels is not None:
            _set_levels(config.data, "cache", cache_levels)

        if workflows_levels is not None:
            _set_levels(config.data, "workflows", workflows_levels)

        if strings_levels is not None:
            _set_levels(config.data, "strings", strings_levels)

        if time_levels is not None:
            _set_levels(config.data, "time", time_levels)

        if file_importers_levels is not None:
            _set_levels(config.data, "file_importers", file_importers_levels)

        if unhandled_errors_levels is not None:
            _set_levels(config.data, "unhandled_errors", unhandled_errors_levels)

        # Context manager exits and writes the changes to config back to the yaml file


def restore_logger_config():
    f"""This function restores the logger configuration yaml file '{_config_path}' in its original state.
    Useful if your changes have broken the file, but beware as this will overwrite any modifications
    in the config.
    """

    config = {
        "private_data": {"log_input": False, "log_output": False, "source": "default"},
        "set_all": {"file": 10, "console": 10},
        "data_types": {"file": 10, "console": 10},
        "prepper": {"file": 10, "console": 10},
        "cascade": {"file": 10, "console": 10},
        "sanitiser": {"file": 10, "console": 10},
        "validators": {"file": 10, "console": 10},
        "llm_handler": {"file": 10, "console": 10},
        "cache": {"file": 10, "console": 10},
        "workflows": {"file": 10, "console": 10},
        "strings": {"file": 10, "console": 10},
        "time": {"file": 10, "console": 10},
        "file_importers": {"file": 10, "console": 10},
        "unhandled_errors": {"file": 10, "console": 10},
    }

    with open(_config_path, "w") as file:
        yaml.dump(config, file)


# Only run in here, for testing
if __name__ == "__main__":
    logging_config(
        log_input_data=False, setting_type="default", strings_levels=(10, 10)
    )
