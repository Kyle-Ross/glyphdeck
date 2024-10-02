import oyaml as yaml
from typing import Optional, Tuple

class LoggerConfig:
    """Context manager for modifying a YAML configuration file.
    Opens the config, makes your changes, and then saves it.
    """

    def __init__(self):
        """Initializes the LoggerConfigManager with the path to the YAML config file."""
        self.path = r"glyphdeck\config\logger_config.yaml"

    def __enter__(self):
        """Loads the YAML data from the file, store it in self.data and returns self

        Returns:
            LoggerConfig: The LoggerConfig instance.
        """
        with open(self.path, "r") as f:
            self.data = yaml.safe_load(f)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Writes the modified data back to the file."""
        with open(self.path, "w") as f:
            yaml.dump(self.data, f)


def set_logging_config(
        # Bool toggles for private data logging
        log_input_data: Optional[bool] = None,
        log_output_data: Optional[bool] = None,
        # Parent level setting directing logger level references
        level_source: Optional[str] = None,
        # Used if level_source == "set_all"
        set_all_levels: Optional[Tuple[int]] = None,
        # Used if level_source == "granular"
        data_types_levels: Optional[Tuple[int]] = None,
        prepper_levels: Optional[Tuple[int]] = None,
        cascade_levels: Optional[Tuple[int]] = None,
        sanitiser_levels: Optional[Tuple[int]] = None,
        validators_levels: Optional[Tuple[int]] = None,
        llm_handler_levels: Optional[Tuple[int]] = None,
        cache_levels: Optional[Tuple[int]] = None,
        workflows_levels: Optional[Tuple[int]] = None,
        unhandled_levels: Optional[Tuple[int]] = None,
        strings_levels: Optional[Tuple[int]] = None,
        time_levels: Optional[Tuple[int]] = None,
        file_importers_levels: Optional[Tuple[int]] = None,
    ):

    def _set_levels(d: dict, key: str, levels: Tuple[int]):
        """Set the logging levels for a given key in a dictionary.

            Args:
                d (dict): The dictionary to modify.
                key (str): The key in the dictionary to modify.
                levels (Tuple[int]): A tuple of two integers representing the logging levels for the file and console.
        """
        # Assert provided arguments
        assert isinstance(d, dict), f"d is type '{type(d)}', expected dict"
        assert isinstance(key, str), f"key is type '{type(key)}', expected str"
        assert key in d, f"Key '{key}' does not exist in the logger config"
        assert isinstance(levels, tuple), f"levels is type '{type(levels)}', expected tuple"
        for level in levels:
            assert isinstance(level, int), f"value in levels is type '{type(level)}', expected int"
        # Set the levels to the dictionary
        print(levels)
        d[key]["file"] = levels[0]
        d[key]["console"] = levels[1]


    # Use the context manager to address the yaml changes
    with LoggerConfig() as config:
        # Context manager opens the config yaml as a dict called config
        # Me make changes to that dict
        
        if log_input_data is not None:
            assert isinstance(log_input_data, bool), f"log_input_data is type '{type(log_input_data)}', expected bool"
            config.data["private_data"]["log_input"] = log_input_data
        
        if log_output_data is not None:
            assert isinstance(log_output_data, bool), f"log_input_data is type '{type(log_output_data)}', expected bool"
            config.data["private_data"]["log_output"] = log_output_data
        
        if level_source is not None:
            assert isinstance(level_source, str), f"level_source is type '{type(level_source)}', expected str"
            allowed_sources = ("default", "set_all", "granular")
            assert level_source in allowed_sources, f"level_source '{level_source}' is not in allowed sources '{allowed_sources}'"
            config.data["source"] = level_source

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

        if unhandled_levels is not None:
            _set_levels(config.data, "unhandled", unhandled_levels)

        if strings_levels is not None:
            _set_levels(config.data, "strings", strings_levels)

        if time_levels is not None:
            _set_levels(config.data, "time", time_levels)

        if file_importers_levels is not None:
            _set_levels(config.data, "file_importers", file_importers_levels)

        # Context manager exits and writes the changes to config back to the yaml file


# Only run in here, for testing
if __name__ == "__main__":
    set_logging_config(log_input_data=False, level_source="default", strings_levels=(10, 10))
