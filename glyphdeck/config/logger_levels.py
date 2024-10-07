"""Stores and distributes the logging levels for various components in the system based on the logging configuration retrieved from `access_logging_config`.

The module supports configurations that allow uniform logging levels across all components or granular levels for each component individually.

Logger levels should not be configured from this object, but instead from the `set_logging_config()`
function in the `logger_config` module.

Attributes
----------
set_all_file_log_level : int
    The default logging level for files.
set_all_console_log_level : int
    The default logging level for console output.
data_types_file_log_level : int
    The file logging level for data_types.
data_types_console_log_level : int
    The console logging level for data_types.
prepper_file_log_level : int
    The file logging level for prepper.
prepper_console_log_level : int
    The console logging level for prepper.
cascade_file_log_level : int
    The file logging level for cascade.
cascade_console_log_level : int
    The console logging level for cascade.
sanitiser_file_log_level : int
    The file logging level for sanitiser.
sanitiser_console_log_level : int
    The console logging level for sanitiser.
validators_file_log_level : int
    The file logging level for validators.
validators_console_log_level : int
    The console logging level for validators.
llmhandler_file_log_level : int
    The file logging level for llmhandler.
llmhandler_console_log_level : int
    The console logging level for llmhandler.
cache_file_log_level : int
    The file logging level for cache.
cache_console_log_level : int
    The console logging level for cache.
workflow_file_log_level : int
    The file logging level for base_workflow.
workflow_console_log_level : int
    The console logging level for base_workflow.
strings_file_log_level : int
    The file logging level for strings.
strings_console_log_level : int
    The console logging level for strings.
time_file_log_level : int
    The file logging level for time.
time_console_log_level : int
    The console logging level for time.
file_importers_file_log_level : int
    The file logging level for file_importers.
file_importers_console_log_level : int
    The console logging level for file_importers.
unhandled_errors_file_log_level : int
    The file logging level for unhandled errors.
unhandled_errors_console_log_level : int
    The console logging level for unhandled errors.
log_input_data : bool
    Control if input data will be logged.
log_output_data : bool
    Control if output data will be logged.

"""

from glyphdeck.config.logger_config import access_logging_config  # noqa: F401

# Logging levels
# This sets the minimum level of logging each logger_arg will save to the file or print to the console
# Levels - 0 NOTSET | 10 DEBUG | 20 INFO | 30 WARNING | 40 ERROR | 50 CRITICAL

# Get the dictionary from
_config = access_logging_config()

# -----------------
# Set all levels
# -----------------
# The set all vars can be changed in different ways by either the default or set_all conditions
if _config["setting_type"] == "default":
    set_all_file_log_level: int = 99
    set_all_console_log_level: int = 20
if _config["setting_type"] == "set_all":
    set_all_file_log_level: int = _config["set_all"]["file"]
    set_all_console_log_level: int = _config["set_all"]["console"]

# -----------------
# Private Information Control
# -----------------
log_input_data: bool = _config["private_data"]["log_input"]
log_output_data: bool = _config["private_data"]["log_output"]

# -----------------
# Distributing values across loggers
# -----------------

# Use the set all value if "default" or "set_all" is the current setting type
if _config["setting_type"] in ("default", "set_all"):
    # data_types.py
    data_types_file_log_level: int = set_all_file_log_level
    data_types_console_log_level: int = set_all_console_log_level

    # Prepper.py
    prepper_file_log_level: int = set_all_file_log_level
    prepper_console_log_level: int = set_all_console_log_level

    # Cascade.py
    cascade_file_log_level: int = set_all_file_log_level
    cascade_console_log_level: int = set_all_console_log_level

    # Sanitiser.py
    sanitiser_file_log_level: int = set_all_file_log_level
    sanitiser_console_log_level: int = set_all_console_log_level

    # Validators.py
    validators_file_log_level: int = set_all_file_log_level
    validators_console_log_level: int = set_all_console_log_level

    # LLMHandler.py
    llmhandler_file_log_level: int = set_all_file_log_level
    llmhandler_console_log_level: int = set_all_console_log_level

    # Cache.py
    cache_file_log_level: int = set_all_file_log_level
    cache_console_log_level: int = set_all_console_log_level

    # base_workflow.py
    workflow_file_log_level: int = set_all_file_log_level
    workflow_console_log_level: int = set_all_console_log_level

    # tools/strings.py
    strings_file_log_level: int = set_all_file_log_level
    strings_console_log_level: int = set_all_console_log_level

    # tools/time.py
    time_file_log_level: int = set_all_file_log_level
    time_console_log_level: int = set_all_console_log_level

    # tools/file_importers.py
    file_importers_file_log_level: int = set_all_file_log_level
    file_importers_console_log_level: int = set_all_console_log_level

    # Unhandled errors as used in workflow files
    unhandled_errors_file_log_level: int = set_all_file_log_level
    unhandled_errors_console_log_level: int = set_all_console_log_level

# Use each individual logger's config if "granular" is the current setting type
if _config["setting_type"] == "granular":
    # data_types.py
    data_types_file_log_level: int = _config["data_types"]["file"]
    data_types_console_log_level: int = _config["data_types"]["console"]

    # Prepper.py
    prepper_file_log_level: int = _config["prepper"]["file"]
    prepper_console_log_level: int = _config["prepper"]["console"]

    # Cascade.py
    cascade_file_log_level: int = _config["cascade"]["file"]
    cascade_console_log_level: int = _config["cascade"]["console"]

    # Sanitiser.py
    sanitiser_file_log_level: int = _config["sanitiser"]["file"]
    sanitiser_console_log_level: int = _config["sanitiser"]["console"]

    # Validators.py
    validators_file_log_level: int = _config["validators"]["file"]
    validators_console_log_level: int = _config["validators"]["console"]

    # LLMHandler.py
    llmhandler_file_log_level: int = _config["llm_handler"]["file"]
    llmhandler_console_log_level: int = _config["llm_handler"]["console"]

    # Cache.py
    cache_file_log_level: int = _config["cache"]["file"]
    cache_console_log_level: int = _config["cache"]["console"]

    # base_workflow.py
    workflow_file_log_level: int = _config["workflows"]["file"]
    workflow_console_log_level: int = _config["workflows"]["console"]

    # tools/strings.py
    strings_file_log_level: int = _config["strings"]["file"]
    strings_console_log_level: int = _config["strings"]["console"]

    # tools/time.py
    time_file_log_level: int = _config["time"]["file"]
    time_console_log_level: int = _config["time"]["console"]
    # tools/file_importers.py
    file_importers_file_log_level: int = _config["file_importers"]["file"]
    file_importers_console_log_level: int = _config["file_importers"]["console"]

    # Unhandled errors as used in workflow files
    # This is used by the global_exception_logger - which logs exclusively at CRITICAL level (50)
    unhandled_errors_file_log_level: int = _config["unhandled_errors"]["file"]
    unhandled_errors_console_log_level: int = _config["unhandled_errors"]["console"]
