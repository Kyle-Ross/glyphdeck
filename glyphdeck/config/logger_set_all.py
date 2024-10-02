from glyphdeck.config.logger_config import access_logging_config  # noqa: F401

# Logging levels
# This sets the minimum level of logging each logger_arg will save to the file or print to the console
# Levels - 0 NOTSET | 10 DEBUG | 20 INFO | 30 WARNING | 40 ERROR | 50 CRITICAL

# Get the dictionary from 
config = access_logging_config()

# Set all levels
# -----------------
if config["setting_type"] == "default":
    set_all_file_log_level: int = 99
    set_all_console_log_level: int = 20
if config["setting_type"] == "set_all":
    set_all_file_log_level: int = config["set_all"]["file"]
    set_all_console_log_level: int = config["set_all"]["console"]

# -----------------
# Private Information Control
# -----------------
log_input_data: bool = config["private_data"]["log_input"]
log_output_data: bool = config["private_data"]["log_output"]

# -----------------
# Distributing values across loggers
# -----------------

# Use the set all value if "default" or "set_all" is the current setting type
if config["setting_type"] in ("default", "set_all"):
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
if config["setting_type"] == "granular":
    # data_types.py
    data_types_file_log_level: int = config["data_types"]["file"]
    data_types_console_log_level: int = config["data_types"]["console"]

    # Prepper.py
    prepper_file_log_level: int = config["prepper"]["file"]
    prepper_console_log_level: int = config["prepper"]["console"]

    # Cascade.py
    cascade_file_log_level: int = config["cascade"]["file"]
    cascade_console_log_level: int = config["cascade"]["console"]

    # Sanitiser.py
    sanitiser_file_log_level: int = config["sanitiser"]["file"]
    sanitiser_console_log_level: int = config["sanitiser"]["console"]

    # Validators.py
    validators_file_log_level: int = config["validators"]["file"]
    validators_console_log_level: int = config["validators"]["console"]

    # LLMHandler.py
    llmhandler_file_log_level: int = config["llm_handler"]["file"]
    llmhandler_console_log_level: int = config["llm_handler"]["console"]

    # Cache.py
    cache_file_log_level: int = config["cache"]["file"]
    cache_console_log_level: int = config["cache"]["console"]

    # base_workflow.py
    workflow_file_log_level: int = config["workflows"]["file"]
    workflow_console_log_level: int = config["workflows"]["console"]

    # tools/strings.py
    strings_file_log_level: int = config["strings"]["file"]
    strings_console_log_level: int = config["strings"]["console"]

    # tools/time.py
    time_file_log_level: int = config["time"]["file"]
    time_console_log_level: int = config["time"]["console"]
    # tools/file_importers.py
    file_importers_file_log_level: int = config["file_importers"]["file"]
    file_importers_console_log_level: int = config["file_importers"]["console"]

    # Unhandled errors as used in workflow files
    unhandled_errors_file_log_level: int = config["unhandled_errors"]["file"]
    unhandled_errors_console_log_level: int = config["unhandled_errors"]["console"]
