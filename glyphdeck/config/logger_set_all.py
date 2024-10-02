from glyphdeck.config.logger_config import access_logging_config  # noqa: F401

# Logging levels
# This sets the minimum level of logging each logger_arg will save to the file or print to the console
# Levels - 0 NOTSET | 10 DEBUG | 20 INFO | 30 WARNING | 40 ERROR | 50 CRITICAL

# Get yaml
config = access_logging_config()
print(config)
setting_type = config["setting_type"]


# print(config)
print(setting_type)


# -----------------
# Default levels
# -----------------
set_all_file_log_level: int = 99
set_all_console_log_level: int = 20

# -----------------
# Private Information Control
# -----------------
log_input_data: bool = False
log_output_data: bool = False

# -----------------
# Granular Control
# -----------------

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
tools_strings_file_log_level: int = set_all_file_log_level
tools_strings_console_log_level: int = set_all_console_log_level

# tools/time.py
tools_time_file_log_level: int = set_all_file_log_level
tools_time_console_log_level: int = set_all_console_log_level

# tools/file_importers.py
tools_file_importers_file_log_level: int = set_all_file_log_level
tools_file_importers_console_log_level: int = set_all_console_log_level

# Unhandled errors as used in workflow files
unhandled_errors_file_log_level: int = set_all_file_log_level
unhandled_errors_console_log_level: int = set_all_console_log_level
