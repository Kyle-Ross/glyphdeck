# Logging levels
# This sets the minimum level of logging each logger_arg will save to the file or print to the console
# Levels - 0 NOTSET | 10 DEBUG | 20 INFO | 30 WARNING | 40 ERROR | 50 CRITICAL

# -----------------
# Default levels
# -----------------
file_log_level_default: int = 10
console_log_level_default: int = 10

# -----------------
# Private Information Control
# -----------------
log_input_data: bool = False
log_output_data: bool = False

# -----------------
# Granular Control
# -----------------

# data_types.py
data_types_file_log_level: int = file_log_level_default
data_types_console_log_level: int = console_log_level_default

# Prepper.py
prepper_file_log_level: int = file_log_level_default
prepper_console_log_level: int = console_log_level_default

# Chain.py
chain_file_log_level: int = file_log_level_default
chain_console_log_level: int = console_log_level_default

# Sanitiser.py
sanitiser_file_log_level: int = file_log_level_default
sanitiser_console_log_level: int = console_log_level_default

# Validators.py
validators_file_log_level: int = file_log_level_default
validators_console_log_level: int = console_log_level_default

# LLMHandler.py
llmhandler_file_log_level: int = file_log_level_default
llmhandler_console_log_level: int = console_log_level_default

# Cache.py
cache_file_log_level: int = file_log_level_default
cache_console_log_level: int = console_log_level_default

# base_workflow.py
base_workflow_file_log_level: int = file_log_level_default
base_workflow_console_log_level: int = console_log_level_default

# Unhandled errors as used in workflow files
unhandled_errors_file_log_level: int = file_log_level_default
unhandled_errors_console_log_level: int = console_log_level_default

# tools/strings.py
tools_strings_file_log_level: int = file_log_level_default
tools_strings_console_log_level: int = console_log_level_default

# tools/time.py
tools_time_file_log_level: int = file_log_level_default
tools_time_console_log_level: int = console_log_level_default
