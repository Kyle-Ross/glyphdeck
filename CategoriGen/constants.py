import os

# Folder paths - keep this .py in the project root directory
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DOCS_DIR = os.path.join(ROOT_DIR, '../docs')
PROCESSORS_DIR = os.path.join(ROOT_DIR, 'processors')
TOOLS_DIR = os.path.join(ROOT_DIR, 'tools')
VALIDATORS_DIR = os.path.join(ROOT_DIR, 'validation')
OUTPUT_DIR = os.path.join(ROOT_DIR, '../output')
OUTPUT_CACHES_DIR = os.path.join(OUTPUT_DIR, 'caches')
OUTPUT_FILES_DIR = os.path.join(OUTPUT_DIR, 'files')
OUTPUT_LOGS_DIR = os.path.join(OUTPUT_DIR, 'logs')

# Logging levels
# This sets the minimum level of logging each logger will save to the file or print to the console
# Levels - 0 NOTSET | 10 DEBUG | 20 INFO | 30 WARNING | 40 ERROR | 50 CRITICAL
file_log_level_constant = 20
console_log_level_constant = 20
