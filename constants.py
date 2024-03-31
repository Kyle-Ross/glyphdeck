import os

# Constants for folder paths - keep this .py in the project root directory
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DOCS_DIR = os.path.join(ROOT_DIR, 'docs')
PROCESSORS_DIR = os.path.join(ROOT_DIR, 'processors')
TOOLS_DIR = os.path.join(ROOT_DIR, 'tools')
VALIDATORS_DIR = os.path.join(ROOT_DIR, 'validation')
OUTPUT_DIR = os.path.join(ROOT_DIR, 'output')
OUTPUT_CACHES_DIR = os.path.join(OUTPUT_DIR, 'caches')
OUTPUT_FILES_DIR = os.path.join(OUTPUT_DIR, 'files')
OUTPUT_LOGS_DIR = os.path.join(OUTPUT_DIR, 'logs')
