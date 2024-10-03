"""Module for defining essential directory paths for the project.

This module initializes and defines various directory paths that are used
throughout the project for documentation, processing, tools, validation, and
output files. The structure ensures organized access to resources required by
different components of the project. All paths are configured relative to the
root directory, which is assumed to be the directory where this script is
located.

Paths
-----
**ROOT_DIR**
  Root directory of the project.

**DOCS_DIR**
  Path to the documentation directory.

**PROCESSORS_DIR**
  Path to the processors directory.

**TOOLS_DIR**
  Path to the tools directory.

**VALIDATORS_DIR**
  Path to the validation directory.

**OUTPUT_DIR**
  Path to the output directory.

**OUTPUT_CACHES_DIR**
  Path to the caches directory within the output directory.

**OUTPUT_FILES_DIR**
  Path to the files directory within the output directory.

**OUTPUT_LOGS_DIR**
  Path to the logs directory within the output directory.
"""

import os

# Folder paths - keep this .py in the project root directory
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DOCS_DIR = os.path.join(ROOT_DIR, "../docs")
PROCESSORS_DIR = os.path.join(ROOT_DIR, "processors")
TOOLS_DIR = os.path.join(ROOT_DIR, "tools")
VALIDATORS_DIR = os.path.join(ROOT_DIR, "validation")
OUTPUT_DIR = os.path.join(ROOT_DIR, "../output")
OUTPUT_CACHES_DIR = os.path.join(OUTPUT_DIR, "caches")
OUTPUT_FILES_DIR = os.path.join(OUTPUT_DIR, "files")
OUTPUT_LOGS_DIR = os.path.join(OUTPUT_DIR, "logs")
