from datetime import datetime
import toml
import sys
import os

# Add the target directory(s) to the Python path.
sys.path.insert(0, os.path.abspath("../glyphdeck"))

# Accessing the poetry config toml file
pyproject_toml = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "../pyproject.toml"
)
with open(pyproject_toml, "r") as f:
    poetry_toml = toml.load(f)

# Use the name from pyproject.toml
project = poetry_toml["tool"]["poetry"]["name"]

# Set copyright with the current year
current_year = datetime.now().year
copyright = f"{current_year}, Kyle Ross"

# Use the author from pyproject.toml
author = poetry_toml["tool"]["poetry"]["authors"][0]

# Use the release from pyproject.toml
release = poetry_toml["tool"]["poetry"]["version"]

# Set extensions
extensions = [
    # Automatically document your Python code via docstrings
    "sphinx.ext.autodoc",
    # Add links to the source code
    "sphinx.ext.viewcode",
    # Support for Google and NumPy style docstrings
    # By default will ignore private objects with _leading underscore
    "sphinx.ext.napoleon",
    # Makes the search results look better
    "sphinxprettysearchresults",
    # Adds emoji compatibility
    "sphinxemoji.sphinxemoji",
]

# Default options for every rst file
autodoc_default_options = {
    "members": True,
    "show-inheritance": False,
    "undoc-members": False,
}

# Types of paths to exclude
templates_path = ["_templates"]
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "_path_constants.py",
    "config/logger_levels.py",
    "tools/strings.py",
    "tools/file_importers.py",
    "tools/directory_creators.py",
    "tools/caching.py",
    "tools/logging_.py",
    "custom_extensions/add_header",
]

# Set your HTML theme here
html_theme = "furo"
html_static_path = ["_static"]
