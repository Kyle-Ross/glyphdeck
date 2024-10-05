import toml
import sys
import os

# Add the target directory(s) to the Python path.
sys.path.insert(0, os.path.abspath("../glyphdeck"))

# Accessing the poetry config toml file
with open("../pyproject.toml", "r") as f:
    poetry_toml = toml.load(f)

# Use the name from pyproject.toml
project = poetry_toml["tool.poetry"]["name"]

# %Y is replaced with the current year
copyright = "%Y, Kyle Ross"

# Use the author from pyproject.toml
author = poetry_toml["tool.poetry"]["authors"][0]

# Use the release from pyproject.toml
release = poetry_toml["tool.poetry"]["version"]

# Set extensions
extensions = [
    # Automatically document your Python code via docstrings
    "sphinx.ext.autodoc",
    # Add links to the source code
    "sphinx.ext.viewcode",
    # Support for Google and NumPy style docstrings
    # By default will ignore private objects with _leading underscore
    "sphinx.ext.napoleon",
]

# Types of paths to exclude
templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# Set your HTML theme here
html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
