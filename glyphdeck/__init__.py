"""The glyphdeck library is a comprehensive toolkit designed to streamline & simplify various aspects of asynchronous LLM data processing workflows over high-volume, dense semantic data - like customer feedback, reviews and comments.

Common aspects of the LLM data workflow are handled end-to-end data validation, sanitisation, transformation, caching and step chaining, facilitating the fast development of robust, error free LLM data workflows.

Its also equiped with a configurable logging facility that makes complex asyncronous LLM workflows much easier to configure, understand and debug.
"""

## Cascade class inherits most functionality
from .processors.cascade import Cascade

# Tools outside of the Cascade class
from glyphdeck.validation import validators
from .tools.prepper import prepare
from .tools.time import LogBlock


# Change and access global logging config
from .tools.logger_interface import loggers
from .config.logger_config import (
    configure_logging,
    reset_logging,
)

# Making common data_types available in the interface
from .validation.data_types import DataDict, RecordDict, RecordsDict

# Among other things, determines the display order in the docs
__all__ = [
    "Cascade",
    "validators",
    "configure_logging",
    "reset_logging",
    "loggers",
    "LogBlock",
    "DataDict",
    "Optional_DataDict",
    "RecordDict",
    "RecordsDict",
    "prepare",
]
