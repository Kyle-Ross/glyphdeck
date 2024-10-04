## Most scripting should occur in a Cascade instance, which interfaces with the other functions
from .processors.cascade import Cascade

# These are for direct access to Data transformation outside of the chain
from .processors.llm_handler import LLMHandler
from .processors.sanitiser import Sanitiser
from glyphdeck.validation import validators
from .tools import prepper

# Provide access to the set_logging_config
from .config.logger_config import (
    access_logging_config,
    set_logging_config,
    restore_logger_config,
)

# Enables logger access in the public interface like gylphdeck.loggers.cascade
# Accesses or creates loggers only if they do not exist globally yet
from .tools.logger_interface import loggers

# Making common data_types available in the interface
from .validation.data_types import DataDict, Optional_DataDict, RecordDict, RecordsDict

# Making the LogBlock context manager available
from .tools.time import LogBlock

# Explicitly defining __all__ for metadata and clarity
# This makes it clear what the intended public interface is
__all__ = [
    "Cascade",
    "LLMHandler",
    "Sanitiser",
    "validators",
    "prepper",
    "access_logging_config",
    "set_logging_config",
    "restore_logger_config",
    "loggers",
    "DataDict",
    "Optional_DataDict",
    "RecordDict",
    "RecordsDict",
    "LogBlock",
]
