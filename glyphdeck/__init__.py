## Most scripting should occur in a Cascade instance, which interfaces with the other functions
from .processors.cascade import Cascade

# These are for direct access to Data transformation outside of the chain
from .tools.logging_levels import LoggingLevels
from .processors.llm_handler import LLMHandler
from .processors.sanitiser import Sanitiser
from glyphdeck.validation import validators
from .tools import prepper

# Enables logger access in the public interface like gylphdeck.loggers.cascade
# Accesses or creates loggers only if they do not exist globally yet
from .tools.loggers import loggers

# Making common data_types available in the interface
from .validation.data_types import DataDict, Optional_DataDict, RecordDict, RecordsDict

# Making the LogBlock context manager available
from .tools._time import LogBlock 

# Initialise dataclass with logger settings
logging_levels = LoggingLevels()

# Explicitly defining __all__ for metadata and clarity
# This makes it clear what the intended public interface is
__all__ = [
    "Cascade",
    "LLMHandler",
    "Sanitiser",
    "validators",
    "prepper",
    "loggers",
    "DataDict",
    "Optional_DataDict",
    "RecordDict",
    "RecordsDict",
    "LogBlock",
    "LoggingLevels"
]
