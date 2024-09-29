## Most scripting should occur in a Cascade instance, which interfaces with the other functions
from .processors.cascade import Cascade

# These are for direct access to Data transformation outside of the chain
from .processors.llm_handler import LLMHandler
from .processors.sanitiser import Sanitiser
from .tools import prepper

# Set up all the loggers and add the loggers instance to the gylphdeck namespace
# Enables logger access like gylphdeck.loggers.cascade
from ._loggers import loggers # noqa: F401

# Explicitly defining __all__ for metadata and clarity
# This makes it clear what the intended public interface is
__all__ = [
    "Cascade",
    "LLMHandler",
    "Sanitiser",
    "prepper",
]
