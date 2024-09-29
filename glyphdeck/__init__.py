## Most scripting should occur in a Cascade instance, which interfaces with the other functions
from .processors.cascade import Cascade

# These are for direct access to Data transformation outside of the chain
from .processors.llm_handler import LLMHandler
from .processors.sanitiser import Sanitiser
from .tools import prepper

# Import all the loggers
from .tools.loggers import (
    CascadeLogger,
    LLMHandlerLogger,
    SanitiserLogger,
    CacheLogger,
    FileImportersToolsLogger,
    PrepperLogger,
    StringsToolsLogger,
    TimeToolsLogger,
    DataTypesLogger,
    ValidatorsLogger,
    BaseWorkflowLogger,
    UnhandledErrorsLogger,
)

# Set up all the loggers
cascade_logger = CascadeLogger().setup()
llm_handler_logger = LLMHandlerLogger().setup()
sanitiser_logger = SanitiserLogger().setup()
cache_logger = CacheLogger().setup()
file_importers_logger = FileImportersToolsLogger().setup()
prepper_logger = PrepperLogger().setup()
string_tools_logger = StringsToolsLogger().setup()
time_tools_logger = TimeToolsLogger().setup()
data_types_logger = DataTypesLogger().setup()
validators_logger = ValidatorsLogger().setup()
workflow_logger = BaseWorkflowLogger().setup()
unhandled_errors_logger = UnhandledErrorsLogger().setup()

# Explicitly defining __all__ for metadata and clarity
# This makes it clear what the intended public interface is
__all__ = [
    "Cascade",
    "LLMHandler",
    "Sanitiser",
    "prepper",
]
