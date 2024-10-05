"""The glyphdeck library is a comprehensive toolkit designed to streamline & simplify various aspects of asynchronous LLM data processing workflows over high-volume, dense semantic data - like customer feedback, reviews and comments.

glyphdeck handles LLM data workflow data validation, sanitisation, transformation and step chaining,
facilitating the fast development of robust, error free LLM data workflows.

glyphdeck.Cascade():
--------------------
The Cascade class is the primary interface for the glyphdeck library. It handles and processes data in a record-like structure, providing easy to use syntax for data handling workflows with LLMs.

It validates and enforces all data movements against a common id, ensuring that each record has a unique, immutable identifier that remains consistent, regardless of other changes.

**cascade.llm_handler**
    Handles the complex asynchronous interaction with LLM providers in the Cascade instance.

**cascade.sanitiser**
    Optionally strip out private information before asynchronous requests are sent to the LLM.

Telemetry:
----------
glyphdeck is provided with a comprehensive logging facility that makes
complex asyncronous LLM workflows much easier to configure, understand and debug.

Example:
---------
Here is a basic example of how to use the main features of the glyphdeck library::

    import glyphdeck as gd

    # Intialising a cascade object, setting its first record from the source file
    cascade = gd.Cascade(
        data_source=r"pizzashopreviews.xlsx",
        id_column="Review Id",
        data_columns=["Review Text", "Reason for score"],
    )

    # Sanitising the data of sensitive information, replacing with placeholders
    cascade.sanitiser.run()

    # Set the LLM Handler for this cascade instance
    cascade.set_llm_handler(
        provider="OpenAI",
        model="gpt-4o-mini",
        system_message="You are an expert customer feedback analyst nlp system. Analyse the feedback and return results in the correct format.",
        validation_model=gd.validators.SubCats,
        cache_identifier="PizzaShopComment_Sub_Categories",
        use_cache=True,
    )

    # Run the llm_handler
    cascade.llm_handler.run("HandlerOutput1")

    # Output the result in the specified format
    # Latest record is used by default, but we specify it here
    cascade.write_output(
        file_type="xlsx",
        file_name_prefix="Cascade Test",
        record_identifiers=["sanitised", "HandlerOutput1"],
        rebase=True,
        xlsx_use_sheets=False,
    )

"""  # noqa: D100

## Most scripting should occur in a Cascade instance, which interfaces with the other functions
from .processors.cascade import Cascade

# These are for direct access to Data transformation outside of the chain
from .processors.llm_handler import LLMHandler
from .processors.sanitiser import Sanitiser
from glyphdeck.validation import validators
from .tools.prepper import prepare

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
    "prepare",
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
