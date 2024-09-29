from .tools.logging import (
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
class Loggers:
    """
    A centralized logger setup for various components within the application.

    This class sets up multiple loggers, each aimed at handling specific types of logging activities.
    Each logger is an instance of its respective logging class and is initialized via its `setup` method.

    This is run on import of glyphdeck, creating an instance of the Logger class called `logger`. 
    This means loggers can be accessed like this:

    ```
    import glyphdeck as gd

    logger = gd.logger.cascade
    ```

    Attributes:
        cascade (logging.Logger): Handles Cascade related logging.
        llm_handler (logging.Logger): Handles logging for language model handler operations.
        sanitiser (logging.Logger): Logs activities related to to the Sanitiser class.
        cache (logging.Logger): Manages cache-related logging.
        file_importers (logging.Logger): Logs events related to file importing tools.
        prepper (logging.Logger): Logs activities in data preparation.
        string_tools (logging.Logger): Handles logging for string manipulation tools.
        time_tools (logging.Logger): Logs activities related to time-related tools.
        data_types (logging.Logger): Manages logging for data type operations.
        validators (logging.Logger): Logs validation operations.
        workflow (logging.Logger): Logs workflow-related activities.
        unhandled_errors (logging.Logger): Captures and any other errors that are not explicitly handled.
    """
    cascade = CascadeLogger().setup()
    llm_handler = LLMHandlerLogger().setup()
    sanitiser = SanitiserLogger().setup()
    cache = CacheLogger().setup()
    file_importers = FileImportersToolsLogger().setup()
    prepper = PrepperLogger().setup()
    string_tools = StringsToolsLogger().setup()
    time_tools = TimeToolsLogger().setup()
    data_types = DataTypesLogger().setup()
    validators = ValidatorsLogger().setup()
    workflow = BaseWorkflowLogger().setup()
    unhandled_errors = UnhandledErrorsLogger().setup()

loggers = Loggers()
