import logging
from .tools._logging import (
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

class Loggers:
    """
    A centralized logger setup for various components within the application, 
    essentially adds loggers to the public interface without changing their internal representation.

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
        sanitiser (logging.Logger): Logs activities related to the Sanitiser class.
        cache (logging.Logger): Manages cache-related logging.
        file_importers (logging.Logger): Logs events related to file importing tools.
        prepper (logging.Logger): Logs activities in data preparation.
        string_tools (logging.Logger): Handles logging for string manipulation tools.
        time_tools (logging.Logger): Logs activities related to time-related tools.
        data_types (logging.Logger): Manages logging for data type operations.
        validators (logging.Logger): Logs validation operations.
        workflow (logging.Logger): Logs workflow-related activities.
        unhandled_errors (logging.Logger): Captures any other errors that are not explicitly handled.
    """

    def __init__(self):
        """
        Initializes a new instance of the Loggers class. All loggers are set 
        to None initially and are created upon first access.
        """
        self._cascade = None
        self._llm_handler = None
        self._sanitiser = None
        self._cache = None
        self._file_importers = None
        self._prepper = None
        self._string_tools = None
        self._time_tools = None
        self._data_types = None
        self._validators = None
        self._workflow = None
        self._unhandled_errors = None

    def _get_or_create_logger(self, name, logger_class):
        """
        Gets an existing logger by name or creates one using the provided logger class.

        Args:
            name (str): The name of the logger.
            logger_class (class): The class of the logger to create if it doesn't exist.

        Returns:
            logging.Logger: The logger instance.
        """
        existing_logger = logging.getLogger(name)
        if existing_logger.hasHandlers():
            return existing_logger
        else:
            return logger_class().setup()

    @property
    def cascade(self):
        """
        Gets the Cascade logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Cascade logger instance.
        """
        if self._cascade is None:
            self._cascade = self._get_or_create_logger('cascade', CascadeLogger)
        return self._cascade

    @property
    def llm_handler(self):
        """
        Gets the LLM handler logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The LLM handler logger instance.
        """
        if self._llm_handler is None:
            self._llm_handler = self._get_or_create_logger('llm_handler', LLMHandlerLogger)
        return self._llm_handler

    @property
    def sanitiser(self):
        """
        Gets the Sanitiser logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Sanitiser logger instance.
        """
        if self._sanitiser is None:
            self._sanitiser = self._get_or_create_logger('sanitiser', SanitiserLogger)
        return self._sanitiser

    @property
    def cache(self):
        """
        Gets the Cache logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Cache logger instance.
        """
        if self._cache is None:
            self._cache = self._get_or_create_logger('cache', CacheLogger)
        return self._cache

    @property
    def file_importers(self):
        """
        Gets the File Importers logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The File Importers logger instance.
        """
        if self._file_importers is None:
            self._file_importers = self._get_or_create_logger('file_importers', FileImportersToolsLogger)
        return self._file_importers

    @property
    def prepper(self):
        """
        Gets the Prepper logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Prepper logger instance.
        """
        if self._prepper is None:
            self._prepper
        if self._prepper is None:
            self._prepper = self._get_or_create_logger('prepper', PrepperLogger)
        return self._prepper

    @property
    def string_tools(self):
        """
        Gets the String Tools logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The String Tools logger instance.
        """
        if self._string_tools is None:
            self._string_tools = self._get_or_create_logger('string_tools', StringsToolsLogger)
        return self._string_tools

    @property
    def time_tools(self):
        """
        Gets the Time Tools logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Time Tools logger instance.
        """
        if self._time_tools is None:
            self._time_tools = self._get_or_create_logger('time_tools', TimeToolsLogger)
        return self._time_tools

    @property
    def data_types(self):
        """
        Gets the Data Types logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Data Types logger instance.
        """
        if self._data_types is None:
            self._data_types = self._get_or_create_logger('data_types', DataTypesLogger)
        return self._data_types

    @property
    def validators(self):
        """
        Gets the Validators logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Validators logger instance.
        """
        if self._validators is None:
            self._validators = self._get_or_create_logger('validators', ValidatorsLogger)
        return self._validators

    @property
    def workflow(self):
        """
        Gets the Workflow logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Workflow logger instance.
        """
        if self._workflow is None:
            self._workflow = self._get_or_create_logger('workflow', BaseWorkflowLogger)
        return self._workflow

    @property
    def unhandled_errors(self):
        """
        Gets the Unhandled Errors logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Unhandled Errors logger instance.
        """
        if self._unhandled_errors is None:
            self._unhandled_errors = self._get_or_create_logger('unhandled_errors', UnhandledErrorsLogger)
        return self._unhandled_errors

loggers = Loggers()
