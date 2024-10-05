"""Accesses the content of logging_.py and makes it available to the public interface.

Does this without changing the internal representation of the loggers themselves.

Classes
-------
**Loggers**
    A class for centralized logger access, in a Dataclass style interface.

    Checks if loggers exist, and creates them if they do not.

Example:
-------
Typical usage example::

    import glyphdeck as gd

    logger = gd.logger.cascade

"""

# Import the python logging module
import logging

# Import the internal logging module
# Contains the global syshook logger which is set on import
from . import logging_


class Loggers:
    """Centralized logger setup for various components within the application.

    Primarily, it adds loggers to the public interface without changing their internal representation.

    On importing the glyphdeck module, an instance of this class called `logger` is created,
    providing the interface for the loggers.

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

    Usage:
    ----------
    ```
    import glyphdeck as gd
    logger = gd.logger.cascade
    ```

    """

    def __init__(self):
        """Initialize a new instance of the Loggers class.
        
        All loggers are set to None initially and are created upon first access.
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
        """Get an existing logger by name or creates one using the provided logger class.

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
        """Gets the Cascade logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Cascade logger instance.

        """
        if self._cascade is None:
            self._cascade = self._get_or_create_logger(
                "cascade", logging_.CascadeLogger
            )
        return self._cascade

    @property
    def llm_handler(self):
        """Gets the LLM handler logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The LLM handler logger instance.

        """
        if self._llm_handler is None:
            self._llm_handler = self._get_or_create_logger(
                "llm_handler", logging_.LLMHandlerLogger
            )
        return self._llm_handler

    @property
    def sanitiser(self):
        """Gets the Sanitiser logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Sanitiser logger instance.

        """
        if self._sanitiser is None:
            self._sanitiser = self._get_or_create_logger(
                "sanitiser", logging_.SanitiserLogger
            )
        return self._sanitiser

    @property
    def cache(self):
        """Gets the Cache logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Cache logger instance.

        """
        if self._cache is None:
            self._cache = self._get_or_create_logger("cache", logging_.CacheLogger)
        return self._cache

    @property
    def file_importers(self):
        """Gets the File Importers logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The File Importers logger instance.

        """
        if self._file_importers is None:
            self._file_importers = self._get_or_create_logger(
                "file_importers", logging_.FileImportersToolsLogger
            )
        return self._file_importers

    @property
    def prepper(self):
        """Gets the Prepper logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Prepper logger instance.

        """
        if self._prepper is None:
            self._prepper
        if self._prepper is None:
            self._prepper = self._get_or_create_logger(
                "prepper", logging_.PrepperLogger
            )
        return self._prepper

    @property
    def string_tools(self):
        """Gets the String Tools logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The String Tools logger instance.

        """
        if self._string_tools is None:
            self._string_tools = self._get_or_create_logger(
                "string_tools", logging_.StringsToolsLogger
            )
        return self._string_tools

    @property
    def time_tools(self):
        """Gets the Time Tools logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Time Tools logger instance.

        """
        if self._time_tools is None:
            self._time_tools = self._get_or_create_logger(
                "time_tools", logging_.TimeToolsLogger
            )
        return self._time_tools

    @property
    def data_types(self):
        """Gets the Data Types logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Data Types logger instance.

        """
        if self._data_types is None:
            self._data_types = self._get_or_create_logger(
                "data_types", logging_.DataTypesLogger
            )
        return self._data_types

    @property
    def validators(self):
        """Gets the Validators logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Validators logger instance.

        """
        if self._validators is None:
            self._validators = self._get_or_create_logger(
                "validators", logging_.ValidatorsLogger
            )
        return self._validators

    @property
    def workflow(self):
        """Gets the Workflow logger, initializing it if it doesn't already exist.

        Returns:
            logging.Logger: The Workflow logger instance.

        """
        if self._workflow is None:
            self._workflow = self._get_or_create_logger(
                "workflow", logging_.BaseWorkflowLogger
            )
        return self._workflow


loggers = Loggers()
