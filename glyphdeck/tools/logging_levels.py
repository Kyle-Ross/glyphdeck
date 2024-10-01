from dataclasses import dataclass, field
from typing import Dict

@dataclass
class LoggerLevel:
    """
    Dataclass to store logging levels for a logger.

    Attributes:
        file_log_level (int): The log level for the file handler. Defaults to 20 (INFO).
        console_log_level (int): The log level for the console handler. Defaults to 20 (INFO).
    """
    file_log_level: int = 20
    console_log_level: int = 20

@dataclass
class LoggingLevels:
    """
    Dataclass to manage logging levels for different loggers.

    Attributes:
        cascade (LoggerLevel): Logging levels for the cascade logger.
        llm_handler (LoggerLevel): Logging levels for the LLM handler logger.
        sanitiser (LoggerLevel): Logging levels for the sanitiser logger.
        cache (LoggerLevel): Logging levels for the cache logger.
        file_importers (LoggerLevel): Logging levels for the file importers logger.
        prepper (LoggerLevel): Logging levels for the prepper logger.
        string_tools (LoggerLevel): Logging levels for the string tools logger.
        time_tools (LoggerLevel): Logging levels for the time tools logger.
        data_types (LoggerLevel): Logging levels for the data types logger.
        validators (LoggerLevel): Logging levels for the validators logger.
        workflow (LoggerLevel): Logging levels for the workflow logger.

    Methods:
        reset_to_defaults(): Resets all logging levels to their default values.
        set_all_levels(file_log_level, console_log_level): Sets all loggers to specified file and console log levels.
    """
    cascade: LoggerLevel = field(default_factory=LoggerLevel)
    llm_handler: LoggerLevel = field(default_factory=LoggerLevel)
    sanitiser: LoggerLevel = field(default_factory=LoggerLevel)
    cache: LoggerLevel = field(default_factory=LoggerLevel)
    file_importers: LoggerLevel = field(default_factory=LoggerLevel)
    prepper: LoggerLevel = field(default_factory=LoggerLevel)
    string_tools: LoggerLevel = field(default_factory=LoggerLevel)
    time_tools: LoggerLevel = field(default_factory=LoggerLevel)
    data_types: LoggerLevel = field(default_factory=LoggerLevel)
    validators: LoggerLevel = field(default_factory=LoggerLevel)
    workflow: LoggerLevel = field(default_factory=LoggerLevel)

    _default_levels: Dict[str, LoggerLevel] = field(default_factory=dict, init=False)
    
    def __post_init__(self):
        """
        Initializes the LoggingLevels instance and stores initial default levels.
        """
        self._default_levels = {key: LoggerLevel(value.file_log_level, value.console_log_level) for key, value in self.__dict__.items() if isinstance(value, LoggerLevel)}

    def reset(self):
        """
        Resets all logging levels to their default values.
        """
        for key, level in self._default_levels.items():
            setattr(self, key, LoggerLevel(level.file_log_level, level.console_log_level))

    def set_all(self, file_log_level: int, console_log_level: int):
        """
        Sets all loggers to the specified file and console log levels.

        Args:
            file_log_level (int): The log level for the file handler.
            console_log_level (int): The log level for the console handler.
        """
        for key in self._default_levels.keys():
            setattr(self, key, LoggerLevel(file_log_level, console_log_level))
