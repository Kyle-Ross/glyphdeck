import logging
import time

from glyphdeck.tools.logging import TimeToolsLogger, log_decorator

logger = TimeToolsLogger().setup()


@log_decorator(logger)
def delta_time_formatter(total_seconds: float) -> str:
    """Formats a float representing seconds into a string with hours, minutes, and seconds.

    Args:
        total_seconds: A float representing the total number of seconds.

    Returns:
        A string formatted as 'HHhMMmSSs' representing hours, minutes, and seconds (e.g '05h30m45s').
    """
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)
    result = f"{hours:02d}h{minutes:02d}m{seconds:02d}s"
    return result


class LogBlock:
    """Context manager that logs the time elapsed over the total runtime of a block.
    Useful for timing glyphdeck workflows.

    Usage:
    ```
    with LogBlock("Workflow Runtime"):
        # Your code here
    ```

    Attributes:
        logger: The logger instance used for logging messages.
        start_time: Time recorded at the start of the block.
        end_time: Time recorded at the end of the block.
        elapsed_time: Total time elapsed during the execution of the block.
        elapsed_time_seconds: Elapsed time formatted as a string in seconds.
    """

    decorator_message = "in LogBlock"

    # Prepare the logger on initialisation
    @log_decorator(logger, show_nesting=False, suffix_message=decorator_message)
    def __init__(self, message, logger_arg: logging.Logger = logger):
        """
        Initializes LogBlock with a logger instance and a custom message.

        Args:
            message (str): A custom message to include in the log.
            logger_arg (logging.Logger): A logging.Logger instance used for logging messages. Defaults to module logger.
        """
        self.message = message
        self.logger = logger_arg

    # On entry, record the time at the start of the block
    @log_decorator(logger, show_nesting=False, suffix_message=decorator_message)
    def __enter__(self):
        """Records the start time at the entry of the block.

        Returns:
            self: Returns the instance of LogBlock.
        """
        self.start_time: float = time.time()
        return self

    # On exit, compare the end time with the start and log the result
    @log_decorator(logger, show_nesting=False, suffix_message=decorator_message)
    def __exit__(self, exc_type, exc_value, exc_tb):
        """Logs the total runtime at the exit of the block.

        Args:
            exc_type: Exception type (if an exception was raised).
            exc_value: Exception value (if an exception was raised).
            exc_tb: Traceback object (if an exception was raised).
        """
        self.end_time: float = time.time()
        self.elapsed_time: float = self.end_time - self.start_time
        self.elapsed_time_seconds: str = format(self.elapsed_time, ",.4f")
        delta_time = delta_time_formatter(self.elapsed_time)
        delta_seconds = f"{self.elapsed_time_seconds} sec"
        message = f" | Class | LogBlock | Finish | Total runtime - {self.message} | | | {delta_time} | {delta_seconds}"
        self.logger.info(message)