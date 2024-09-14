import logging
import time

from CategoriGen.tools.loggers import TimeToolsLogger, log_decorator

logger = TimeToolsLogger().setup()


@log_decorator(logger)
def delta_time_formatter(total_seconds: float) -> str:
    """Takes a float representing seconds and turns it into a nice string like '05h30m45s'"""
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)
    result = f"{hours:02d}h{minutes:02d}m{seconds:02d}s"
    return result


class RuntimeLogBlock:
    """Content manager for 'with' blocks that logs the time elapsed over the total runtime of the block"""

    message = "in RuntimeLogBlock class"

    # Prepare the logger on initialisation
    @log_decorator(logger, show_nesting=False, suffix_message=message)
    def __init__(self, logger_arg: logging.Logger):
        self.logger = logger_arg

    # On entry, record the time at the start of the block
    @log_decorator(logger, show_nesting=False, suffix_message=message)
    def __enter__(self):
        self.start_time: float = time.time()
        return self

    # On exit, compare the end time with the start and log the result
    @log_decorator(logger, show_nesting=False, suffix_message=message)
    def __exit__(self, exc_type, exc_value, exc_tb):
        self.end_time: float = time.time()
        self.elapsed_time: float = self.end_time - self.start_time
        self.elapsed_time_seconds: str = format(self.elapsed_time, ",.4f")
        delta_time = delta_time_formatter(self.elapsed_time)
        delta_seconds = f"{self.elapsed_time_seconds} sec"
        message = f" | Class | RuntimeLogBlock | Finish | Total runtime | | | {delta_time} | {delta_seconds}"
        self.logger.info(message)
