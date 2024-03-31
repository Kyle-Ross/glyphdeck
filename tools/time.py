import logging
import time


def delta_time_formatter(total_seconds: float) -> str:
    """Takes a float representing seconds and turns it into a nice string like '05h30m45s'"""
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)
    result = f"{hours:02d}h{minutes:02d}m{seconds:02d}s"
    return result


class RuntimeLogBlock:
    """Content manager for 'with' blocks that logs the time elapsed over the total runtime of the block"""
    def __init__(self,
                 logger: logging.Logger):
        self.logger = logger

    def __enter__(self):  # Record the time at the start of the block
        self.start_time: float = time.time()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):  # Compare that to the time at the end and log the result
        self.end_time: float = time.time()
        self.elapsed_time: float = self.end_time - self.start_time
        self.elapsed_time_seconds: str = format(self.elapsed_time, ',.4f')
        self.logger.info(f'Total runtime - {delta_time_formatter(self.elapsed_time)} '
                         f'({self.elapsed_time_seconds} sec)')
