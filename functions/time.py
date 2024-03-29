from datetime import timedelta
import time

start_time = time.time()


def time_since_start(active=True):
    """Returns string representing sec delta since script start. Start time is initialised in the function source .py.
    Can be turned on and off using the 'active' variable."""
    global start_time
    elapsed_time = time.time() - start_time
    elapsed_time = "{:.4f}".format(elapsed_time)
    if active:
        return f'\u25B2 {elapsed_time}s'
    else:
        return ''


def delta_time_format(td: timedelta) -> str:
    """Takes a TimeDelta object and turns it into a nice string like '05h30m45s'"""
    total_seconds = td.total_seconds()
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)
    result = f"{hours:02d}h{minutes:02d}m{seconds:02d}s"
    return result
