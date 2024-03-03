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


def string_cleaner(input_str: str) -> str:
    """Basic function to clean input strings."""
    return input_str.strip().lower().replace(' ', '')
