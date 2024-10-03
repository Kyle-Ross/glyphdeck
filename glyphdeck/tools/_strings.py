from glyphdeck.tools._logging import StringsToolsLogger, log_decorator

logger = StringsToolsLogger().setup()


@log_decorator(logger)
def string_cleaner(input_str: str) -> str:
    """Cleans a given string by trimming whitespace, converting to lowercase, and removing spaces.

    Args:
        input_str: The string to be cleaned.

    Returns:
        A cleaned string which is trimmed, converted to lowercase, and has all spaces removed.
    """
    return input_str.strip().lower().replace(" ", "")
