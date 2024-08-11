from CategoriGen.tools.loggers import StringsToolsLogger, log_decorator

logger = StringsToolsLogger().setup()


@log_decorator(logger)
def string_cleaner(input_str: str) -> str:
    """Basic function to clean input strings."""
    return input_str.strip().lower().replace(" ", "")
