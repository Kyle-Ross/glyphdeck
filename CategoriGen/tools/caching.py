from typing import Tuple, Callable
import hashlib
import os

from diskcache import Cache

from CategoriGen.tools.directory_creators import create_caches_directory
from CategoriGen.tools.loggers import CacheLogger, log_decorator

logger = CacheLogger().setup()


@log_decorator(logger, suffix_message="Check or create cache, return object and path")
def cache_creator(cache_dir: str, max_mb_size: int) -> Tuple[Cache, str]:
    """Checks if a cache exists, if not, creates it. Returns the cache object and file path afterwards."""
    # Creates the caches directory and returns the full cache file path within
    full_cache_dir = os.path.join(create_caches_directory(logger), cache_dir)
    # Create a DiskCache instance with the specified max_size
    max_size = max_mb_size * 1024 * 1024  # Convert megabytes to bytes
    cache = Cache(full_cache_dir, size_limit=max_size)
    return cache, full_cache_dir


@log_decorator(logger, suffix_message="Grab OpenAI completions if in cache, otherwise API")
def openai_cache(cache_dir: str, max_mb_size: int = 1000) -> Callable:
    """Facilitates caching for openai in the handler when used as a decorator. Function result will be taken from the
    cache if available, otherwise the function will call the API. When max size is reached the 'least recent use'
    records will be culled."""
    # Finds or creates the cache folder and object, as well as returning the directory of the cache
    cache, full_cache_dir = cache_creator(cache_dir, max_mb_size)

    # Define the actual decorator function
    def decorator(func):
        # counter for the amount of completions
        completions = 0  

        async def wrapper(self, *args, **kwargs):
            nonlocal completions
            # Accessing the self variables from the class in which this decorator is used
            self_cache_identifier = self.cache_identifier
            # self.active_record_title only exists when this wrapper is used in the chain.llm_handler inheritance of LLMHandler
            # So if we don't find it, we replace with a default value
            self_record_identifier = getattr(self, 'active_record_title', 'no_active_chain_record')
            self_cache_and_record_identifier = f"{self_cache_identifier} | {self_record_identifier}"
            self_use_cache = self.use_cache
            self_provider = self.provider
            self_model = self.model
            self_system_message = self.system_message
            self_validation_model = self.validation_model.__name__

            # Accessing individual args and kwargs if they exist, for use in logs
            key_arg = args[2] if len(args) > 2 else kwargs.get("key")
            index_arg = args[3] if len(args) > 3 else kwargs.get("index")

            # Building a string key out of the accessible information
            cache_key = (
                f"{self_cache_and_record_identifier}|{str(key_arg)}|{str(index_arg)}|{self_provider}|"
                f"{self_validation_model}|{self_model}|{self_system_message}"
            )

            # Hashing the key, decreasing size and potentially speeding up lookups
            # Deterministically creates the same hash for any given string
            hash_object = hashlib.sha256(cache_key.encode())
            key = hash_object.hexdigest()

            # If 'use_cache' is true, and the key is in the cache, return the cached result
            if self_use_cache:
                if key in cache:
                    completions += 1
                    cache_message = f" | Step | openai_cache() | Action | Completion success | | | | | CACHE | {key_arg} | {index_arg} | {completions} | {full_cache_dir}"
                    logger.info(cache_message)
                    return cache[key]

            # Otherwise, call the function and store the result in the cache
            result = await func(self, *args, **kwargs)
            cache[key] = result
            completions += 1
            api_message = f" | Step | {func.__name__}() | Action | Completion success | | | | | API | {key_arg} | {index_arg} | {completions}"
            logger.info(api_message)

            # Return the result
            return result

        # Return the wrapper function
        return wrapper

    # Return the decorator
    return decorator
