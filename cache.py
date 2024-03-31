from functions.logs import CacheLogger
from diskcache import Cache
import hashlib
import os

logger = CacheLogger().setup()


def cache_creator(parent_dir: str, cache_dir: str, max_mb_size: int):
    """Checks if a cache director exists, if not, creates it. Returns the cache object afterwards."""
    full_cache_dir = parent_dir + '\\' + cache_dir
    # Check if the specified cache directory exists, if not create it
    if not os.path.exists(full_cache_dir):
        os.makedirs(full_cache_dir)
        logger.info(f"'{full_cache_dir}' - Cache created")
    else:
        logger.info(f"'{full_cache_dir}' - Cache exists")

    # Create a DiskCache instance with the specified max_size
    max_size = max_mb_size * 1024 * 1024  # Convert megabytes to bytes
    cache = Cache(full_cache_dir, size_limit=max_size)
    return cache, full_cache_dir


def openai_cache(cache_dir, parent_dir='caches', max_mb_size: int = 1000):
    """Facilitates caching for openai in the handler when used as a decorator. Function result will be taken from the
    cache if available, otherwise the function will call the API. When max size is reached the 'least recent use'
    records will be culled."""
    # Finds or creates the cache folder and object, as well as returning the directory of the cache
    cache, full_cache_dir = cache_creator(parent_dir, cache_dir, max_mb_size)

    # Define the actual decorator function
    def decorator(func):
        completions = 0  # counter for the amount of completions

        async def wrapper(self, *args, **kwargs):
            nonlocal completions
            # Accessing the self variables from the class in which this decorator is used
            self_cache_identifier = self.cache_identifier
            self_use_cache = self.use_cache
            self_provider = self.provider
            self_model = self.model
            self_role = self.role
            self_request = self.request
            self_validation_model = self.validation_model.__name__

            # Accessing individual args and kwargs if they exist, for use in logs
            key_arg = args[2] if len(args) > 2 else kwargs.get('key')
            index_arg = args[3] if len(args) > 3 else kwargs.get('index')

            # Building a string key out the accessible information
            cache_key = f"{self_cache_identifier}|{str(key_arg)}|{str(index_arg)}|{self_provider}|" \
                        f"{self_validation_model}|{self_model}|{self_role}|{self_request}"
            # Hashing the key, decreasing size and potentially speeding up lookups
            # Deterministically creates the same hash for any given string
            hash_object = hashlib.sha256(cache_key.encode())
            key = hash_object.hexdigest()

            # If 'use_cache' is true, and the key is in the cache, return the cached result
            if self_use_cache:
                if key in cache:
                    completions += 1
                    logger.info(f"CACHE - Key: {key_arg} - Index: {index_arg} - "
                                f"#{completions} - '{full_cache_dir}'")
                    return cache[key]

            # Otherwise, call the function and store the result in the cache
            result = await func(self, *args, **kwargs)
            cache[key] = result
            completions += 1
            logger.info(f"API  - Key: {key_arg} - Index: {index_arg} - #{completions}")

            # Return the result
            return result

        # Return the wrapper function
        return wrapper

    # Return the decorator
    return decorator
