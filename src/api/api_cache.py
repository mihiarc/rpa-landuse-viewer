from fastapi import Depends
from fastapi_cache import FastAPICache
from fastapi_cache.decorator import cache
from typing import Optional, Callable, Any
import logging

logger = logging.getLogger(__name__)

# Cache expiration times (in seconds)
CACHE_EXPIRE_SCENARIOS = 3600  # 1 hour
CACHE_EXPIRE_COUNTIES = 3600   # 1 hour
CACHE_EXPIRE_TIMESTEPS = 3600  # 1 hour

def cache_key_builder(
    func: Callable,
    namespace: str = "",
    *args: Any,
    **kwargs: Any,
) -> str:
    """Build cache key based on function name and arguments."""
    return f"{namespace}:{func.__module__}:{func.__name__}:{args}:{kwargs}"

def cached(
    *, 
    expire: int,
    namespace: Optional[str] = None,
) -> Callable:
    """
    Custom cache decorator that uses the cache_key_builder.
    
    Args:
        expire: Cache expiration time in seconds
        namespace: Optional namespace for the cache key
    """
    def decorator(func: Callable) -> Callable:
        return cache(
            expire=expire,
            namespace=namespace,
            key_builder=cache_key_builder
        )(func)
    return decorator 