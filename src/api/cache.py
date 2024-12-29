from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from redis import asyncio as aioredis
from typing import Optional
import os
from datetime import timedelta

# Cache expiration times (in seconds)
CACHE_EXPIRE_SCENARIOS = int(os.getenv('CACHE_EXPIRE_SCENARIOS', 3600))  # 1 hour
CACHE_EXPIRE_COUNTIES = int(os.getenv('CACHE_EXPIRE_COUNTIES', 86400))   # 24 hours
CACHE_EXPIRE_TIMESTEPS = int(os.getenv('CACHE_EXPIRE_TIMESTEPS', 86400)) # 24 hours
CACHE_EXPIRE_DEFAULT = int(os.getenv('CACHE_EXPIRE_DEFAULT', 300))       # 5 minutes

async def init_cache():
    """Initialize the Redis cache backend."""
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
    redis = aioredis.from_url(redis_url, encoding="utf8", decode_responses=True)
    FastAPICache.init(RedisBackend(redis), prefix="rpa_api_cache:")

def cache_key_builder(
    func,
    namespace: Optional[str] = "",
    *args,
    **kwargs,
):
    """Custom cache key builder that includes query parameters."""
    # Create base cache key from function name and namespace
    cache_key = f"{namespace}:{func.__module__}:{func.__name__}"
    
    # Add query parameters to cache key if they exist
    if kwargs:
        param_strings = [f"{k}={v}" for k, v in sorted(kwargs.items()) if v is not None]
        if param_strings:
            cache_key = f"{cache_key}:{','.join(param_strings)}"
    
    return cache_key 