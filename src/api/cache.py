from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from redis import asyncio as aioredis
from fastapi import Depends
from contextlib import asynccontextmanager
from .config import Config
import logging
from functools import wraps
import asyncio

logger = logging.getLogger(__name__)

@asynccontextmanager
async def get_redis_connection():
    """Create and manage Redis connection lifecycle."""
    redis = None
    try:
        # Get and validate Redis configuration
        redis_config = Config.get_redis_config()
        if not redis_config:
            raise ValueError("Failed to retrieve Redis configuration")
        
        # Validate required configuration fields
        required_fields = ['host', 'port']
        missing_fields = [field for field in required_fields if field not in redis_config]
        if missing_fields:
            raise ValueError(f"Missing required Redis configuration fields: {', '.join(missing_fields)}")
        
        # Build the connection string
        redis_url = f"redis://{redis_config['host']}:{redis_config['port']}"
        if 'db' in redis_config:
            redis_url += f"/{redis_config['db']}"
            
        logger.info(f"Connecting to Redis at {redis_url}")
        
        # Initialize Redis client
        redis = aioredis.from_url(
            redis_url,
            encoding="utf8",
            decode_responses=True
        )
        
        # Test the connection
        await redis.ping()
        logger.info("Redis connection established successfully")
        yield redis
    except (ValueError, aioredis.RedisError) as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise
    finally:
        if redis:
            try:
                await redis.aclose()
                logger.info("Redis connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")

async def initialize_cache(redis: aioredis.Redis):
    """Initialize FastAPI cache with provided Redis client."""
    try:
        # Create a new event loop for sync operations if needed
        def init_sync():
            FastAPICache.init(
                RedisBackend(redis), 
                prefix="fastapi-cache"
            )
        
        # Run sync initialization in a thread if needed
        if asyncio.iscoroutinefunction(FastAPICache.init):
            await FastAPICache.init(
                RedisBackend(redis), 
                prefix="fastapi-cache"
            )
        else:
            await asyncio.to_thread(init_sync)
            
        logger.info("FastAPI cache initialized successfully")
        return FastAPICache
    except Exception as e:
        logger.error(f"Failed to initialize FastAPI cache: {e}")
        raise

async def get_cache(redis: aioredis.Redis = Depends(get_redis_connection)):
    """Dependency for getting initialized cache."""
    return await initialize_cache(redis) 