import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from src.api.main import app
import os
import mysql.connector
from typing import Generator, Dict, Any
import json
import asyncio
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from src.api.cache import get_redis_connection
from src.api.validation import validator

# Test database configuration
TEST_DB_CONFIG = {
    'host': os.getenv('TEST_DB_HOST', 'localhost'),
    'user': os.getenv('TEST_DB_USER', 'test_user'),
    'password': os.getenv('TEST_DB_PASSWORD', 'test_password'),
    'database': os.getenv('TEST_DB_NAME', 'rpa_test_db')
}

@pytest_asyncio.fixture(scope="function")
async def initialized_cache():
    """Initialize FastAPI cache with Redis backend."""
    # Set environment to testing
    os.environ["ENV"] = "testing"
    
    async with get_redis_connection() as redis:
        # Initialize cache based on whether FastAPICache.init is async or not
        if asyncio.iscoroutinefunction(FastAPICache.init):
            await FastAPICache.init(
                RedisBackend(redis),
                prefix="fastapi-cache"
            )
        else:
            def init_sync():
                FastAPICache.init(
                    RedisBackend(redis),
                    prefix="fastapi-cache"
                )
            await asyncio.to_thread(init_sync)
            
        await validator.initialize()
        yield
        
        # Cleanup
        if hasattr(FastAPICache, 'close'):
            await FastAPICache.close()

@pytest.fixture(autouse=True)
def mock_rate_limit(monkeypatch):
    """Disable rate limiting for tests."""
    def mock_limit(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    monkeypatch.setattr("src.api.rate_limit.limiter.limit", mock_limit)

@pytest.fixture
def test_client() -> Generator:
    """Create a test client for the FastAPI application."""
    with TestClient(app) as client:
        yield client

@pytest.fixture
def sample_scenario() -> Dict[str, Any]:
    """Return a sample scenario for testing."""
    return {
        "scenario_id": 1,
        "scenario_name": "CNRM_CM5_rcp45_ssp1",
        "gcm": "CNRM_CM5",
        "rcp": "rcp45",
        "ssp": "ssp1"
    }

@pytest.fixture
def sample_transition() -> Dict[str, Any]:
    """Return a sample land use transition for testing."""
    return {
        "scenario_name": "CNRM_CM5_rcp45_ssp1",
        "time_period": "2020-2030",
        "fips_code": "36001",
        "from_land_use": "cr",
        "to_land_use": "ur",
        "acres": 150.5
    }

@pytest.fixture
def sample_time_step() -> Dict[str, Any]:
    """Return a sample time step for testing."""
    return {
        "time_step_id": 1,
        "start_year": 2020,
        "end_year": 2030
    }

@pytest.fixture
def sample_county() -> Dict[str, Any]:
    """Return a sample county for testing."""
    return {
        "fips_code": "36001"
    } 