import pytest
from fastapi.testclient import TestClient
from src.api.main import app
import os
import mysql.connector
from typing import Generator, Dict, Any
import json

# Test database configuration
TEST_DB_CONFIG = {
    'host': os.getenv('TEST_DB_HOST', 'localhost'),
    'user': os.getenv('TEST_DB_USER', 'test_user'),
    'password': os.getenv('TEST_DB_PASSWORD', 'test_password'),
    'database': os.getenv('TEST_DB_NAME', 'rpa_test_db')
}

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

@pytest.fixture(autouse=True)
async def setup_test_cache():
    """Setup test cache configuration."""
    os.environ['REDIS_URL'] = 'redis://localhost:6379/1'  # Use database 1 for testing
    yield
    # Cleanup will happen automatically

@pytest.fixture(autouse=True)
def mock_rate_limit(monkeypatch):
    """Disable rate limiting for tests."""
    def mock_limit(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    monkeypatch.setattr("src.api.rate_limit.limiter.limit", mock_limit) 