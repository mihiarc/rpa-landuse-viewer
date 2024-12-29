import sys
from pathlib import Path
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
import os
import logging
from fastapi_cache import FastAPICache

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add the project root to the Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.api.main import app
from src.api.database import DatabaseConnection

# Set environment to testing
os.environ["ENV"] = "testing"

@pytest_asyncio.fixture(scope="function")
async def initialized_app(initialized_cache):
    """Initialize FastAPI app with cache."""
    logger.info("Initializing FastAPI app for testing")
    return app

@pytest.fixture(scope="function")
def client(initialized_app):
    """Create a test client"""
    logger.info("Creating test client")
    return TestClient(initialized_app)

@pytest_asyncio.fixture(autouse=True)
async def setup_teardown():
    """Setup and teardown for each test"""
    logger.info("Setting up test environment")
    DatabaseConnection.reset_pool()
    yield
    logger.info("Tearing down test environment")
    DatabaseConnection.reset_pool()

@pytest.mark.asyncio
async def test_health_check(client):
    """Test the health check endpoint"""
    logger.info("Testing /health endpoint")
    response = client.get("/health")
    assert response.status_code == 200
    assert "status" in response.json()
    assert response.json()["status"] == "healthy"
    logger.info("Health check test passed")

@pytest.mark.asyncio
async def test_get_scenarios(client):
    """Test the scenarios endpoint"""
    logger.info("Testing /scenarios endpoint")
    response = client.get("/scenarios")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    logger.info(f"Scenarios test passed. Found {len(data)} scenarios")

@pytest.mark.asyncio
async def test_get_transitions_no_params(client):
    """Test the transitions endpoint without parameters"""
    logger.info("Testing /transitions endpoint without parameters")
    response = client.get("/transitions")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    logger.info(f"Transitions test (no params) passed. Found {len(data)} transitions")

@pytest.mark.asyncio
async def test_get_transitions_with_filters(client):
    """Test the transitions endpoint with filters"""
    params = {
        "scenario": "CNRM_CM5_rcp45_ssp1",
        "year": 2030,
        "fips": "36001"
    }
    logger.info(f"Testing /transitions endpoint with filters: {params}")
    response = client.get("/transitions", params=params)
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    logger.info(f"Found {len(data)} transitions matching filters")
    
    # If we got results, verify their structure
    if data:
        first_item = data[0]
        logger.info(f"Verifying structure of first result: {first_item}")
        assert "scenario_name" in first_item
        assert "time_period" in first_item
        assert "fips_code" in first_item
        assert "from_land_use" in first_item
        assert "to_land_use" in first_item
        assert "acres" in first_item
    logger.info("Transitions test with filters passed")

@pytest.mark.asyncio
async def test_get_time_steps(client):
    """Test the time-steps endpoint"""
    logger.info("Testing /time-steps endpoint")
    response = client.get("/time-steps")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    logger.info(f"Time steps test passed. Found {len(data)} time steps")

@pytest.mark.asyncio
async def test_get_counties(client):
    """Test the counties endpoint"""
    logger.info("Testing /counties endpoint")
    response = client.get("/counties")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    logger.info(f"Counties test passed. Found {len(data)} counties") 