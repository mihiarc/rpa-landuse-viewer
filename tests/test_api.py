import sys
from pathlib import Path
import pytest
from fastapi.testclient import TestClient
import os

# Add the project root to the Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.api.main import app
from src.api.database import DatabaseConnection

# Set environment to testing
os.environ["ENV"] = "testing"

@pytest.fixture(scope="module")
def client():
    """Create a test client"""
    return TestClient(app)

@pytest.fixture(autouse=True)
def setup_teardown():
    """Setup and teardown for each test"""
    DatabaseConnection.reset_pool()
    yield
    DatabaseConnection.reset_pool()

@pytest.mark.asyncio
async def test_health_check(client):
    """Test the health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    assert "status" in response.json()
    assert response.json()["status"] == "healthy"

@pytest.mark.asyncio
async def test_get_scenarios(client):
    """Test the scenarios endpoint"""
    response = client.get("/scenarios")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

@pytest.mark.asyncio
async def test_get_transitions_no_params(client):
    """Test the transitions endpoint without parameters"""
    response = client.get("/transitions")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

@pytest.mark.asyncio
async def test_get_transitions_with_filters(client):
    """Test the transitions endpoint with filters"""
    params = {
        "scenario": "GFDL-ESM4_ssp245_2",
        "year": 2030,
        "fips": "25001"
    }
    response = client.get("/transitions", params=params)
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    
    # If we got results, verify their structure
    if response.json():
        first_item = response.json()[0]
        assert "scenario_name" in first_item
        assert "time_period" in first_item
        assert "fips_code" in first_item
        assert "from_land_use" in first_item
        assert "to_land_use" in first_item
        assert "acres" in first_item

@pytest.mark.asyncio
async def test_get_time_steps(client):
    """Test the time-steps endpoint"""
    response = client.get("/time-steps")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

@pytest.mark.asyncio
async def test_get_counties(client):
    """Test the counties endpoint"""
    response = client.get("/counties")
    assert response.status_code == 200
    assert isinstance(response.json(), list) 