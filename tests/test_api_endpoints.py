import pytest
from fastapi import status
from typing import Dict, Any

def test_health_check(test_client):
    """Test the health check endpoint."""
    response = test_client.get("/health")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["status"] == "healthy"
    assert "timestamp" in data

def test_get_scenarios(test_client, sample_scenario):
    """Test retrieving scenarios."""
    response = test_client.get("/scenarios")
    assert response.status_code == status.HTTP_200_OK
    scenarios = response.json()
    assert isinstance(scenarios, list)
    # Verify response structure matches our model
    if scenarios:
        scenario = scenarios[0]
        assert all(key in scenario for key in sample_scenario.keys())

def test_get_time_steps(test_client, sample_time_step):
    """Test retrieving time steps."""
    response = test_client.get("/time-steps")
    assert response.status_code == status.HTTP_200_OK
    time_steps = response.json()
    assert isinstance(time_steps, list)
    # Verify response structure matches our model
    if time_steps:
        time_step = time_steps[0]
        assert all(key in time_step for key in sample_time_step.keys())

def test_get_counties(test_client, sample_county):
    """Test retrieving counties."""
    response = test_client.get("/counties")
    assert response.status_code == status.HTTP_200_OK
    counties = response.json()
    assert isinstance(counties, list)
    # Verify response structure matches our model
    if counties:
        county = counties[0]
        assert all(key in county for key in sample_county.keys())

class TestTransitionsEndpoint:
    """Tests for the transitions endpoint."""

    def test_get_transitions_no_params(self, test_client):
        """Test retrieving transitions without parameters."""
        response = test_client.get("/transitions")
        assert response.status_code == status.HTTP_200_OK
        transitions = response.json()
        assert isinstance(transitions, list)

    def test_get_transitions_with_scenario(self, test_client, sample_transition):
        """Test retrieving transitions filtered by scenario."""
        response = test_client.get(
            "/transitions",
            params={"scenario": sample_transition["scenario_name"]}
        )
        assert response.status_code == status.HTTP_200_OK
        transitions = response.json()
        assert isinstance(transitions, list)
        # Verify filtered results
        if transitions:
            assert all(t["scenario_name"] == sample_transition["scenario_name"] 
                      for t in transitions)

    def test_get_transitions_with_year(self, test_client):
        """Test retrieving transitions filtered by year."""
        response = test_client.get("/transitions", params={"year": 2030})
        assert response.status_code == status.HTTP_200_OK
        transitions = response.json()
        assert isinstance(transitions, list)

    def test_get_transitions_with_fips(self, test_client, sample_transition):
        """Test retrieving transitions filtered by FIPS code."""
        response = test_client.get(
            "/transitions",
            params={"fips": sample_transition["fips_code"]}
        )
        assert response.status_code == status.HTTP_200_OK
        transitions = response.json()
        assert isinstance(transitions, list)
        # Verify filtered results
        if transitions:
            assert all(t["fips_code"] == sample_transition["fips_code"] 
                      for t in transitions)

    def test_get_transitions_with_land_use(self, test_client, sample_transition):
        """Test retrieving transitions filtered by land use types."""
        response = test_client.get(
            "/transitions",
            params={
                "from_use": sample_transition["from_land_use"],
                "to_use": sample_transition["to_land_use"]
            }
        )
        assert response.status_code == status.HTTP_200_OK
        transitions = response.json()
        assert isinstance(transitions, list)
        # Verify filtered results
        if transitions:
            assert all(
                t["from_land_use"] == sample_transition["from_land_use"] and
                t["to_land_use"] == sample_transition["to_land_use"]
                for t in transitions
            )

    def test_invalid_scenario(self, test_client):
        """Test error handling for invalid scenario."""
        response = test_client.get("/transitions", params={"scenario": "invalid_scenario"})
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_invalid_year(self, test_client):
        """Test error handling for invalid year."""
        response = test_client.get("/transitions", params={"year": 1900})
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_invalid_fips(self, test_client):
        """Test error handling for invalid FIPS code."""
        response = test_client.get("/transitions", params={"fips": "invalid"})
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_invalid_land_use(self, test_client):
        """Test error handling for invalid land use type."""
        response = test_client.get("/transitions", params={"from_use": "invalid"})
        assert response.status_code == status.HTTP_400_BAD_REQUEST 