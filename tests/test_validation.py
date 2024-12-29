import pytest
from fastapi import HTTPException, status
from src.api.validation import DataValidator, validator
from typing import Set

class TestDataValidator:
    """Tests for the DataValidator class."""

    def test_validate_scenario_valid(self):
        """Test validation of valid scenario."""
        validator._valid_scenarios = {"CNRM_CM5_rcp45_ssp1"}
        validator.validate_scenario("CNRM_CM5_rcp45_ssp1")  # Should not raise exception

    def test_validate_scenario_invalid(self):
        """Test validation of invalid scenario."""
        validator._valid_scenarios = {"CNRM_CM5_rcp45_ssp1"}
        with pytest.raises(HTTPException) as exc_info:
            validator.validate_scenario("invalid_scenario")
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid scenario" in str(exc_info.value.detail)

    def test_validate_year_valid(self):
        """Test validation of valid year."""
        validator._valid_years = {2020, 2030, 2040, 2050}
        validator.validate_year(2030)  # Should not raise exception

    def test_validate_year_invalid(self):
        """Test validation of invalid year."""
        validator._valid_years = {2020, 2030, 2040, 2050}
        with pytest.raises(HTTPException) as exc_info:
            validator.validate_year(1900)
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid year" in str(exc_info.value.detail)

    def test_validate_fips_valid(self):
        """Test validation of valid FIPS code."""
        validator._valid_fips = {"36001"}
        validator.validate_fips("36001")  # Should not raise exception

    def test_validate_fips_invalid_format(self):
        """Test validation of FIPS code with invalid format."""
        with pytest.raises(HTTPException) as exc_info:
            validator.validate_fips("123")
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "FIPS code must be exactly 5 digits" in str(exc_info.value.detail)

    def test_validate_fips_invalid_code(self):
        """Test validation of non-existent FIPS code."""
        validator._valid_fips = {"36001"}
        with pytest.raises(HTTPException) as exc_info:
            validator.validate_fips("99999")
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid FIPS code" in str(exc_info.value.detail)

    def test_validate_land_use_valid(self):
        """Test validation of valid land use type."""
        validator._valid_land_uses = {"cr", "ur"}
        validator.validate_land_use("cr", "from_use")  # Should not raise exception

    def test_validate_land_use_invalid(self):
        """Test validation of invalid land use type."""
        validator._valid_land_uses = {"cr", "ur"}
        with pytest.raises(HTTPException) as exc_info:
            validator.validate_land_use("invalid", "from_use")
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid from_use" in str(exc_info.value.detail)

def test_validation_middleware_valid_request(test_client):
    """Test validation middleware with valid request parameters."""
    # Setup valid data in validator
    validator._valid_scenarios = {"CNRM_CM5_rcp45_ssp1"}
    validator._valid_years = {2030}
    validator._valid_fips = {"36001"}
    validator._valid_land_uses = {"cr", "ur"}

    # Make request with valid parameters
    response = test_client.get(
        "/transitions",
        params={
            "scenario": "CNRM_CM5_rcp45_ssp1",
            "year": 2030,
            "fips": "36001",
            "from_use": "cr",
            "to_use": "ur"
        }
    )
    assert response.status_code == status.HTTP_200_OK

def test_validation_middleware_invalid_request(test_client):
    """Test validation middleware with invalid request parameters."""
    response = test_client.get(
        "/transitions",
        params={
            "scenario": "invalid_scenario",
            "year": 1900,
            "fips": "invalid",
            "from_use": "invalid",
            "to_use": "invalid"
        }
    )
    assert response.status_code == status.HTTP_400_BAD_REQUEST

def test_validation_middleware_non_transitions_endpoint(test_client):
    """Test validation middleware on non-transitions endpoints."""
    # These endpoints should not trigger validation
    response = test_client.get("/scenarios")
    assert response.status_code == status.HTTP_200_OK

    response = test_client.get("/counties")
    assert response.status_code == status.HTTP_200_OK

    response = test_client.get("/time-steps")
    assert response.status_code == status.HTTP_200_OK 