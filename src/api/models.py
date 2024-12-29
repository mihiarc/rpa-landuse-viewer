from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

class LandUseTransition(BaseModel):
    """
    Represents a land use transition between two time periods.
    """
    scenario_name: str = Field(..., description="Name of the scenario", example="CNRM_CM5_rcp45_ssp1")
    time_period: str = Field(..., description="Time period of the transition (e.g., '2020-2030')", example="2020-2030")
    fips_code: str = Field(..., description="FIPS county code", example="36001")
    from_land_use: str = Field(..., description="Original land use type", example="cr")
    to_land_use: str = Field(..., description="New land use type", example="ur")
    acres: float = Field(..., description="Area of land transitioned in acres", example=150.5)

    class Config:
        schema_extra = {
            "description": "Represents a land use transition between two time periods",
            "example": {
                "scenario_name": "CNRM_CM5_rcp45_ssp1",
                "time_period": "2020-2030",
                "fips_code": "36001",
                "from_land_use": "cr",
                "to_land_use": "ur",
                "acres": 150.5
            }
        }

class ScenarioInfo(BaseModel):
    """
    Information about a climate scenario including model and pathway details.
    """
    scenario_id: int = Field(..., description="Unique identifier for the scenario")
    scenario_name: str = Field(..., description="Full name of the scenario", example="CNRM_CM5_rcp45_ssp1")
    gcm: str = Field(..., description="Global Climate Model used", example="CNRM_CM5")
    rcp: str = Field(..., description="Representative Concentration Pathway", example="rcp45")
    ssp: str = Field(..., description="Shared Socioeconomic Pathway", example="ssp1")

    class Config:
        schema_extra = {
            "description": "Climate scenario information",
            "example": {
                "scenario_id": 1,
                "scenario_name": "CNRM_CM5_rcp45_ssp1",
                "gcm": "CNRM_CM5",
                "rcp": "rcp45",
                "ssp": "ssp1"
            }
        }

class TimeStep(BaseModel):
    """
    Represents a time period in the projection data.
    """
    time_step_id: int = Field(..., description="Unique identifier for the time step")
    start_year: int = Field(..., description="Start year of the time period", example=2020)
    end_year: int = Field(..., description="End year of the time period", example=2030)

    class Config:
        schema_extra = {
            "description": "Time period information",
            "example": {
                "time_step_id": 1,
                "start_year": 2020,
                "end_year": 2030
            }
        }

class County(BaseModel):
    """
    County information identified by FIPS code.
    """
    fips_code: str = Field(..., description="FIPS county code", example="36001")

    class Config:
        schema_extra = {
            "description": "County information",
            "example": {
                "fips_code": "36001"
            }
        }

class DataVersion(BaseModel):
    """
    Information about a data version import.
    """
    version_id: int = Field(..., description="Unique identifier for the data version")
    import_date: datetime = Field(..., description="Date and time of data import")
    data_hash: str = Field(..., description="Hash of the imported data for verification")
    file_path: str = Field(..., description="Path to the source data file")
    is_active: bool = Field(..., description="Whether this is the currently active data version")

    class Config:
        schema_extra = {
            "description": "Data version information",
            "example": {
                "version_id": 1,
                "import_date": "2023-12-28T12:00:00",
                "data_hash": "abc123def456",
                "file_path": "/data/imports/2023-12-28.json",
                "is_active": True
            }
        } 