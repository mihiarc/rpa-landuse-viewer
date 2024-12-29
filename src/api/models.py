from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

class LandUseTransition(BaseModel):
    scenario_name: str
    time_period: str
    fips_code: str
    from_land_use: str
    to_land_use: str
    acres: float

class ScenarioInfo(BaseModel):
    scenario_id: int
    scenario_name: str
    gcm: str
    rcp: str
    ssp: str

class TimeStep(BaseModel):
    time_step_id: int
    start_year: int
    end_year: int

class County(BaseModel):
    fips_code: str

class DataVersion(BaseModel):
    version_id: int
    import_date: datetime
    data_hash: str
    file_path: str
    is_active: bool 