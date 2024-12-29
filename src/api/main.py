from fastapi import FastAPI, Query, HTTPException, Depends, status
from typing import List, Optional
from .database import DatabaseConnection
from .models import LandUseTransition, ScenarioInfo, TimeStep, County, DataVersion
import mysql.connector
from datetime import datetime

app = FastAPI(
    title="RPA Land Use Change API",
    description="""
    API for accessing RPA (Resources Planning Act) assessment land use change projection data.
    This API provides access to land use transition data across different climate and socioeconomic
    scenarios, time periods, and counties in the RPA region.
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

@app.get(
    "/health",
    summary="Health Check",
    description="Check if the API is operational",
    response_description="Returns the current health status and timestamp",
    status_code=status.HTTP_200_OK,
    tags=["System"]
)
async def health_check():
    """
    Perform a health check of the API service.
    
    Returns:
        dict: A dictionary containing the service status and current timestamp
    """
    return {"status": "healthy", "timestamp": datetime.now()}

@app.get(
    "/scenarios",
    response_model=List[ScenarioInfo],
    summary="List Scenarios",
    description="Retrieve all available climate-socioeconomic scenarios",
    response_description="List of available scenarios with their details",
    status_code=status.HTTP_200_OK,
    tags=["Data"]
)
async def get_scenarios():
    """
    Retrieve all available climate scenarios.
    
    Returns:
        List[ScenarioInfo]: A list of all available scenarios with their details
        
    Raises:
        HTTPException: If there's an error accessing the database
    """
    conn = None
    try:
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM scenarios")
        scenarios = cursor.fetchall()
        return scenarios
    except mysql.connector.Error as err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(err)}"
        )
    finally:
        if cursor:
            cursor.close()
        if conn:
            DatabaseConnection.close_connection(conn)

@app.get(
    "/transitions",
    response_model=List[LandUseTransition],
    summary="Get Land Use Transitions",
    description="Retrieve land use transitions with optional filtering by scenario, year, county, and land use types",
    response_description="List of land use transitions matching the specified criteria",
    status_code=status.HTTP_200_OK,
    tags=["Data"]
)
async def get_transitions(
    scenario: Optional[str] = Query(
        None,
        description="Filter by scenario name",
        example="CNRM_CM5_rcp45_ssp1"
    ),
    year: Optional[int] = Query(
        None,
        description="Filter by year",
        example=2030,
        ge=2020,
        le=2050
    ),
    fips: Optional[str] = Query(
        None,
        description="Filter by county FIPS code",
        example="36001",
        min_length=5,
        max_length=5
    ),
    from_use: Optional[str] = Query(
        None,
        description="Filter by original land use type",
        example="cr"
    ),
    to_use: Optional[str] = Query(
        None,
        description="Filter by new land use type",
        example="ur"
    )
):
    """
    Retrieve land use transitions with optional filtering.
    
    Args:
        scenario: Optional scenario name to filter by
        year: Optional year to filter by (must be between 2020 and 2050)
        fips: Optional 5-digit FIPS county code
        from_use: Optional original land use type
        to_use: Optional new land use type
        
    Returns:
        List[LandUseTransition]: A list of land use transitions matching the criteria
        
    Raises:
        HTTPException: If there's an error accessing the database
    """
    conn = None
    try:
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        query = """
        SELECT 
            s.scenario_name,
            CONCAT(ts.start_year, '-', ts.end_year) as time_period,
            t.fips_code,
            t.from_land_use,
            t.to_land_use,
            t.acres
        FROM land_use_transitions t
        JOIN scenarios s ON t.scenario_id = s.scenario_id
        JOIN time_steps ts ON t.time_step_id = ts.time_step_id
        WHERE 1=1
        """
        params = []

        if scenario:
            query += " AND s.scenario_name = %s"
            params.append(scenario)
        if year:
            query += " AND (ts.start_year <= %s AND ts.end_year >= %s)"
            params.extend([year, year])
        if fips:
            query += " AND t.fips_code = %s"
            params.append(fips)
        if from_use:
            query += " AND t.from_land_use = %s"
            params.append(from_use)
        if to_use:
            query += " AND t.to_land_use = %s"
            params.append(to_use)

        cursor.execute(query, tuple(params))
        transitions = cursor.fetchall()
        return transitions

    except mysql.connector.Error as err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(err)}"
        )
    finally:
        if cursor:
            cursor.close()
        if conn:
            DatabaseConnection.close_connection(conn)

@app.get(
    "/time-steps",
    response_model=List[TimeStep],
    summary="List Time Steps",
    description="Retrieve all available time periods",
    response_description="List of available time periods",
    status_code=status.HTTP_200_OK,
    tags=["Data"]
)
async def get_time_steps():
    """
    Retrieve all available time periods.
    
    Returns:
        List[TimeStep]: A list of all available time periods
        
    Raises:
        HTTPException: If there's an error accessing the database
    """
    conn = None
    try:
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM time_steps ORDER BY start_year")
        time_steps = cursor.fetchall()
        return time_steps
    except mysql.connector.Error as err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(err)}"
        )
    finally:
        if cursor:
            cursor.close()
        if conn:
            DatabaseConnection.close_connection(conn)

@app.get(
    "/counties",
    response_model=List[County],
    summary="List Counties",
    description="Retrieve all available counties",
    response_description="List of available counties by FIPS code",
    status_code=status.HTTP_200_OK,
    tags=["Data"]
)
async def get_counties():
    """
    Retrieve all available counties.
    
    Returns:
        List[County]: A list of all available counties
        
    Raises:
        HTTPException: If there's an error accessing the database
    """
    conn = None
    try:
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT DISTINCT fips_code FROM counties ORDER BY fips_code")
        counties = cursor.fetchall()
        return counties
    except mysql.connector.Error as err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(err)}"
        )
    finally:
        if cursor:
            cursor.close()
        if conn:
            DatabaseConnection.close_connection(conn) 