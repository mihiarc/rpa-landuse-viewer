from fastapi import FastAPI, Query, HTTPException, Depends
from typing import List, Optional
from .database import DatabaseConnection
from .models import LandUseTransition, ScenarioInfo, TimeStep, County, DataVersion
import mysql.connector
from datetime import datetime

app = FastAPI(
    title="RPA Land Use Change API",
    description="API for accessing RPA land use change projection data",
    version="1.0.0"
)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now()}

@app.get("/scenarios", response_model=List[ScenarioInfo])
async def get_scenarios():
    """Get all available scenarios"""
    conn = None
    try:
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM scenarios")
        scenarios = cursor.fetchall()
        return scenarios
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=f"Database error: {str(err)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            DatabaseConnection.close_connection(conn)

@app.get("/transitions", response_model=List[LandUseTransition])
async def get_transitions(
    scenario: Optional[str] = Query(None, description="Scenario name"),
    year: Optional[int] = Query(None, description="Year to filter by"),
    fips: Optional[str] = Query(None, description="FIPS code"),
    from_use: Optional[str] = Query(None, description="From land use type"),
    to_use: Optional[str] = Query(None, description="To land use type")
):
    """
    Get land use transitions with optional filtering
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
        raise HTTPException(status_code=500, detail=f"Database error: {str(err)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            DatabaseConnection.close_connection(conn)

@app.get("/time-steps", response_model=List[TimeStep])
async def get_time_steps():
    """Get all available time steps"""
    conn = None
    try:
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM time_steps ORDER BY start_year")
        time_steps = cursor.fetchall()
        return time_steps
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=f"Database error: {str(err)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            DatabaseConnection.close_connection(conn)

@app.get("/counties", response_model=List[County])
async def get_counties():
    """Get all available counties"""
    conn = None
    try:
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT DISTINCT fips_code FROM counties ORDER BY fips_code")
        counties = cursor.fetchall()
        return counties
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=f"Database error: {str(err)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            DatabaseConnection.close_connection(conn) 