"""
Data loading utilities for the RPA Land Use Viewer.
"""

import streamlit as st
import pandas as pd
from src.db.database import DatabaseConnection

@st.cache_data
def get_scenarios():
    """Get available scenarios from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT scenario_id, scenario_name, gcm, rcp, ssp FROM scenarios ORDER BY scenario_name")
    scenarios = [{'id': row[0], 'name': row[1], 'gcm': row[2], 'rcp': row[3], 'ssp': row[4]} for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return scenarios

@st.cache_data
def get_years():
    """Get a list of all available years from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT start_year FROM time_steps UNION SELECT DISTINCT end_year FROM time_steps ORDER BY 1")
    years = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return years

@st.cache_data
def get_land_use_types():
    """Get a list of all land use types from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT from_land_use AS land_use_type 
        FROM land_use_transitions
        UNION
        SELECT DISTINCT to_land_use AS land_use_type
        FROM land_use_transitions
        ORDER BY land_use_type
    """)
    land_use_types = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return land_use_types

@st.cache_data
def get_states():
    """Get a list of all states from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT state_fips, state_name, state_abbr FROM states ORDER BY state_name")
    states = [{'fips': row[0], 'name': row[1], 'abbr': row[2]} for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return states

@st.cache_data
def get_counties_by_state(state_fips=None):
    """Get a list of counties, optionally filtered by state."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    if state_fips:
        cursor.execute("""
            SELECT county_fips, county_name, state_fips, state_name, state_abbr
            FROM counties_by_state
            WHERE state_fips = ?
            ORDER BY county_name
        """, (state_fips,))
    else:
        cursor.execute("""
            SELECT county_fips, county_name, state_fips, state_name, state_abbr
            FROM counties_by_state
            ORDER BY state_name, county_name
        """)
    
    counties = [{'fips': row[0], 'name': row[1], 'state_fips': row[2], 
                'state_name': row[3], 'state_abbr': row[4]} for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return counties

@st.cache_data
def get_database_stats():
    """Get statistics about the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    stats = {}
    
    # Count scenarios
    cursor.execute("SELECT COUNT(*) FROM scenarios")
    stats['scenario_count'] = cursor.fetchone()[0]
    
    # Count regions (counties)
    cursor.execute("SELECT COUNT(*) FROM counties")
    stats['region_count'] = cursor.fetchone()[0]
    
    # For region types, we only have counties in this database
    stats['region_type_count'] = 1
    
    # Count unique land use types (combining both from and to land uses)
    cursor.execute("""
        SELECT COUNT(DISTINCT land_use_type) 
        FROM (
            SELECT from_land_use AS land_use_type FROM land_use_transitions
            UNION
            SELECT to_land_use AS land_use_type FROM land_use_transitions
        )
    """)
    stats['land_use_type_count'] = cursor.fetchone()[0]
    
    # Count unique time steps
    cursor.execute("SELECT COUNT(*) FROM time_steps")
    stats['year_count'] = cursor.fetchone()[0]
    
    # Get total data points
    cursor.execute("SELECT COUNT(*) FROM land_use_transitions")
    stats['data_point_count'] = cursor.fetchone()[0]
    
    # Get min and max years
    cursor.execute("SELECT MIN(start_year), MAX(end_year) FROM time_steps")
    min_year, max_year = cursor.fetchone()
    stats['year_range'] = (min_year, max_year)
    
    DatabaseConnection.close_connection(conn)
    return stats

@st.cache_data
def get_national_summary(scenario_id=None, year=None, aggregation_level="national"):
    """
    Get a summary of land use data at different aggregation levels.
    
    Args:
        scenario_id: Optional scenario ID to filter results
        year: Optional year to filter results
        aggregation_level: The level of aggregation - "national", "state", or "county"
    
    Returns:
        DataFrame containing the aggregated land use data
    """
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    # First, check if the scenario has data
    if scenario_id:
        cursor.execute(
            "SELECT COUNT(*) FROM land_use_transitions WHERE scenario_id = ?", 
            (scenario_id,)
        )
        count = cursor.fetchone()[0]
        if count == 0:
            # If no data for this scenario, find a scenario that has data
            cursor.execute(
                """
                SELECT scenario_id, COUNT(*) as count 
                FROM land_use_transitions 
                GROUP BY scenario_id 
                ORDER BY count DESC 
                LIMIT 1
                """
            )
            result = cursor.fetchone()
            if result:
                scenario_id = result[0]
                st.warning(f"Selected scenario has no data. Using scenario ID {scenario_id} instead.")
    
    # Find the time_step(s) that include this year (either as start or end year)
    year_filter = ""
    params = []
    if year:
        year_filter = "AND (ts.start_year = ? OR ts.end_year = ?)"
        params.extend([year, year])
    
    # This query aggregates data to get current land use state by calculating
    # net changes from transitions and assuming transitions happen at year boundary
    if aggregation_level == "national":
        query = """
        WITH transitions_summary AS (
            SELECT 
                lut.scenario_id,
                s.scenario_name,
                CASE 
                    WHEN ts.end_year = ? THEN ts.end_year
                    ELSE ts.start_year
                END as year,
                lut.from_land_use as land_use_type,
                -SUM(lut.acres) as net_change
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            WHERE 1=1
            {scenario_filter}
            {year_filter}
            GROUP BY lut.scenario_id, s.scenario_name, year, lut.from_land_use
            
            UNION ALL
            
            SELECT 
                lut.scenario_id,
                s.scenario_name,
                CASE 
                    WHEN ts.end_year = ? THEN ts.end_year
                    ELSE ts.start_year
                END as year,
                lut.to_land_use as land_use_type,
                SUM(lut.acres) as net_change
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            WHERE 1=1
            {scenario_filter}
            {year_filter}
            GROUP BY lut.scenario_id, s.scenario_name, year, lut.to_land_use
        )
        
        SELECT 
            scenario_id,
            scenario_name,
            year,
            land_use_type,
            SUM(net_change) as total_acres,
            NULL as state_fips,
            NULL as state_name,
            NULL as state_abbr
        FROM transitions_summary
        GROUP BY scenario_id, scenario_name, year, land_use_type
        ORDER BY scenario_name, year, land_use_type
        """
    elif aggregation_level == "state":
        query = """
        WITH state_transitions_summary AS (
            SELECT 
                lut.scenario_id,
                s.scenario_name,
                CASE 
                    WHEN ts.end_year = ? THEN ts.end_year
                    ELSE ts.start_year
                END as year,
                SUBSTR(lut.fips_code, 1, 2) as state_fips,
                st.state_name,
                st.state_abbr,
                lut.from_land_use as land_use_type,
                -SUM(lut.acres) as net_change
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            JOIN states st ON SUBSTR(lut.fips_code, 1, 2) = st.state_fips
            WHERE 1=1
            {scenario_filter}
            {year_filter}
            GROUP BY lut.scenario_id, s.scenario_name, year, state_fips, st.state_name, st.state_abbr, lut.from_land_use
            
            UNION ALL
            
            SELECT 
                lut.scenario_id,
                s.scenario_name,
                CASE 
                    WHEN ts.end_year = ? THEN ts.end_year
                    ELSE ts.start_year
                END as year,
                SUBSTR(lut.fips_code, 1, 2) as state_fips,
                st.state_name,
                st.state_abbr,
                lut.to_land_use as land_use_type,
                SUM(lut.acres) as net_change
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            JOIN states st ON SUBSTR(lut.fips_code, 1, 2) = st.state_fips
            WHERE 1=1
            {scenario_filter}
            {year_filter}
            GROUP BY lut.scenario_id, s.scenario_name, year, state_fips, st.state_name, st.state_abbr, lut.to_land_use
        )
        
        SELECT 
            scenario_id,
            scenario_name,
            year,
            land_use_type,
            SUM(net_change) as total_acres,
            state_fips,
            state_name,
            state_abbr
        FROM state_transitions_summary
        GROUP BY scenario_id, scenario_name, year, state_fips, state_name, state_abbr, land_use_type
        ORDER BY scenario_name, year, state_name, land_use_type
        """
    else:  # county level
        query = """
        WITH county_transitions_summary AS (
            SELECT 
                lut.scenario_id,
                s.scenario_name,
                CASE 
                    WHEN ts.end_year = ? THEN ts.end_year
                    ELSE ts.start_year
                END as year,
                lut.fips_code as county_fips,
                c.county_name,
                SUBSTR(lut.fips_code, 1, 2) as state_fips,
                st.state_name,
                st.state_abbr,
                lut.from_land_use as land_use_type,
                -SUM(lut.acres) as net_change
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            JOIN counties c ON lut.fips_code = c.fips_code
            JOIN states st ON SUBSTR(lut.fips_code, 1, 2) = st.state_fips
            WHERE 1=1
            {scenario_filter}
            {year_filter}
            GROUP BY lut.scenario_id, s.scenario_name, year, county_fips, c.county_name, state_fips, st.state_name, st.state_abbr, lut.from_land_use
            
            UNION ALL
            
            SELECT 
                lut.scenario_id,
                s.scenario_name,
                CASE 
                    WHEN ts.end_year = ? THEN ts.end_year
                    ELSE ts.start_year
                END as year,
                lut.fips_code as county_fips,
                c.county_name,
                SUBSTR(lut.fips_code, 1, 2) as state_fips,
                st.state_name,
                st.state_abbr,
                lut.to_land_use as land_use_type,
                SUM(lut.acres) as net_change
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            JOIN counties c ON lut.fips_code = c.fips_code
            JOIN states st ON SUBSTR(lut.fips_code, 1, 2) = st.state_fips
            WHERE 1=1
            {scenario_filter}
            {year_filter}
            GROUP BY lut.scenario_id, s.scenario_name, year, county_fips, c.county_name, state_fips, st.state_name, st.state_abbr, lut.to_land_use
        )
        
        SELECT 
            scenario_id,
            scenario_name,
            year,
            land_use_type,
            SUM(net_change) as total_acres,
            county_fips,
            county_name,
            state_fips,
            state_name,
            state_abbr
        FROM county_transitions_summary
        GROUP BY scenario_id, scenario_name, year, county_fips, county_name, state_fips, state_name, state_abbr, land_use_type
        ORDER BY scenario_name, year, state_name, county_name, land_use_type
        """
    
    # Add scenario filter if specified
    scenario_filter = ""
    if scenario_id:
        scenario_filter = "AND lut.scenario_id = ?"
        params.append(scenario_id)
    
    # Format the query with the appropriate filters
    query = query.format(
        scenario_filter=scenario_filter,
        year_filter=year_filter
    )
    
    # Add the year parameters for both subqueries
    # We use the first available year as a base year if none specified
    selected_year = year
    if not selected_year:
        cursor.execute("SELECT MIN(start_year) FROM time_steps")
        selected_year = cursor.fetchone()[0]
    
    # Add year parameters (needed twice for the two subqueries)
    full_params = [selected_year] + params + [selected_year] + params
    
    cursor.execute(query, full_params)
    data = cursor.fetchall()
    
    # Get column names
    columns = [desc[0] for desc in cursor.description]
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    
    # Handle empty results
    if df.empty:
        st.warning(f"No data found for the selected filters. Try different criteria.")
        # Create empty dataframe with expected columns
        df = pd.DataFrame(columns=columns)
    
    DatabaseConnection.close_connection(conn)
    return df 