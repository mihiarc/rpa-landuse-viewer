"""
Shared utility functions for RPA Land Use Viewer.

This module contains common functions used across multiple pages in the Streamlit app.
"""

import streamlit as st
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple, Union
from pathlib import Path
import os
from src.db.database import DatabaseConnection

# ----------------- Data Loading Functions -----------------

@st.cache_data(ttl=3600)  # Cache data for 1 hour
def get_scenarios() -> List[Dict[str, Any]]:
    """Get available scenarios from the database.
    
    Returns:
        List[Dict[str, Any]]: List of scenario dictionaries with id, name, gcm, rcp, and ssp keys
    """
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT scenario_id, scenario_name, gcm, rcp, ssp FROM scenarios ORDER BY scenario_name")
    scenarios = [{'id': row[0], 'name': row[1], 'gcm': row[2], 'rcp': row[3], 'ssp': row[4]} for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return scenarios

@st.cache_data(ttl=3600)
def get_years() -> List[int]:
    """Get a list of all available years from the database.
    
    Returns:
        List[int]: List of available years
    """
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT year FROM land_use_summary ORDER BY year")
    years = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return years

@st.cache_data(ttl=3600)
def get_land_use_types() -> List[str]:
    """Get a list of all land use types from the database.
    
    Returns:
        List[str]: List of land use type names
    """
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT from_land_use as land_use_type 
        FROM land_use_transitions
        UNION
        SELECT DISTINCT to_land_use as land_use_type
        FROM land_use_transitions
        ORDER BY land_use_type
    """)
    land_use_types = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return land_use_types

@st.cache_data(ttl=3600)
def get_states() -> List[Dict[str, Any]]:
    """Get a list of all states from the database.
    
    Returns:
        List[Dict[str, Any]]: List of state dictionaries with fips and name keys
    """
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT state_fips, state_name FROM states ORDER BY state_name")
    states = [{'fips': row[0], 'name': row[1]} for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return states

@st.cache_data(ttl=3600)
def get_counties_by_state(state_fips: str) -> List[Dict[str, Any]]:
    """Get a list of counties for a specific state.
    
    Args:
        state_fips (str): The FIPS code of the state
        
    Returns:
        List[Dict[str, Any]]: List of county dictionaries with fips and name keys
    """
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    # Use SUBSTRING to extract the state part from the county FIPS code
    cursor.execute("""
        SELECT fips_code, county_name 
        FROM counties 
        WHERE SUBSTRING(fips_code, 1, 2) = ? 
        ORDER BY county_name
    """, [state_fips])
    counties = [{'fips': row[0], 'name': row[1]} for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return counties

@st.cache_data(ttl=1800)  # Cache for 30 minutes
def get_national_summary(scenario_id: int, year: int) -> pd.DataFrame:
    """Load scenario data for a specific scenario and year.
    
    Args:
        scenario_id (int): The ID of the scenario
        year (int): The year to get data for
        
    Returns:
        pd.DataFrame: A DataFrame containing the scenario data
    """
    conn = DatabaseConnection.get_connection()
    
    query = """
    SELECT * FROM land_use_summary
    WHERE scenario_id = ? AND year = ?
    """
    
    # Use the execute_pandas_query method from DatabaseConnection
    df = DatabaseConnection.execute_pandas_query(query, [scenario_id, year])
    
    return df

# ----------------- UI Helper Functions -----------------

def load_css():
    """Load custom CSS styles from file.
    
    Loads and applies CSS styles from src/styles/style.css if it exists.
    """
    css_file = Path("src/styles/style.css")
    if css_file.exists():
        with open(css_file) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

def set_page_config(title: str, icon: str = "🌆", layout: str = "wide"):
    """Set the page configuration for the app.
    
    Args:
        title (str): The title of the page
        icon (str, optional): The icon to display. Defaults to "🌆".
        layout (str, optional): The layout of the page. Defaults to "wide".
    """
    st.set_page_config(
        page_title=f"RPA Land Use Viewer - {title}",
        page_icon=icon,
        layout=layout,
        initial_sidebar_state="expanded"
    )

def format_metric_value(value: Union[int, float], metric_name: str) -> str:
    """Format a metric value for display.
    
    Args:
        value (Union[int, float]): The value to format
        metric_name (str): The name of the metric
        
    Returns:
        str: The formatted value
    """
    if isinstance(value, (int, float)):
        if metric_name.startswith('percent'):
            formatted_value = f"{value:.1f}%"
        elif value >= 1_000_000:
            formatted_value = f"{value/1_000_000:.2f}M"
        elif value >= 1_000:
            formatted_value = f"{value/1_000:.1f}K"
        else:
            formatted_value = f"{value:,.0f}"
    else:
        formatted_value = str(value)
    
    return formatted_value

def display_metrics(metrics: Dict[str, Any]):
    """Display a row of formatted metrics.
    
    Args:
        metrics (Dict[str, Any]): Dictionary of metric names and values
    """
    # Create a row of metrics
    metric_cols = st.columns(len(metrics))
    for i, (metric_name, metric_value) in enumerate(metrics.items()):
        # Format the metric name to be more readable
        formatted_name = metric_name.replace('_', ' ').title()
        # Format the value
        formatted_value = format_metric_value(metric_value, metric_name)
        
        metric_cols[i].metric(formatted_name, formatted_value) 