"""
Session state management for the RPA Land Use Viewer.
"""

import streamlit as st
from typing import Dict, Any, List, Optional

def initialize_session_state():
    """Initialize the session state variables if they don't exist."""
    if 'initialized' not in st.session_state:
        # Filters
        if 'selected_scenario' not in st.session_state:
            st.session_state.selected_scenario = None
        
        if 'selected_year' not in st.session_state:
            st.session_state.selected_year = None
        
        if 'selected_state' not in st.session_state:
            st.session_state.selected_state = None
            
        if 'selected_county' not in st.session_state:
            st.session_state.selected_county = None
            
        if 'aggregation_level' not in st.session_state:
            st.session_state.aggregation_level = "national"
            
        # Data cache
        if 'current_data' not in st.session_state:
            st.session_state.current_data = None
            
        # Mark as initialized
        st.session_state.initialized = True

def update_scenario(scenario_id: int):
    """Update the selected scenario in session state."""
    st.session_state.selected_scenario = scenario_id
    
def update_year(year: int):
    """Update the selected year in session state."""
    st.session_state.selected_year = year
    
def update_state(state_fips: str):
    """Update the selected state in session state."""
    st.session_state.selected_state = state_fips
    # Clear county selection when state changes
    st.session_state.selected_county = None
    
def update_county(county_fips: str):
    """Update the selected county in session state."""
    st.session_state.selected_county = county_fips
    
def update_aggregation_level(level: str):
    """Update the aggregation level in session state."""
    st.session_state.aggregation_level = level

def update_current_data(data):
    """Update the current data in session state."""
    st.session_state.current_data = data
    
def get_selected_scenario() -> Optional[int]:
    """Get the currently selected scenario ID from session state."""
    return st.session_state.selected_scenario

def get_selected_year() -> Optional[int]:
    """Get the currently selected year from session state."""
    return st.session_state.selected_year

def get_selected_state() -> Optional[str]:
    """Get the currently selected state FIPS from session state."""
    return st.session_state.selected_state

def get_selected_county() -> Optional[str]:
    """Get the currently selected county FIPS from session state."""
    return st.session_state.selected_county

def get_aggregation_level() -> str:
    """Get the current aggregation level from session state."""
    return st.session_state.aggregation_level

def get_current_data():
    """Get the current data from session state."""
    return st.session_state.current_data 