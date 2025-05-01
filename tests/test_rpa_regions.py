#!/usr/bin/env python3
"""
Test module for RPA regions functionality.
"""

import sys
from pathlib import Path
import pytest
import pandas as pd

# Add the parent directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.rpa_regions import RPARegions
from src.db.database import DatabaseConnection


@pytest.fixture(scope="module")
def sample_scenario_id():
    """Get the first scenario ID from the database for testing."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT scenario_id FROM scenarios LIMIT 1")
    scenario_id = cursor.fetchone()[0]
    DatabaseConnection.close_connection(conn)
    return scenario_id


def test_get_all_regions():
    """Test getting all regions."""
    regions = RPARegions.get_all_regions()
    assert regions is not None
    assert len(regions) > 0
    # Verify structure of returned data
    assert all(r.get('region_id') is not None for r in regions)
    assert all(r.get('region_name') is not None for r in regions)


def test_get_subregions():
    """Test getting all subregions."""
    subregions = RPARegions.get_subregions()
    assert subregions is not None
    assert len(subregions) > 0
    # Verify structure of returned data
    assert all(s.get('subregion_id') is not None for s in subregions)
    assert all(s.get('subregion_name') is not None for s in subregions)
    assert all(s.get('region_id') is not None for s in subregions)


def test_get_subregions_for_region():
    """Test getting subregions for a specific region."""
    north_subregions = RPARegions.get_subregions('NORTH')
    assert north_subregions is not None
    assert len(north_subregions) > 0
    # Verify all subregions belong to the North region
    assert all(s.get('region_id') == 'NORTH' for s in north_subregions)


def test_get_states_by_region():
    """Test getting states by region."""
    north_states = RPARegions.get_states_by_region('NORTH')
    assert north_states is not None
    assert len(north_states) > 0
    # Verify structure of returned data
    assert all(s.get('state_code') is not None for s in north_states)
    assert all(s.get('state_name') is not None for s in north_states)
    assert all(s.get('region_id') is not None for s in north_states)


def test_get_states_by_subregion():
    """Test getting states by subregion."""
    northeast_states = RPARegions.get_states_by_region(subregion_id='NORTHEAST')
    assert northeast_states is not None
    assert len(northeast_states) > 0
    # Verify all states belong to the Northeast subregion
    assert all(s.get('subregion_id') == 'NORTHEAST' for s in northeast_states)


def test_get_counties_by_region():
    """Test getting counties by region."""
    pacific_counties = RPARegions.get_counties_by_region('PACIFIC')
    assert pacific_counties is not None
    assert len(pacific_counties) > 0
    # Verify structure of returned data
    assert all(c.get('fips_code') is not None for c in pacific_counties)
    assert all(c.get('county_name') is not None for c in pacific_counties)
    assert all(c.get('state_code') is not None for c in pacific_counties)
    assert all(c.get('region_id') == 'PACIFIC' for c in pacific_counties)


def test_get_counties_by_subregion():
    """Test getting counties by subregion."""
    alaska_counties = RPARegions.get_counties_by_region(subregion_id='ALASKA')
    assert alaska_counties is not None
    assert len(alaska_counties) > 0
    # Verify all counties belong to the Alaska subregion
    assert all(c.get('subregion_id') == 'ALASKA' for c in alaska_counties)


def test_get_land_use_by_region(sample_scenario_id):
    """Test getting land use by region."""
    region_data = RPARegions.get_land_use_by_region(sample_scenario_id)
    assert region_data is not None
    assert not region_data.empty
    # Verify required columns exist
    required_columns = ['region_id', 'region_name', 'time_step_id', 'land_use_type', 'acres']
    assert all(col in region_data.columns for col in required_columns)


def test_get_land_use_by_subregion(sample_scenario_id):
    """Test getting land use by subregion."""
    subregion_data = RPARegions.get_land_use_by_subregion(sample_scenario_id)
    assert subregion_data is not None
    assert not subregion_data.empty
    # Verify required columns exist
    required_columns = ['subregion_id', 'subregion_name', 'region_id', 'time_step_id', 'land_use_type', 'acres']
    assert all(col in subregion_data.columns for col in required_columns) 