import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from data_loader import load_landuse_data, filter_data, get_unique_values, summarize_transitions

@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    data = {
        'Scenario': ['scenario_1', 'scenario_1', 'scenario_2'] * 2,
        'Year': [2020, 2020, 2025] * 2,
        'FIPS': ['01001', '01002', '01003'] * 2,
        'From': ['Crop', 'Forest', 'Urban'] * 2,
        'To': ['Urban', 'Crop', 'Forest'] * 2,
        'Acres': [100, 200, 300, 400, 500, 600]
    }
    return pd.DataFrame(data)

def test_load_landuse_data_file_not_found():
    """Test that FileNotFoundError is raised when file doesn't exist."""
    with pytest.raises(FileNotFoundError):
        load_landuse_data("nonexistent.parquet")

def test_filter_data_scenario(sample_df):
    """Test filtering by scenario."""
    filtered = filter_data(sample_df, scenario='scenario_1')
    assert len(filtered) == 4
    assert all(filtered['Scenario'] == 'scenario_1')

def test_filter_data_year(sample_df):
    """Test filtering by year."""
    filtered = filter_data(sample_df, year=2020)
    assert len(filtered) == 4
    assert all(filtered['Year'] == 2020)

def test_filter_data_fips(sample_df):
    """Test filtering by FIPS code."""
    filtered = filter_data(sample_df, fips='01001')
    assert len(filtered) == 2
    assert all(filtered['FIPS'] == '01001')

def test_filter_data_multiple_criteria(sample_df):
    """Test filtering by multiple criteria."""
    filtered = filter_data(
        sample_df,
        scenario='scenario_1',
        year=2020,
        fips='01001'
    )
    assert len(filtered) == 1
    assert filtered.iloc[0]['Scenario'] == 'scenario_1'
    assert filtered.iloc[0]['Year'] == 2020
    assert filtered.iloc[0]['FIPS'] == '01001'

def test_get_unique_values(sample_df):
    """Test getting unique values from a column."""
    scenarios = get_unique_values(sample_df, 'Scenario')
    assert scenarios == ['scenario_1', 'scenario_2']
    
    years = get_unique_values(sample_df, 'Year')
    assert years == [2020, 2025]

def test_summarize_transitions(sample_df):
    """Test summarizing transitions."""
    # Test without grouping
    summary = summarize_transitions(sample_df)
    assert len(summary) == 3  # unique From-To combinations
    
    # Test with grouping
    summary_by_scenario = summarize_transitions(sample_df, group_by=['Scenario'])
    assert len(summary_by_scenario) == 6  # unique Scenario-From-To combinations
    
    # Verify sums are correct
    total_acres = summary['Acres'].sum()
    assert total_acres == sample_df['Acres'].sum()

if __name__ == '__main__':
    pytest.main([__file__]) 