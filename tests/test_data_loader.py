import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from src.data_loader import load_landuse_data, filter_data, get_unique_values, summarize_transitions

@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing.
    
    Scenarios:
    1. CNRM_CM5_rcp45_ssp1 (Lower emissions, sustainability-focused):
       - Moderate development pressure
       - Strong environmental protection
       - Sustainable land use practices
       
    2. IPSL_CM5A_MR_rcp85_ssp2 (Higher emissions, middle-of-the-road):
       - High development pressure
       - Mixed adaptation capacity
       - Moderate environmental protection
    
    Time Periods:
    - 2012-2020: Calibration period
    - 2020-2030: Near-term projection
    - 2030-2040: Mid-term projection
    - 2040-2050: Mid-term projection
    - 2050-2060: Long-term projection
    - 2060-2070: Long-term projection
    
    Counties (FIPS = STATE(2) + COUNTY(3)):
    - 25001: Barnstable County, MA (Coastal)
    - 25009: Essex County, MA (Urban/Suburban)
    - 25011: Franklin County, MA (Rural)
    """
    data = {
        'Scenario': [
            # CNRM_CM5_rcp45_ssp1 (Sustainability)
            'CNRM_CM5_rcp45_ssp1', 'CNRM_CM5_rcp45_ssp1', 'CNRM_CM5_rcp45_ssp1',
            'CNRM_CM5_rcp45_ssp1', 'CNRM_CM5_rcp45_ssp1', 'CNRM_CM5_rcp45_ssp1',
            'CNRM_CM5_rcp45_ssp1', 'CNRM_CM5_rcp45_ssp1', 'CNRM_CM5_rcp45_ssp1',
            # IPSL_CM5A_MR_rcp85_ssp2 (Middle-of-the-road)
            'IPSL_CM5A_MR_rcp85_ssp2', 'IPSL_CM5A_MR_rcp85_ssp2', 'IPSL_CM5A_MR_rcp85_ssp2',
            'IPSL_CM5A_MR_rcp85_ssp2', 'IPSL_CM5A_MR_rcp85_ssp2', 'IPSL_CM5A_MR_rcp85_ssp2',
            'IPSL_CM5A_MR_rcp85_ssp2', 'IPSL_CM5A_MR_rcp85_ssp2', 'IPSL_CM5A_MR_rcp85_ssp2'
        ],
        'Period': [
            # CNRM_CM5_rcp45_ssp1 transitions
            '2012-2020', '2012-2020', '2012-2020',  # Calibration
            '2020-2030', '2020-2030', '2020-2030',  # Near-term
            '2030-2040', '2030-2040', '2030-2040',  # Mid-term
            # IPSL_CM5A_MR_rcp85_ssp2 transitions
            '2012-2020', '2012-2020', '2012-2020',  # Calibration
            '2020-2030', '2020-2030', '2020-2030',  # Near-term
            '2030-2040', '2030-2040', '2030-2040'   # Mid-term
        ],
        'FIPS': [
            # CNRM_CM5_rcp45_ssp1
            '25001', '25009', '25011',  # 2012-2020
            '25001', '25009', '25011',  # 2020-2030
            '25001', '25009', '25011',  # 2030-2040
            # IPSL_CM5A_MR_rcp85_ssp2
            '25001', '25009', '25011',  # 2012-2020
            '25001', '25009', '25011',  # 2020-2030
            '25001', '25009', '25011'   # 2030-2040
        ],
        'From': [
            # CNRM_CM5_rcp45_ssp1 - Sustainability focus
            'Crop',   'Forest', 'Forest',  # 2012-2020
            'Crop',   'Forest', 'Forest',  # 2020-2030
            'Crop',   'Forest', 'Forest',  # 2030-2040
            # IPSL_CM5A_MR_rcp85_ssp2 - Mixed development
            'Crop',   'Forest', 'Forest',  # 2012-2020
            'Forest', 'Forest', 'Crop',    # 2020-2030
            'Forest', 'Crop',   'Forest'   # 2030-2040
        ],
        'To': [
            # CNRM_CM5_rcp45_ssp1 - Sustainability focus
            'Forest', 'Urban', 'Crop',    # 2012-2020
            'Urban',  'Urban', 'Crop',    # 2020-2030
            'Urban',  'Urban', 'Urban',   # 2030-2040
            # IPSL_CM5A_MR_rcp85_ssp2 - Mixed development
            'Urban',  'Urban', 'Urban',   # 2012-2020
            'Urban',  'Crop',  'Urban',   # 2020-2030
            'Urban',  'Urban', 'Urban'    # 2030-2040
        ],
        'Acres': [
            # CNRM_CM5_rcp45_ssp1 - More moderate changes
            100, 150, 200,  # 2012-2020
            250, 300, 350,  # 2020-2030
            400, 450, 500,  # 2030-2040
            # IPSL_CM5A_MR_rcp85_ssp2 - Mixed intensity changes
            550, 600, 650,  # 2012-2020
            700, 750, 800,  # 2020-2030
            850, 900, 950   # 2030-2040
        ]
    }
    return pd.DataFrame(data)

def test_load_landuse_data_file_not_found():
    """Test that FileNotFoundError is raised when file doesn't exist."""
    with pytest.raises(FileNotFoundError):
        load_landuse_data("nonexistent.parquet")

def test_filter_data_scenario(sample_df):
    """Test filtering by scenario."""
    filtered = filter_data(sample_df, scenario='CNRM_CM5_rcp45_ssp1')
    assert len(filtered) == 9  # 3 counties × 3 time periods
    assert all(filtered['Scenario'] == 'CNRM_CM5_rcp45_ssp1')

def test_filter_data_period(sample_df):
    """Test filtering by time period."""
    filtered = filter_data(sample_df, period='2020-2030')
    assert len(filtered) == 6  # 3 counties × 2 scenarios
    assert all(filtered['Period'] == '2020-2030')

def test_filter_data_fips(sample_df):
    """Test filtering by FIPS code."""
    filtered = filter_data(sample_df, fips='25001')
    assert len(filtered) == 6  # 2 scenarios × 3 time periods
    assert all(filtered['FIPS'] == '25001')

def test_filter_data_multiple_criteria(sample_df):
    """Test filtering by multiple criteria."""
    filtered = filter_data(
        sample_df,
        scenario='CNRM_CM5_rcp45_ssp1',
        period='2012-2020',
        fips='25001'
    )
    assert len(filtered) == 1
    row = filtered.iloc[0]
    assert row['Scenario'] == 'CNRM_CM5_rcp45_ssp1'
    assert row['Period'] == '2012-2020'
    assert row['FIPS'] == '25001'
    assert row['From'] == 'Crop'
    assert row['To'] == 'Forest'
    assert row['Acres'] == 100

def test_get_unique_values(sample_df):
    """Test getting unique values from a column."""
    scenarios = get_unique_values(sample_df, 'Scenario')
    assert sorted(scenarios) == ['CNRM_CM5_rcp45_ssp1', 'IPSL_CM5A_MR_rcp85_ssp2']
    
    periods = get_unique_values(sample_df, 'Period')
    assert sorted(periods) == ['2012-2020', '2020-2030', '2030-2040']
    
    counties = get_unique_values(sample_df, 'FIPS')
    assert sorted(counties) == ['25001', '25009', '25011']
    
    land_uses = get_unique_values(sample_df, 'From')
    assert sorted(land_uses) == ['Crop', 'Forest']

def test_summarize_transitions(sample_df):
    """Test summarizing transitions.
    
    Expected transitions:
    CNRM_CM5_rcp45_ssp1 (Sustainability):
    1. Crop → Forest (Reforestation)
    2. Crop → Urban (Moderate development)
    3. Forest → Crop (Sustainable agriculture)
    4. Forest → Urban (Limited development)
    
    IPSL_CM5A_MR_rcp85_ssp2 (Middle-of-the-road):
    5. Crop → Urban (Development)
    6. Forest → Urban (Development)
    7. Forest → Crop (Agricultural expansion)
    """
    # Test without grouping
    summary = summarize_transitions(sample_df)
    assert len(summary) == 4  # Forest→Urban, Forest→Crop, Crop→Urban, Crop→Forest
    
    # Test with grouping by scenario
    summary_by_scenario = summarize_transitions(sample_df, group_by=['Scenario'])
    
    # Verify scenario-specific patterns
    sust_scenario = summary_by_scenario[summary_by_scenario['Scenario'] == 'CNRM_CM5_rcp45_ssp1']
    assert len(sust_scenario) == 4  # Four types of transitions in sustainability scenario
    
    mixed_scenario = summary_by_scenario[summary_by_scenario['Scenario'] == 'IPSL_CM5A_MR_rcp85_ssp2']
    assert len(mixed_scenario) == 3  # Three types of transitions in mixed scenario
    
    # Verify total area conservation
    total_acres = summary['Acres'].sum()
    assert total_acres == sample_df['Acres'].sum()
    
    # Verify specific transition patterns for sustainability scenario
    sust_transitions = set(zip(sust_scenario['From'], sust_scenario['To']))
    assert ('Crop', 'Forest') in sust_transitions  # Includes reforestation
    assert ('Forest', 'Crop') in sust_transitions  # Sustainable agriculture
    assert ('Forest', 'Urban') in sust_transitions  # Limited development
    
    # Verify specific transition patterns for mixed scenario
    mixed_transitions = set(zip(mixed_scenario['From'], mixed_scenario['To']))
    assert ('Forest', 'Urban') in mixed_transitions  # Development
    assert ('Forest', 'Crop') in mixed_transitions  # Agricultural expansion
    assert ('Crop', 'Urban') in mixed_transitions  # Urban growth
    
    # Verify transition intensities
    sust_urban = sust_scenario[sust_scenario['To'] == 'Urban']['Acres'].sum()
    mixed_urban = mixed_scenario[mixed_scenario['To'] == 'Urban']['Acres'].sum()
    assert mixed_urban > sust_urban  # Mixed scenario has more urban development

if __name__ == '__main__':
    pytest.main([__file__]) 