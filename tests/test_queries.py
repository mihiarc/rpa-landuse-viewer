import pytest
from datetime import datetime
from src.api.queries import LandUseQueries
from src.api.models import LandUseTransition, ScenarioInfo

@pytest.fixture
def sample_data(db_connection):
    """Insert sample data for testing."""
    cursor = db_connection.cursor()
    
    # Insert test scenarios
    cursor.execute("""
        INSERT INTO scenarios (scenario_id, scenario_name, gcm, rcp, ssp)
        VALUES 
            (1, 'CNRM_CM5_rcp45_ssp1', 'CNRM_CM5', 'rcp45', 'ssp1'),
            (2, 'CNRM_CM5_rcp85_ssp5', 'CNRM_CM5', 'rcp85', 'ssp5')
    """)
    
    # Insert test time steps
    cursor.execute("""
        INSERT INTO time_steps (time_step_id, start_year, end_year)
        VALUES 
            (1, 2012, 2020),
            (2, 2020, 2030),
            (3, 2030, 2040)
    """)
    
    # Insert test counties
    cursor.execute("""
        INSERT INTO counties (fips_code, county_name)
        VALUES 
            ('06001', 'Test County 1'),
            ('06002', 'Test County 2')
    """)
    
    # Insert test land use transitions
    cursor.execute("""
        INSERT INTO land_use_transitions 
        (transition_id, scenario_id, time_step_id, fips_code, from_land_use, to_land_use, acres)
        VALUES 
            (1, 1, 1, '06001', 'Crop', 'Urban', 1000),
            (2, 1, 1, '06001', 'Forest', 'Urban', 500),
            (3, 1, 2, '06001', 'Pasture', 'Crop', 750),
            (4, 2, 1, '06001', 'Crop', 'Urban', 1200),
            (5, 2, 1, '06001', 'Forest', 'Urban', 600)
    """)
    
    db_connection.commit()
    yield
    
    # Clean up test data
    cursor.execute("DELETE FROM land_use_transitions")
    cursor.execute("DELETE FROM counties")
    cursor.execute("DELETE FROM time_steps")
    cursor.execute("DELETE FROM scenarios")
    db_connection.commit()

def test_total_net_change(sample_data):
    """Test total net change calculation."""
    results = LandUseQueries.total_net_change(2012, 2030, "CNRM_CM5_rcp45_ssp1")
    
    # Verify urban gain
    urban_change = next(r for r in results if r['land_use'] == 'Urban')
    assert urban_change['net_change'] == 1500  # 1000 + 500 from crops and forest
    
    # Verify crop loss
    crop_change = next(r for r in results if r['land_use'] == 'Crop')
    assert crop_change['net_change'] == -250  # -1000 to urban, +750 from pasture

def test_annualized_change_rate(sample_data):
    """Test annualized change rate calculation."""
    results = LandUseQueries.annualized_change_rate(2012, 2020, "CNRM_CM5_rcp45_ssp1")
    
    # Verify urban gain rate
    urban_rate = next(r for r in results if r['land_use'] == 'Urban')
    assert urban_rate['annual_rate'] == 187.5  # 1500 acres / 8 years

def test_compare_scenarios(sample_data):
    """Test scenario comparison."""
    results = LandUseQueries.compare_scenarios(
        2012, 2020, "Urban", "CNRM_CM5_rcp45_ssp1", "CNRM_CM5_rcp85_ssp5"
    )
    
    # Verify results for both scenarios
    scenario1 = next(r for r in results if r['scenario_name'] == 'CNRM_CM5_rcp45_ssp1')
    scenario2 = next(r for r in results if r['scenario_name'] == 'CNRM_CM5_rcp85_ssp5')
    
    assert scenario1['net_change'] == 1500
    assert scenario2['net_change'] == 1800
    assert scenario1['rcp'] == 'rcp45'
    assert scenario2['rcp'] == 'rcp85'

def test_major_transitions(sample_data):
    """Test major transitions identification."""
    results = LandUseQueries.major_transitions(2012, 2020, "CNRM_CM5_rcp45_ssp1", 2)
    
    # Verify top transitions
    assert len(results) == 2
    assert results[0]['from_land_use'] == 'Crop'
    assert results[0]['to_land_use'] == 'Urban'
    assert results[0]['acres_changed'] == 1000
    
    # Verify percentage calculation
    total_change = 1500  # 1000 + 500
    assert abs(results[0]['percentage_of_all_changes'] - (1000 / total_change * 100)) < 0.01

def test_check_data_integrity(sample_data):
    """Test data integrity checks."""
    results = LandUseQueries.check_data_integrity("CNRM_CM5_rcp45_ssp1")
    
    # Verify no area inconsistencies in sample data
    assert len(results['area_inconsistencies']) == 0
    
    # Verify no negative acres in sample data
    assert len(results['negative_acres']) == 0
    
    # Add a test case with negative acres
    with pytest.raises(Exception):
        cursor = db_connection.cursor()
        cursor.execute("""
            INSERT INTO land_use_transitions 
            (transition_id, scenario_id, time_step_id, fips_code, from_land_use, to_land_use, acres)
            VALUES (6, 1, 1, '06001', 'Crop', 'Urban', -100)
        """)
        db_connection.commit()
        
        results = LandUseQueries.check_data_integrity("CNRM_CM5_rcp45_ssp1")
        assert len(results['negative_acres']) == 1 