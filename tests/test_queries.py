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
        INSERT INTO scenarios (scenario_name, gcm, rcp, ssp)
        VALUES 
            ('CNRM_CM5_rcp45_ssp1', 'CNRM_CM5', 'rcp45', 'ssp1'),
            ('IPSL_CM5A_MR_rcp85_ssp3', 'IPSL_CM5A_MR', 'rcp85', 'ssp3')
    """)
    
    # Insert test transitions
    cursor.execute("""
        INSERT INTO land_use_transitions 
        (scenario_name, time_period, fips_code, from_land_use, to_land_use, acres)
        VALUES
            ('CNRM_CM5_rcp45_ssp1', '2020-2030', '36001', 'cr', 'ur', 100),
            ('CNRM_CM5_rcp45_ssp1', '2020-2030', '36001', 'fr', 'ur', 150),
            ('CNRM_CM5_rcp45_ssp1', '2020-2030', '36002', 'ur', 'cr', 50)
    """)
    
    db_connection.commit()

def test_total_net_change(db_connection, sample_data):
    """Test Query I.1: Total net change calculation."""
    results = LandUseQueries.total_net_change(2020, 2030, "CNRM_CM5_rcp45_ssp1")
    
    # Verify results
    assert len(results) > 0
    
    # Check urban land change
    urban_change = next(r for r in results if r["land_use"] == "ur")
    assert urban_change["net_change"] == 200  # 250 gained - 50 lost
    
    # Check cropland change
    crop_change = next(r for r in results if r["land_use"] == "cr")
    assert crop_change["net_change"] == -50  # 50 gained - 100 lost

def test_annualized_change_rate(db_connection, sample_data):
    """Test Query I.2: Annualized change rate calculation."""
    # Implementation coming soon
    pass

def test_peak_change_period(db_connection, sample_data):
    """Test Query I.3: Peak change period identification."""
    # Implementation coming soon
    pass

def test_change_by_state(db_connection, sample_data):
    """Test Query II.4: Change by state calculation."""
    # Implementation coming soon
    pass 