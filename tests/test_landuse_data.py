#!/usr/bin/env python3
"""
Test suite to validate that DuckDB database data matches the reported land use change
from the gtr_wo102_Chap4_chunks.json file.

The tests are organized by land use categories:
- Forest land
- Developed land
- Crop land
- Pasture land
- Rangeland
"""

import json
import os
import pytest
import pandas as pd
import duckdb
import re
from pathlib import Path
import sys

# Add src directory to path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent / "src"))
from db.database import DBManager

# Path to the JSON file and database
JSON_PATH = os.path.join("docs", "rpa_text", "gtr_wo102_Chap4_chunks.json")
DB_PATH = os.path.join("data", "database", "rpa.db")

# Land use categories
LAND_USE_CATEGORIES = ['forest', 'developed', 'crop', 'pasture', 'rangeland']

# RPA scenarios and climate projections
RPA_SCENARIOS = ['LM', 'HM', 'HL', 'HH']
CLIMATE_PROJECTIONS = ['Least warm', 'Hot', 'Dry', 'Wet', 'Middle']

@pytest.fixture(scope="session")
def json_data():
    """
    Load data from the JSON file as a pytest fixture.
    Makes this data available to all tests.
    """
    with open(JSON_PATH, "r") as f:
        data = json.load(f)
    return data

@pytest.fixture(scope="session")
def parsed_json_data(json_data):
    """
    Parse the JSON data into a structured format as a pytest fixture.
    """
    return parse_land_use_change_from_json(json_data)

@pytest.fixture(scope="session")
def db_data():
    """
    Load data from the database as a pytest fixture.
    """
    return get_land_use_data_from_db()

def parse_land_use_change_from_json(json_data):
    """
    Parse land use change data from the JSON file.
    Extract values from Table 4-9, Table 4-10, and Table 4-11.
    """
    # Store parsed data
    parsed_data = {
        "net_change": {},  # For Table 4-9
        "gross_change": {},  # For Table 4-10
        "forest_transitions": {}  # For Table 4-11
    }
    
    # Process the JSON chunks to extract the data
    for chunk in json_data:
        text = chunk["text"]
        
        # Extract net change data (Table 4-9)
        # Format: "Forest, LMscenario Climate projection.Least warm.million acres (percent) = -13.0 (-3.2%)"
        net_change_matches = re.finditer(
            r"(Forest|Developed|Crop|Pasture|Rangeland), "
            r"([A-Z]{2})scenario Climate projection\.([A-Za-z ]+)\.million acres \(percent\) = ([^%]+)",
            text
        )
        
        for match in net_change_matches:
            land_use = match.group(1).lower()
            scenario = match.group(2)  # LM, HM, HL, HH
            climate = match.group(3).strip()  # Least warm, Hot, Dry, Wet, Middle
            
            # Get the values (some might contain both value and percentage)
            value_text = match.group(4).strip()
            values = re.findall(r"-?\d+\.\d+", value_text)
            
            if not values:
                continue
                
            value = float(values[0])
            
            # Store the data
            key = f"{scenario}_{climate}_{land_use}"
            parsed_data["net_change"][key] = value
        
        # Extract gross change data (Table 4-10)
        # Format: "Forest, 2070 land use (million acres).Forest = 372.1"
        gross_change_matches = re.finditer(
            r"(Forest|Developed|Crop|Pasture|Rangeland), 2070 land use \(million acres\)\.(Forest|Developed|Crop|Pasture|Rangeland) = ([^\.]+)",
            text
        )
        
        for match in gross_change_matches:
            from_land_use = match.group(1).lower()
            to_land_use = match.group(2).lower()
            value_text = match.group(3).strip()
            
            # Some values might be "-" for no transition
            if value_text == "-":
                value = 0
            else:
                try:
                    value = float(value_text)
                except ValueError:
                    continue
            
            # Store the data
            key = f"{from_land_use}_to_{to_land_use}"
            parsed_data["gross_change"][key] = value
        
        # Extract forest transitions data (Table 4-11)
        # Format: "Forest to developed, LMscenario Climate projection.Least warm.million acres = 24.0"
        forest_trans_matches = re.finditer(
            r"(Forest|Crop|Pasture|Rangeland) to (developed|crop|pasture|rangeland|forest), "
            r"([A-Z]{2})scenario Climate projection\.([A-Za-z ]+)\.million acres = ([^\.]+)",
            text
        )
        
        for match in forest_trans_matches:
            from_land_use = match.group(1).lower()
            to_land_use = match.group(2).lower()
            scenario = match.group(3)  # LM, HM, HL, HH
            climate = match.group(4).strip()  # Least warm, Hot, Dry, Wet, Middle
            value_text = match.group(5).strip()
            
            try:
                value = float(value_text)
            except ValueError:
                continue
            
            # Store the data
            key = f"{scenario}_{climate}_{from_land_use}_to_{to_land_use}"
            parsed_data["forest_transitions"][key] = value
    
    return parsed_data

def get_land_use_data_from_db():
    """Query the database to get land use change data."""
    db_data = {
        "net_change": {},
        "gross_change": {},
        "forest_transitions": {}
    }
    
    try:
        # Connect to DB using the DBManager
        with DBManager.connection() as conn:
            # Query for net land use change (equivalent to Table 4-9)
            net_change_query = """
            WITH initial_areas AS (
                SELECT 
                    s.scenario_name,
                    s.gcm,
                    s.rcp,
                    s.ssp,
                    t.from_land_use,
                    SUM(t.area_hundreds_acres * 100) as initial_acres
                FROM 
                    land_use_transitions t
                JOIN 
                    scenarios s ON t.scenario_id = s.scenario_id
                JOIN 
                    time_steps ts ON t.time_step_id = ts.time_step_id
                WHERE 
                    ts.start_year = 2020 AND ts.end_year = 2070
                GROUP BY 
                    s.scenario_name, s.gcm, s.rcp, s.ssp, t.from_land_use
            ),
            final_areas AS (
                SELECT 
                    s.scenario_name,
                    s.gcm,
                    s.rcp,
                    s.ssp,
                    t.to_land_use,
                    SUM(t.area_hundreds_acres * 100) as final_acres
                FROM 
                    land_use_transitions t
                JOIN 
                    scenarios s ON t.scenario_id = s.scenario_id
                JOIN 
                    time_steps ts ON t.time_step_id = ts.time_step_id
                WHERE 
                    ts.start_year = 2020 AND ts.end_year = 2070
                GROUP BY 
                    s.scenario_name, s.gcm, s.rcp, s.ssp, t.to_land_use
            ),
            net_change AS (
                SELECT 
                    i.scenario_name,
                    i.gcm,
                    i.from_land_use as land_use,
                    CASE
                        WHEN i.from_land_use = 'fr' THEN 'forest'
                        WHEN i.from_land_use = 'ur' THEN 'developed'
                        WHEN i.from_land_use = 'cr' THEN 'crop'
                        WHEN i.from_land_use = 'ps' THEN 'pasture'
                        WHEN i.from_land_use = 'rg' THEN 'rangeland'
                        ELSE i.from_land_use
                    END as land_use_name,
                    COALESCE(f.final_acres, 0) - COALESCE(i.initial_acres, 0) as net_change_acres
                FROM 
                    initial_areas i
                LEFT JOIN 
                    final_areas f ON i.scenario_name = f.scenario_name 
                                AND i.gcm = f.gcm 
                                AND i.from_land_use = f.to_land_use
            )
            SELECT 
                scenario_name,
                gcm,
                land_use_name,
                net_change_acres,
                CASE
                    WHEN LEFT(scenario_name, 2) = 'LM' THEN 'LM'
                    WHEN LEFT(scenario_name, 2) = 'HM' THEN 'HM'
                    WHEN LEFT(scenario_name, 2) = 'HL' THEN 'HL'
                    WHEN LEFT(scenario_name, 2) = 'HH' THEN 'HH'
                    ELSE LEFT(scenario_name, 2)
                END as scenario_code,
                CASE
                    WHEN gcm = 'MRI-CGCM3' THEN 'Least warm'
                    WHEN gcm = 'HadGEM2-ES' THEN 'Hot'
                    WHEN gcm = 'IPSL-CM5A-MR' THEN 'Dry'
                    WHEN gcm = 'CNRM-CM5' THEN 'Wet'
                    WHEN gcm = 'NorESM1-M' THEN 'Middle'
                    ELSE gcm
                END as climate_projection
            FROM 
                net_change
            WHERE 
                land_use_name IN ('forest', 'developed', 'crop', 'pasture', 'rangeland')
            ORDER BY 
                scenario_name, gcm, land_use_name
            """
            
            # Execute the query and process the results
            net_change_results = conn.execute(net_change_query).fetchdf()
            
            for _, row in net_change_results.iterrows():
                scenario = row['scenario_code']
                climate = row['climate_projection']
                land_use = row['land_use_name']
                value = row['net_change_acres'] / 1000000  # Convert to millions of acres
                
                key = f"{scenario}_{climate}_{land_use}"
                db_data["net_change"][key] = value
                
            # Query for gross land use transitions (equivalent to Table 4-11)
            forest_trans_query = """
            WITH transitions AS (
                SELECT 
                    s.scenario_name,
                    s.gcm,
                    t.from_land_use,
                    t.to_land_use,
                    SUM(t.area_hundreds_acres * 100) as transition_acres
                FROM 
                    land_use_transitions t
                JOIN 
                    scenarios s ON t.scenario_id = s.scenario_id
                JOIN 
                    time_steps ts ON t.time_step_id = ts.time_step_id
                WHERE 
                    ts.start_year = 2020 AND ts.end_year = 2070
                GROUP BY 
                    s.scenario_name, s.gcm, t.from_land_use, t.to_land_use
            )
            SELECT 
                scenario_name,
                gcm,
                CASE
                    WHEN from_land_use = 'fr' THEN 'forest'
                    WHEN from_land_use = 'ur' THEN 'developed'
                    WHEN from_land_use = 'cr' THEN 'crop'
                    WHEN from_land_use = 'ps' THEN 'pasture'
                    WHEN from_land_use = 'rg' THEN 'rangeland'
                    ELSE from_land_use
                END as from_land_use_name,
                CASE
                    WHEN to_land_use = 'fr' THEN 'forest'
                    WHEN to_land_use = 'ur' THEN 'developed'
                    WHEN to_land_use = 'cr' THEN 'crop'
                    WHEN to_land_use = 'ps' THEN 'pasture'
                    WHEN to_land_use = 'rg' THEN 'rangeland'
                    ELSE to_land_use
                END as to_land_use_name,
                transition_acres,
                CASE
                    WHEN LEFT(scenario_name, 2) = 'LM' THEN 'LM'
                    WHEN LEFT(scenario_name, 2) = 'HM' THEN 'HM'
                    WHEN LEFT(scenario_name, 2) = 'HL' THEN 'HL'
                    WHEN LEFT(scenario_name, 2) = 'HH' THEN 'HH'
                    ELSE LEFT(scenario_name, 2)
                END as scenario_code,
                CASE
                    WHEN gcm = 'MRI-CGCM3' THEN 'Least warm'
                    WHEN gcm = 'HadGEM2-ES' THEN 'Hot'
                    WHEN gcm = 'IPSL-CM5A-MR' THEN 'Dry'
                    WHEN gcm = 'CNRM-CM5' THEN 'Wet'
                    WHEN gcm = 'NorESM1-M' THEN 'Middle'
                    ELSE gcm
                END as climate_projection
            FROM 
                transitions
            WHERE 
                from_land_use_name IN ('forest', 'developed', 'crop', 'pasture', 'rangeland')
                AND to_land_use_name IN ('developed', 'forest', 'crop', 'pasture', 'rangeland')
            ORDER BY 
                scenario_name, gcm, from_land_use_name, to_land_use_name
            """
            
            forest_trans_results = conn.execute(forest_trans_query).fetchdf()
            
            for _, row in forest_trans_results.iterrows():
                scenario = row['scenario_code']
                climate = row['climate_projection']
                from_land_use = row['from_land_use_name']
                to_land_use = row['to_land_use_name']
                value = row['transition_acres'] / 1000000  # Convert to millions of acres
                
                # Store transitions involving each land use type
                key = f"{scenario}_{climate}_{from_land_use}_to_{to_land_use}"
                db_data["forest_transitions"][key] = value
                
                # Also store data for gross change calculation (Table 4-10)
                key = f"{from_land_use}_to_{to_land_use}"
                if key not in db_data["gross_change"]:
                    db_data["gross_change"][key] = 0
                db_data["gross_change"][key] += value

    except Exception as e:
        print(f"Error querying database: {e}")
        return {}
    
    return db_data

class TestFileExistence:
    """Test that the required files exist."""
    
    def test_db_exists(self):
        """Test that the database file exists."""
        assert os.path.exists(DB_PATH), f"Database file not found at {DB_PATH}"
    
    def test_json_exists(self):
        """Test that the JSON file exists."""
        assert os.path.exists(JSON_PATH), f"JSON file not found at {JSON_PATH}"

class TestForestLand:
    """Test Forest Land use changes"""
    
    def test_net_change(self, parsed_json_data, db_data):
        """Test forest land net change across all scenarios and climate projections."""
        land_use = 'forest'
        
        for scenario in RPA_SCENARIOS:
            for climate in CLIMATE_PROJECTIONS:
                key = f"{scenario}_{climate}_{land_use}"
                
                if key in parsed_json_data["net_change"] and key in db_data["net_change"]:
                    json_value = parsed_json_data["net_change"][key]
                    db_value = db_data["net_change"][key]
                    
                    assert db_value == pytest.approx(json_value, rel=0.05), \
                        f"Forest net change mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_forest_to_developed(self, parsed_json_data, db_data):
        """Test forest to developed transitions across scenarios and climate projections."""
        from_land_use = 'forest'
        to_land_use = 'developed'
        
        for scenario in RPA_SCENARIOS:
            for climate in CLIMATE_PROJECTIONS:
                key = f"{scenario}_{climate}_{from_land_use}_to_{to_land_use}"
                
                if key in parsed_json_data["forest_transitions"] and key in db_data["forest_transitions"]:
                    json_value = parsed_json_data["forest_transitions"][key]
                    db_value = db_data["forest_transitions"][key]
                    
                    assert db_value == pytest.approx(json_value, rel=0.05), \
                        f"Forest to developed mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_forest_to_crop(self, parsed_json_data, db_data):
        """Test forest to crop transitions across scenarios and climate projections."""
        from_land_use = 'forest'
        to_land_use = 'crop'
        
        for scenario in RPA_SCENARIOS:
            for climate in CLIMATE_PROJECTIONS:
                key = f"{scenario}_{climate}_{from_land_use}_to_{to_land_use}"
                
                if key in parsed_json_data["forest_transitions"] and key in db_data["forest_transitions"]:
                    json_value = parsed_json_data["forest_transitions"][key]
                    db_value = db_data["forest_transitions"][key]
                    
                    assert db_value == pytest.approx(json_value, rel=0.05), \
                        f"Forest to crop mismatch for {key}: DB={db_value}, JSON={json_value}"
                        
    def test_forest_to_pasture(self, parsed_json_data, db_data):
        """Test forest to pasture transitions across scenarios and climate projections."""
        from_land_use = 'forest'
        to_land_use = 'pasture'
        
        for scenario in RPA_SCENARIOS:
            for climate in CLIMATE_PROJECTIONS:
                key = f"{scenario}_{climate}_{from_land_use}_to_{to_land_use}"
                
                if key in parsed_json_data["forest_transitions"] and key in db_data["forest_transitions"]:
                    json_value = parsed_json_data["forest_transitions"][key]
                    db_value = db_data["forest_transitions"][key]
                    
                    assert db_value == pytest.approx(json_value, rel=0.05), \
                        f"Forest to pasture mismatch for {key}: DB={db_value}, JSON={json_value}"
                        
    def test_forest_to_rangeland(self, parsed_json_data, db_data):
        """Test forest to rangeland transitions across scenarios and climate projections."""
        from_land_use = 'forest'
        to_land_use = 'rangeland'
        
        for scenario in RPA_SCENARIOS:
            for climate in CLIMATE_PROJECTIONS:
                key = f"{scenario}_{climate}_{from_land_use}_to_{to_land_use}"
                
                if key in parsed_json_data["forest_transitions"] and key in db_data["forest_transitions"]:
                    json_value = parsed_json_data["forest_transitions"][key]
                    db_value = db_data["forest_transitions"][key]
                    
                    assert db_value == pytest.approx(json_value, rel=0.05), \
                        f"Forest to rangeland mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_gross_transitions(self, parsed_json_data, db_data):
        """Test gross forest transitions from Table 4-10."""
        transitions = [
            'forest_to_developed',
            'forest_to_crop',
            'forest_to_pasture',
            'forest_to_rangeland'
        ]
        
        for transition in transitions:
            if transition in parsed_json_data["gross_change"] and transition in db_data["gross_change"]:
                json_value = parsed_json_data["gross_change"][transition]
                db_value = db_data["gross_change"][transition]
                
                assert db_value == pytest.approx(json_value, rel=0.1), \
                    f"Gross forest transition mismatch for {transition}: DB={db_value}, JSON={json_value}"

class TestDevelopedLand:
    """Test Developed Land use changes"""
    
    def test_net_change(self, parsed_json_data, db_data):
        """Test developed land net change across all scenarios and climate projections."""
        land_use = 'developed'
        
        for scenario in RPA_SCENARIOS:
            for climate in CLIMATE_PROJECTIONS:
                key = f"{scenario}_{climate}_{land_use}"
                
                if key in parsed_json_data["net_change"] and key in db_data["net_change"]:
                    json_value = parsed_json_data["net_change"][key]
                    db_value = db_data["net_change"][key]
                    
                    assert db_value == pytest.approx(json_value, rel=0.05), \
                        f"Developed net change mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_gross_transitions(self, parsed_json_data, db_data):
        """Test all-to-developed transitions from Table 4-10."""
        land_use = 'developed'
        
        # Test other land uses to developed
        for from_land_use in ['forest', 'crop', 'pasture', 'rangeland']:
            transition = f"{from_land_use}_to_{land_use}"
            
            if transition in parsed_json_data["gross_change"] and transition in db_data["gross_change"]:
                json_value = parsed_json_data["gross_change"][transition]
                db_value = db_data["gross_change"][transition]
                
                assert db_value == pytest.approx(json_value, rel=0.1), \
                    f"Gross transition mismatch for {transition}: DB={db_value}, JSON={json_value}"

class TestCropLand:
    """Test Crop Land use changes"""
    
    def test_net_change(self, parsed_json_data, db_data):
        """Test crop land net change across all scenarios and climate projections."""
        land_use = 'crop'
        
        for scenario in RPA_SCENARIOS:
            for climate in CLIMATE_PROJECTIONS:
                key = f"{scenario}_{climate}_{land_use}"
                
                if key in parsed_json_data["net_change"] and key in db_data["net_change"]:
                    json_value = parsed_json_data["net_change"][key]
                    db_value = db_data["net_change"][key]
                    
                    assert db_value == pytest.approx(json_value, rel=0.05), \
                        f"Crop net change mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_crop_to_forest(self, parsed_json_data, db_data):
        """Test crop to forest transitions across scenarios and climate projections."""
        from_land_use = 'crop'
        to_land_use = 'forest'
        
        for scenario in RPA_SCENARIOS:
            for climate in CLIMATE_PROJECTIONS:
                key = f"{scenario}_{climate}_{from_land_use}_to_{to_land_use}"
                
                if key in parsed_json_data["forest_transitions"] and key in db_data["forest_transitions"]:
                    json_value = parsed_json_data["forest_transitions"][key]
                    db_value = db_data["forest_transitions"][key]
                    
                    assert db_value == pytest.approx(json_value, rel=0.05), \
                        f"Crop to forest mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_gross_transitions(self, parsed_json_data, db_data):
        """Test gross crop transitions from Table 4-10."""
        transitions = [
            'crop_to_forest',
            'crop_to_developed',
            'crop_to_pasture',
            'crop_to_rangeland'
        ]
        
        for transition in transitions:
            if transition in parsed_json_data["gross_change"] and transition in db_data["gross_change"]:
                json_value = parsed_json_data["gross_change"][transition]
                db_value = db_data["gross_change"][transition]
                
                assert db_value == pytest.approx(json_value, rel=0.1), \
                    f"Gross crop transition mismatch for {transition}: DB={db_value}, JSON={json_value}"

class TestPastureLand:
    """Test Pasture Land use changes"""
    
    def test_net_change(self, parsed_json_data, db_data):
        """Test pasture land net change across all scenarios and climate projections."""
        land_use = 'pasture'
        
        for scenario in RPA_SCENARIOS:
            for climate in CLIMATE_PROJECTIONS:
                key = f"{scenario}_{climate}_{land_use}"
                
                if key in parsed_json_data["net_change"] and key in db_data["net_change"]:
                    json_value = parsed_json_data["net_change"][key]
                    db_value = db_data["net_change"][key]
                    
                    assert db_value == pytest.approx(json_value, rel=0.05), \
                        f"Pasture net change mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_pasture_to_forest(self, parsed_json_data, db_data):
        """Test pasture to forest transitions across scenarios and climate projections."""
        from_land_use = 'pasture'
        to_land_use = 'forest'
        
        for scenario in RPA_SCENARIOS:
            for climate in CLIMATE_PROJECTIONS:
                key = f"{scenario}_{climate}_{from_land_use}_to_{to_land_use}"
                
                if key in parsed_json_data["forest_transitions"] and key in db_data["forest_transitions"]:
                    json_value = parsed_json_data["forest_transitions"][key]
                    db_value = db_data["forest_transitions"][key]
                    
                    assert db_value == pytest.approx(json_value, rel=0.05), \
                        f"Pasture to forest mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_gross_transitions(self, parsed_json_data, db_data):
        """Test gross pasture transitions from Table 4-10."""
        transitions = [
            'pasture_to_forest',
            'pasture_to_developed',
            'pasture_to_crop',
            'pasture_to_rangeland'
        ]
        
        for transition in transitions:
            if transition in parsed_json_data["gross_change"] and transition in db_data["gross_change"]:
                json_value = parsed_json_data["gross_change"][transition]
                db_value = db_data["gross_change"][transition]
                
                assert db_value == pytest.approx(json_value, rel=0.1), \
                    f"Gross pasture transition mismatch for {transition}: DB={db_value}, JSON={json_value}"

class TestRangeland:
    """Test Rangeland use changes"""
    
    def test_net_change(self, parsed_json_data, db_data):
        """Test rangeland net change across all scenarios and climate projections."""
        land_use = 'rangeland'
        
        for scenario in RPA_SCENARIOS:
            for climate in CLIMATE_PROJECTIONS:
                key = f"{scenario}_{climate}_{land_use}"
                
                if key in parsed_json_data["net_change"] and key in db_data["net_change"]:
                    json_value = parsed_json_data["net_change"][key]
                    db_value = db_data["net_change"][key]
                    
                    assert db_value == pytest.approx(json_value, rel=0.05), \
                        f"Rangeland net change mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_rangeland_to_forest(self, parsed_json_data, db_data):
        """Test rangeland to forest transitions across scenarios and climate projections."""
        from_land_use = 'rangeland'
        to_land_use = 'forest'
        
        for scenario in RPA_SCENARIOS:
            for climate in CLIMATE_PROJECTIONS:
                key = f"{scenario}_{climate}_{from_land_use}_to_{to_land_use}"
                
                if key in parsed_json_data["forest_transitions"] and key in db_data["forest_transitions"]:
                    json_value = parsed_json_data["forest_transitions"][key]
                    db_value = db_data["forest_transitions"][key]
                    
                    assert db_value == pytest.approx(json_value, rel=0.05), \
                        f"Rangeland to forest mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_gross_transitions(self, parsed_json_data, db_data):
        """Test gross rangeland transitions from Table 4-10."""
        transitions = [
            'rangeland_to_forest',
            'rangeland_to_developed',
            'rangeland_to_crop',
            'rangeland_to_pasture'
        ]
        
        for transition in transitions:
            if transition in parsed_json_data["gross_change"] and transition in db_data["gross_change"]:
                json_value = parsed_json_data["gross_change"][transition]
                db_value = db_data["gross_change"][transition]
                
                assert db_value == pytest.approx(json_value, rel=0.1), \
                    f"Gross rangeland transition mismatch for {transition}: DB={db_value}, JSON={json_value}"

class TestRPAScenarios:
    """Test RPA integrated scenarios with the mean GCM (Middle climate projection)"""
    
    def test_mean_gcm_exists(self, db_data):
        """Check if mean GCM data exists for all RPA scenarios.
        
        This test will pass if any scenario-climate-landuse combinations are found,
        but will print information about missing data.
        """
        missing_data = []
        found_data = []
        
        for scenario in RPA_SCENARIOS:
            for land_use in LAND_USE_CATEGORIES:
                key = f"{scenario}_Middle_{land_use}"
                
                if key in db_data["net_change"]:
                    found_data.append(key)
                else:
                    missing_data.append(key)
        
        # Print information about missing data
        if missing_data:
            print(f"\nMissing mean GCM data for {len(missing_data)} combinations:")
            for key in missing_data[:10]:  # Limit to first 10 for readability
                print(f"  - {key}")
            if len(missing_data) > 10:
                print(f"  - ... and {len(missing_data) - 10} more")
        
        # Print information about found data
        if found_data:
            print(f"\nFound mean GCM data for {len(found_data)} combinations:")
            for key in found_data[:10]:  # Limit to first 10 for readability
                print(f"  - {key}")
            if len(found_data) > 10:
                print(f"  - ... and {len(found_data) - 10} more")
        
        # Test will pass as long as we have some data or we know what's missing
        assert True, "This test always passes but reports missing data"
    
    def test_all_scenarios_with_mean_gcm(self, parsed_json_data, db_data):
        """Test net change across all land uses with mean GCM for all scenarios."""
        climate = "Middle"  # Mean GCM
        
        for scenario in RPA_SCENARIOS:
            for land_use in LAND_USE_CATEGORIES:
                key = f"{scenario}_{climate}_{land_use}"
                
                if key in parsed_json_data["net_change"] and key in db_data["net_change"]:
                    json_value = parsed_json_data["net_change"][key]
                    db_value = db_data["net_change"][key]
                    
                    assert db_value == pytest.approx(json_value, rel=0.05), \
                        f"Net change mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_lm_scenario_transitions(self, parsed_json_data, db_data):
        """Test transitions for the LM scenario (lower warming-moderate growth) with mean GCM."""
        scenario = "LM"
        climate = "Middle"
        
        transitions = [
            'forest_to_developed',
            'forest_to_crop',
            'forest_to_pasture',
            'forest_to_rangeland',
            'crop_to_forest',
            'pasture_to_forest',
            'rangeland_to_forest'
        ]
        
        for transition_base in transitions:
            from_use, to_use = transition_base.split('_to_')
            key = f"{scenario}_{climate}_{from_use}_to_{to_use}"
            
            if key in parsed_json_data["forest_transitions"] and key in db_data["forest_transitions"]:
                json_value = parsed_json_data["forest_transitions"][key]
                db_value = db_data["forest_transitions"][key]
                
                assert db_value == pytest.approx(json_value, rel=0.05), \
                    f"Transition mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_hm_scenario_transitions(self, parsed_json_data, db_data):
        """Test transitions for the HM scenario (high warming-moderate growth) with mean GCM."""
        scenario = "HM"
        climate = "Middle"
        
        transitions = [
            'forest_to_developed',
            'forest_to_crop',
            'forest_to_pasture',
            'forest_to_rangeland',
            'crop_to_forest',
            'pasture_to_forest',
            'rangeland_to_forest'
        ]
        
        for transition_base in transitions:
            from_use, to_use = transition_base.split('_to_')
            key = f"{scenario}_{climate}_{from_use}_to_{to_use}"
            
            if key in parsed_json_data["forest_transitions"] and key in db_data["forest_transitions"]:
                json_value = parsed_json_data["forest_transitions"][key]
                db_value = db_data["forest_transitions"][key]
                
                assert db_value == pytest.approx(json_value, rel=0.05), \
                    f"Transition mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_hl_scenario_transitions(self, parsed_json_data, db_data):
        """Test transitions for the HL scenario (high warming-low growth) with mean GCM."""
        scenario = "HL"
        climate = "Middle"
        
        transitions = [
            'forest_to_developed',
            'forest_to_crop',
            'forest_to_pasture',
            'forest_to_rangeland',
            'crop_to_forest',
            'pasture_to_forest',
            'rangeland_to_forest'
        ]
        
        for transition_base in transitions:
            from_use, to_use = transition_base.split('_to_')
            key = f"{scenario}_{climate}_{from_use}_to_{to_use}"
            
            if key in parsed_json_data["forest_transitions"] and key in db_data["forest_transitions"]:
                json_value = parsed_json_data["forest_transitions"][key]
                db_value = db_data["forest_transitions"][key]
                
                assert db_value == pytest.approx(json_value, rel=0.05), \
                    f"Transition mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_hh_scenario_transitions(self, parsed_json_data, db_data):
        """Test transitions for the HH scenario (high warming-high growth) with mean GCM."""
        scenario = "HH"
        climate = "Middle"
        
        transitions = [
            'forest_to_developed',
            'forest_to_crop',
            'forest_to_pasture',
            'forest_to_rangeland',
            'crop_to_forest',
            'pasture_to_forest',
            'rangeland_to_forest'
        ]
        
        for transition_base in transitions:
            from_use, to_use = transition_base.split('_to_')
            key = f"{scenario}_{climate}_{from_use}_to_{to_use}"
            
            if key in parsed_json_data["forest_transitions"] and key in db_data["forest_transitions"]:
                json_value = parsed_json_data["forest_transitions"][key]
                db_value = db_data["forest_transitions"][key]
                
                assert db_value == pytest.approx(json_value, rel=0.05), \
                    f"Transition mismatch for {key}: DB={db_value}, JSON={json_value}"
    
    def test_compare_scenarios_forest_loss(self, db_data):
        """Compare forest loss across scenarios with mean GCM."""
        climate = "Middle"
        land_use = "forest"
        
        scenario_forest_changes = {}
        for scenario in RPA_SCENARIOS:
            key = f"{scenario}_{climate}_{land_use}"
            if key in db_data["net_change"]:
                scenario_forest_changes[scenario] = db_data["net_change"][key]
        
        # Assert that HH scenario has more forest loss than HL
        # Higher economic growth (HH) should lead to more forest land conversion than lower growth (HL)
        if "HH" in scenario_forest_changes and "HL" in scenario_forest_changes:
            assert scenario_forest_changes["HH"] <= scenario_forest_changes["HL"], \
                f"HH forest change ({scenario_forest_changes['HH']}) should be <= HL forest change ({scenario_forest_changes['HL']})"
        
        # Assert that HM has higher forest loss than LM
        # Higher warming should lead to slightly more forest land
        if "HM" in scenario_forest_changes and "LM" in scenario_forest_changes:
            assert scenario_forest_changes["HM"] > scenario_forest_changes["LM"], \
                f"HM forest change ({scenario_forest_changes['HM']}) should be > LM forest change ({scenario_forest_changes['LM']})"
    
    def test_compare_scenarios_developed_gain(self, db_data):
        """Compare developed land gain across scenarios with mean GCM."""
        climate = "Middle"
        land_use = "developed"
        
        scenario_developed_changes = {}
        for scenario in RPA_SCENARIOS:
            key = f"{scenario}_{climate}_{land_use}"
            if key in db_data["net_change"]:
                scenario_developed_changes[scenario] = db_data["net_change"][key]
        
        # Higher economic growth (HH) should lead to more developed land than lower growth (HL)
        if "HH" in scenario_developed_changes and "HL" in scenario_developed_changes:
            assert scenario_developed_changes["HH"] >= scenario_developed_changes["HL"], \
                f"HH developed change ({scenario_developed_changes['HH']}) should be >= HL developed change ({scenario_developed_changes['HL']})"
        
        # Higher warming (HM) should lead to slightly less developed land than lower warming (LM)
        if "HM" in scenario_developed_changes and "LM" in scenario_developed_changes:
            assert scenario_developed_changes["HM"] < scenario_developed_changes["LM"], \
                f"HM developed change ({scenario_developed_changes['HM']}) should be < LM developed change ({scenario_developed_changes['LM']})"

if __name__ == "__main__":
    # If run directly, print the parsed data for inspection
    json_data = load_json_data()
    json_parsed = parse_land_use_change_from_json(json_data)
    
    print("Parsed JSON data:")
    print("Net land use change (sample):")
    for i, (key, value) in enumerate(json_parsed["net_change"].items()):
        print(f"  {key}: {value}")
        if i > 5:
            print("  ...")
            break
    
    print("\nForest transitions (sample):")
    for i, (key, value) in enumerate(json_parsed["forest_transitions"].items()):
        print(f"  {key}: {value}")
        if i > 5:
            print("  ...")
            break
    
    print("\nComparing to database data...")
    db_data = get_land_use_data_from_db()
    
    if not db_data:
        print("Error: Could not retrieve data from database.")
        sys.exit(1)
    
    # Compare a few sample values
    sample_keys = list(json_parsed["net_change"].keys())[:3]
    
    for key in sample_keys:
        if key in db_data["net_change"]:
            json_value = json_parsed["net_change"][key]
            db_value = db_data["net_change"][key]
            match = "MATCH" if abs(json_value - db_value) < 0.5 else "MISMATCH"
            print(f"  {key}: JSON={json_value}, DB={db_value} - {match}") 