import os
import pytest
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add the src directory to the path so we can import modules
sys.path.append(str(Path(__file__).parent.parent))
from src.db.database import DatabaseConnection
from src.db.queries import LandUseQueries

# Define database path
DB_PATH = os.getenv('DB_PATH', 'data/database/rpa_landuse_duck.db')

# Create fixtures for database connection and cursor
@pytest.fixture(scope="module")
def db_connection():
    conn = duckdb.connect(DB_PATH)
    yield conn
    conn.close()

@pytest.fixture(scope="function")
def db_cursor(db_connection):
    return db_connection

# Get a sample scenario, year range, and county for testing
@pytest.fixture(scope="function")
def test_data(db_connection):
    """Fetch sample data for tests."""
    # Get a sample scenario
    scenario = db_connection.execute("SELECT scenario_id, scenario_name FROM scenarios LIMIT 1").fetchone()
    
    # Get a sample time range
    time_range = db_connection.execute("SELECT start_year, end_year FROM time_steps LIMIT 1").fetchone()
    
    # Get a sample county
    county = db_connection.execute("SELECT fips_code FROM counties LIMIT 1").fetchone()
    
    # Get sample land use types
    land_use_types = db_connection.execute("""
        SELECT DISTINCT from_land_use FROM land_use_transitions 
        UNION SELECT DISTINCT to_land_use FROM land_use_transitions
        LIMIT 5
    """).fetchdf()['from_land_use'].tolist()
    
    return {
        'scenario_id': scenario[0],
        'scenario_name': scenario[1],
        'start_year': time_range[0],
        'end_year': time_range[1],
        'county_fips': county[0],
        'land_use_types': land_use_types
    }

class TestCoreQueries:
    """Test implementation of core queries from common_queries.md."""
    
    def test_total_net_change_by_land_use_type(self, db_connection, test_data):
        """
        Test Query 1: Total Net Change by Land Use Type
        
        "What is the total net change (gain or loss in acres) for each land use type 
        (cropland, pasture, rangeland, forest, urban) across all counties over the 
        entire projection period (2012-2070)?"
        """
        query = """
        WITH from_sums AS (
            SELECT 
                from_land_use AS land_use_type,
                -SUM(acres) AS net_change
            FROM land_use_transitions
            GROUP BY from_land_use
        ),
        to_sums AS (
            SELECT 
                to_land_use AS land_use_type,
                SUM(acres) AS net_change
            FROM land_use_transitions
            GROUP BY to_land_use
        ),
        combined AS (
            SELECT * FROM from_sums
            UNION ALL
            SELECT * FROM to_sums
        )
        SELECT 
            land_use_type,
            SUM(net_change) AS total_net_change
        FROM combined
        GROUP BY land_use_type
        ORDER BY total_net_change DESC
        """
        
        results = db_connection.execute(query).fetchall()
        
        # Check the results
        assert len(results) > 0, "No results returned for total net change query"
        
        # Sum of all net changes should be approximately zero (conservation of land)
        total_net_change = sum(row[1] for row in results)
        assert abs(total_net_change) < 1.0, f"Sum of net changes should be close to zero, got {total_net_change}"
        
        print("Total net change by land use type:")
        for row in results:
            print(f"{row[0]}: {row[1]:,.2f} acres")

    def test_annualized_change_rate(self, db_connection, test_data):
        """
        Test Query 2: Annualized Change Rate
        
        "What is the average annual rate of change (in acres per year) for each 
        land use type during each projection period (2020-2030, 2030-2040, etc.)?"
        """
        query = """
        WITH period_changes AS (
            SELECT 
                ts.start_year,
                ts.end_year,
                lut.from_land_use AS land_use_type,
                -SUM(lut.acres) AS acres_lost
            FROM land_use_transitions lut
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            GROUP BY ts.start_year, ts.end_year, lut.from_land_use
            
            UNION ALL
            
            SELECT 
                ts.start_year,
                ts.end_year,
                lut.to_land_use AS land_use_type,
                SUM(lut.acres) AS acres_gained
            FROM land_use_transitions lut
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            GROUP BY ts.start_year, ts.end_year, lut.to_land_use
        ),
        net_changes AS (
            SELECT
                start_year,
                end_year,
                land_use_type,
                SUM(acres_lost) AS net_change
            FROM period_changes
            GROUP BY start_year, end_year, land_use_type
        )
        SELECT
            start_year,
            end_year,
            land_use_type,
            net_change,
            (end_year - start_year) AS period_years,
            net_change / (end_year - start_year) AS annual_change_rate
        FROM net_changes
        ORDER BY start_year, annual_change_rate DESC
        """
        
        results = db_connection.execute(query).fetchall()
        
        # Check the results
        assert len(results) > 0, "No results returned for annualized change rate query"
        
        print("Annualized change rate by time period and land use type:")
        for row in results[:5]:  # Show just the first 5 for brevity
            print(f"{row[0]}-{row[1]} | {row[2]}: {row[5]:,.2f} acres/year")

    def test_peak_change_time_period(self, db_connection, test_data):
        """
        Test Query 3: Peak Change Time Period
        
        "During which 10-year period is the largest net change observed for each land use type?"
        """
        query = """
        WITH period_changes AS (
            SELECT 
                ts.start_year,
                ts.end_year,
                lut.from_land_use AS land_use_type,
                -SUM(lut.acres) AS acres_lost
            FROM land_use_transitions lut
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            GROUP BY ts.start_year, ts.end_year, lut.from_land_use
            
            UNION ALL
            
            SELECT 
                ts.start_year,
                ts.end_year,
                lut.to_land_use AS land_use_type,
                SUM(lut.acres) AS acres_gained
            FROM land_use_transitions lut
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            GROUP BY ts.start_year, ts.end_year, lut.to_land_use
        ),
        net_changes AS (
            SELECT
                start_year,
                end_year,
                land_use_type,
                SUM(acres_lost) AS net_change,
                ABS(SUM(acres_lost)) AS absolute_change
            FROM period_changes
            GROUP BY start_year, end_year, land_use_type
        ),
        ranked_changes AS (
            SELECT
                start_year,
                end_year,
                land_use_type,
                net_change,
                absolute_change,
                ROW_NUMBER() OVER (PARTITION BY land_use_type ORDER BY absolute_change DESC) as rank
            FROM net_changes
        )
        SELECT
            land_use_type,
            start_year,
            end_year,
            net_change
        FROM ranked_changes
        WHERE rank = 1
        ORDER BY absolute_change DESC
        """
        
        results = db_connection.execute(query).fetchall()
        
        # Check the results
        assert len(results) > 0, "No results returned for peak change time period query"
        
        print("Peak change time period by land use type:")
        for row in results:
            print(f"{row[0]}: {row[1]}-{row[2]} with change of {row[3]:,.2f} acres")

    def test_change_by_state(self, db_connection, test_data):
        """
        Test Query 4: Change by State
        
        "What is the net change for each land use type within each state 
        over the entire projection period?"
        """
        query = """
        WITH state_from_changes AS (
            SELECT 
                SUBSTRING(lut.fips_code, 1, 2) AS state_fips,
                s.state_name,
                lut.from_land_use AS land_use_type,
                -SUM(lut.acres) AS acres_lost
            FROM land_use_transitions lut
            JOIN counties_by_state c ON lut.fips_code = c.county_fips
            JOIN states s ON c.state_fips = s.state_fips
            GROUP BY SUBSTRING(lut.fips_code, 1, 2), s.state_name, lut.from_land_use
        ),
        state_to_changes AS (
            SELECT 
                SUBSTRING(lut.fips_code, 1, 2) AS state_fips,
                s.state_name,
                lut.to_land_use AS land_use_type,
                SUM(lut.acres) AS acres_gained
            FROM land_use_transitions lut
            JOIN counties_by_state c ON lut.fips_code = c.county_fips
            JOIN states s ON c.state_fips = s.state_fips
            GROUP BY SUBSTRING(lut.fips_code, 1, 2), s.state_name, lut.to_land_use
        ),
        combined AS (
            SELECT * FROM state_from_changes
            UNION ALL
            SELECT * FROM state_to_changes
        )
        SELECT 
            state_name,
            land_use_type,
            SUM(acres_lost) AS net_change
        FROM combined
        GROUP BY state_name, land_use_type
        ORDER BY state_name, net_change DESC
        """
        
        results = db_connection.execute(query).fetchall()
        
        # Check the results
        assert len(results) > 0, "No results returned for change by state query"
        
        # Sample of results
        print("Net change by state (sample):")
        sample_state = results[0][0]  # Get first state in results
        for row in results:
            if row[0] == sample_state:
                print(f"{row[0]} - {row[1]}: {row[2]:,.2f} acres")

    def test_top_counties_by_change(self, db_connection, test_data):
        """
        Test Query 5: Top Counties by Change
        
        "Which counties experience the largest absolute change (gain or loss) for 
        each land use type? List the top 5 counties by absolute change for each land use type."
        """
        query = """
        WITH county_from_changes AS (
            SELECT 
                lut.fips_code,
                c.county_name,
                s.state_name,
                lut.from_land_use AS land_use_type,
                -SUM(lut.acres) AS acres_lost
            FROM land_use_transitions lut
            JOIN counties c ON lut.fips_code = c.fips_code
            JOIN county_state_map s ON c.fips_code = s.county_fips
            GROUP BY lut.fips_code, c.county_name, s.state_name, lut.from_land_use
        ),
        county_to_changes AS (
            SELECT 
                lut.fips_code,
                c.county_name,
                s.state_name,
                lut.to_land_use AS land_use_type,
                SUM(lut.acres) AS acres_gained
            FROM land_use_transitions lut
            JOIN counties c ON lut.fips_code = c.fips_code
            JOIN county_state_map s ON c.fips_code = s.county_fips
            GROUP BY lut.fips_code, c.county_name, s.state_name, lut.to_land_use
        ),
        combined AS (
            SELECT * FROM county_from_changes
            UNION ALL
            SELECT * FROM county_to_changes
        ),
        county_net_changes AS (
            SELECT 
                fips_code,
                county_name,
                state_name,
                land_use_type,
                SUM(acres_lost) AS net_change,
                ABS(SUM(acres_lost)) AS absolute_change
            FROM combined
            GROUP BY fips_code, county_name, state_name, land_use_type
        ),
        ranked_counties AS (
            SELECT
                fips_code,
                county_name,
                state_name,
                land_use_type,
                net_change,
                absolute_change,
                ROW_NUMBER() OVER (PARTITION BY land_use_type ORDER BY absolute_change DESC) as rank
            FROM county_net_changes
        )
        SELECT
            land_use_type,
            fips_code,
            county_name,
            state_name,
            net_change,
            absolute_change,
            rank
        FROM ranked_counties
        WHERE rank <= 5
        ORDER BY land_use_type, rank
        """
        
        results = db_connection.execute(query).fetchall()
        
        # Check the results
        assert len(results) > 0, "No results returned for top counties by change query"
        
        # Check that we have top 5 for each land use type
        land_use_counts = {}
        for row in results:
            land_use = row[0]
            if land_use not in land_use_counts:
                land_use_counts[land_use] = 0
            land_use_counts[land_use] += 1
        
        # Assuming we have at least one land use type
        for land_use, count in land_use_counts.items():
            assert count <= 5, f"Expected maximum 5 counties per land use type, got {count} for {land_use}"
        
        # Sample of results
        print("Top counties by change (sample):")
        sample_land_use = results[0][0]  # Get first land use type in results
        for row in results:
            if row[0] == sample_land_use:
                print(f"{row[0]} - Rank {row[6]}: {row[2]}, {row[3]} with change of {row[4]:,.2f} acres")

    def test_major_transitions(self, db_connection, test_data):
        """
        Test Query 6: Major Transitions
        
        "What are the most significant land use transitions (e.g., forest to urban, 
        cropland to urban) in terms of total acres converted over the entire projection period?"
        """
        query = """
        SELECT 
            from_land_use,
            to_land_use,
            SUM(acres) AS total_acres
        FROM land_use_transitions
        GROUP BY from_land_use, to_land_use
        ORDER BY total_acres DESC
        LIMIT 10
        """
        
        results = db_connection.execute(query).fetchall()
        
        # Check the results
        assert len(results) > 0, "No results returned for major transitions query"
        assert len(results) <= 10, "Expected at most 10 results"
        
        print("Major transitions:")
        for row in results:
            print(f"{row[0]} to {row[1]}: {row[2]:,.2f} acres")

    def test_data_integrity_total_area_consistency(self, db_connection, test_data):
        """
        Test Data Integrity: Total Area Consistency Check
        
        "Verify that the total area of all land use types remains consistent over 
        time (conservation of land area principle)."
        """
        query = """
        WITH initial_areas AS (
            SELECT 
                lut.scenario_id,
                s.scenario_name,
                ts.start_year,
                lut.from_land_use,
                SUM(lut.acres) AS initial_area
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            WHERE ts.start_year = (SELECT MIN(start_year) FROM time_steps)
            GROUP BY lut.scenario_id, s.scenario_name, ts.start_year, lut.from_land_use
        ),
        final_areas AS (
            SELECT 
                lut.scenario_id,
                s.scenario_name,
                ts.end_year AS year,
                lut.to_land_use AS land_use,
                SUM(lut.acres) AS final_area
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            WHERE ts.end_year = (SELECT MAX(end_year) FROM time_steps)
            GROUP BY lut.scenario_id, s.scenario_name, ts.end_year, lut.to_land_use
        ),
        total_areas AS (
            SELECT 
                scenario_id,
                scenario_name,
                start_year AS year,
                SUM(initial_area) AS total_area
            FROM initial_areas
            GROUP BY scenario_id, scenario_name, start_year
            
            UNION ALL
            
            SELECT 
                scenario_id,
                scenario_name,
                year,
                SUM(final_area) AS total_area
            FROM final_areas
            GROUP BY scenario_id, scenario_name, year
        )
        SELECT 
            scenario_name,
            year,
            total_area
        FROM total_areas
        ORDER BY scenario_id, year
        """
        
        results = db_connection.execute(query).fetchall()
        
        # Check the results
        assert len(results) > 0, "No results returned for total area consistency check"
        
        # Group by scenario
        scenarios = {}
        for row in results:
            scenario = row[0]
            if scenario not in scenarios:
                scenarios[scenario] = []
            scenarios[scenario].append((row[1], row[2]))
        
        # Check consistency for each scenario
        for scenario, areas in scenarios.items():
            # Sort by year
            areas.sort(key=lambda x: x[0])
            
            # Check if first and last area are similar (within 0.1% tolerance)
            if len(areas) >= 2:
                first_area = areas[0][1]
                last_area = areas[-1][1]
                difference_pct = abs(first_area - last_area) / first_area * 100
                
                assert difference_pct < 0.1, f"Total area changed by {difference_pct:.2f}% in scenario {scenario}"
                print(f"Scenario {scenario}: Area conservation check passed. Difference: {difference_pct:.4f}%")

    def test_data_integrity_no_negative_acres(self, db_connection):
        """
        Test Data Integrity: No Negative Acres
        
        "Ensure that no land use transition record has negative acres."
        """
        query = """
        SELECT COUNT(*) 
        FROM land_use_transitions 
        WHERE acres < 0
        """
        
        result = db_connection.execute(query).fetchone()
        count = result[0]
        
        assert count == 0, f"Found {count} records with negative acres"
        print("Negative acres check passed: No records with negative acres found")
        
        # Also check for zero acres as potentially suspicious
        query = """
        SELECT COUNT(*) 
        FROM land_use_transitions 
        WHERE acres = 0
        """
        
        result = db_connection.execute(query).fetchone()
        count = result[0]
        
        print(f"Found {count} records with exactly zero acres")

    def test_data_integrity_unique_identifiers(self, db_connection):
        """
        Test Data Integrity: Unique Identifiers
        
        "Verify that primary keys are unique across relevant tables."
        """
        # Test scenarios
        result = db_connection.execute("""
            SELECT COUNT(*) FROM (
                SELECT scenario_id, COUNT(*) 
                FROM scenarios 
                GROUP BY scenario_id 
                HAVING COUNT(*) > 1
            )
        """).fetchone()
        assert result[0] == 0, f"Found {result[0]} duplicate scenario_id values"
        
        # Test time_steps
        result = db_connection.execute("""
            SELECT COUNT(*) FROM (
                SELECT time_step_id, COUNT(*) 
                FROM time_steps 
                GROUP BY time_step_id 
                HAVING COUNT(*) > 1
            )
        """).fetchone()
        assert result[0] == 0, f"Found {result[0]} duplicate time_step_id values"
        
        # Test counties
        result = db_connection.execute("""
            SELECT COUNT(*) FROM (
                SELECT fips_code, COUNT(*) 
                FROM counties 
                GROUP BY fips_code 
                HAVING COUNT(*) > 1
            )
        """).fetchone()
        assert result[0] == 0, f"Found {result[0]} duplicate fips_code values"
        
        # Test transitions
        result = db_connection.execute("""
            SELECT COUNT(*) FROM (
                SELECT transition_id, COUNT(*) 
                FROM land_use_transitions 
                GROUP BY transition_id 
                HAVING COUNT(*) > 1
            )
        """).fetchone()
        assert result[0] == 0, f"Found {result[0]} duplicate transition_id values"
        
        print("Unique identifiers check passed: All primary keys are unique")


if __name__ == "__main__":
    pytest.main(["-v", __file__]) 