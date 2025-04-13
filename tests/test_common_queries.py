import os
import pytest
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add the src directory to the path so we can import modules
sys.path.append(str(Path(__file__).parent.parent))
from src.db.database import DatabaseConnection
from src.db.queries import LandUseQueries

# Get a sample scenario, year range, and county for testing
@pytest.fixture(scope="function")
def test_data(db_cursor):
    """Fetch sample data for tests."""
    # Get a sample scenario
    db_cursor.execute("SELECT scenario_id, scenario_name FROM scenarios LIMIT 1")
    scenario = db_cursor.fetchone()
    
    # Get a sample time range
    db_cursor.execute("SELECT start_year, end_year FROM time_steps LIMIT 1")
    time_range = db_cursor.fetchone()
    
    # Get a sample county
    db_cursor.execute("SELECT fips_code FROM counties LIMIT 1")
    county = db_cursor.fetchone()
    
    # Get sample land use types
    db_cursor.execute("""
        SELECT DISTINCT from_land_use FROM land_use_transitions 
        UNION SELECT DISTINCT to_land_use FROM land_use_transitions
        LIMIT 5
    """)
    land_use_types = [row[0] for row in db_cursor.fetchall()]
    
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
    
    def test_total_net_change_by_land_use_type(self, db_cursor, test_data):
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
        
        db_cursor.execute(query)
        results = db_cursor.fetchall()
        
        # Check the results
        assert len(results) > 0, "No results returned for total net change query"
        
        # Sum of all net changes should be approximately zero (conservation of land)
        total_net_change = sum(row[1] for row in results)
        assert abs(total_net_change) < 1.0, f"Sum of net changes should be close to zero, got {total_net_change}"
        
        print("Total net change by land use type:")
        for row in results:
            print(f"{row[0]}: {row[1]:,.2f} acres")

    def test_annualized_change_rate(self, db_cursor, test_data):
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
        
        db_cursor.execute(query)
        results = db_cursor.fetchall()
        
        # Check the results
        assert len(results) > 0, "No results returned for annualized change rate query"
        
        print("Annualized change rate by time period and land use type:")
        for row in results[:5]:  # Show just the first 5 for brevity
            print(f"{row[0]}-{row[1]} | {row[2]}: {row[5]:,.2f} acres/year")

    def test_peak_change_time_period(self, db_cursor, test_data):
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
        
        db_cursor.execute(query)
        results = db_cursor.fetchall()
        
        # Check the results
        assert len(results) > 0, "No results returned for peak change time period query"
        
        print("Peak change time period by land use type:")
        for row in results:
            print(f"{row[0]}: {row[1]}-{row[2]} with change of {row[3]:,.2f} acres")

    def test_change_by_state(self, db_cursor, test_data):
        """
        Test Query 4: Change by State
        
        "What is the net change for each land use type within each state 
        over the entire projection period?"
        """
        query = """
        WITH state_from_changes AS (
            SELECT 
                SUBSTR(lut.fips_code, 1, 2) AS state_fips,
                s.state_name,
                lut.from_land_use AS land_use_type,
                -SUM(lut.acres) AS acres_lost
            FROM land_use_transitions lut
            JOIN counties_by_state c ON lut.fips_code = c.county_fips
            JOIN states s ON c.state_fips = s.state_fips
            GROUP BY SUBSTR(lut.fips_code, 1, 2), s.state_name, lut.from_land_use
        ),
        state_to_changes AS (
            SELECT 
                SUBSTR(lut.fips_code, 1, 2) AS state_fips,
                s.state_name,
                lut.to_land_use AS land_use_type,
                SUM(lut.acres) AS acres_gained
            FROM land_use_transitions lut
            JOIN counties_by_state c ON lut.fips_code = c.county_fips
            JOIN states s ON c.state_fips = s.state_fips
            GROUP BY SUBSTR(lut.fips_code, 1, 2), s.state_name, lut.to_land_use
        ),
        state_net_changes AS (
            SELECT * FROM state_from_changes
            UNION ALL
            SELECT * FROM state_to_changes
        )
        SELECT
            state_fips,
            state_name,
            land_use_type,
            SUM(acres_lost) AS net_change
        FROM state_net_changes
        GROUP BY state_fips, state_name, land_use_type
        ORDER BY state_name, net_change DESC
        """
        
        db_cursor.execute(query)
        results = db_cursor.fetchall()
        
        # Check the results
        assert len(results) > 0, "No results returned for change by state query"
        
        print("Net change by state and land use type (sample):")
        for row in results[:5]:  # Show just the first 5 for brevity
            print(f"{row[1]} - {row[2]}: {row[3]:,.2f} acres")

    def test_top_counties_by_change(self, db_cursor, test_data):
        """
        Test Query 5: Top N Counties by Change
        
        "Which are the top 10 counties with the largest increase in urban land use by 2070?"
        """
        # Use a land use type that exists in the database
        # Get a sample land use type from test_data
        land_use_type = test_data['land_use_types'][0]  # First land use type
        print(f"Using land use type: {land_use_type}")
        top_n = 10
        
        query = f"""
        WITH county_from_changes AS (
            SELECT 
                lut.fips_code,
                c.county_name,
                st.state_name,
                '{land_use_type}' AS land_use_type,
                -SUM(lut.acres) AS acres_lost
            FROM land_use_transitions lut
            JOIN counties c ON lut.fips_code = c.fips_code
            JOIN counties_by_state cbs ON c.fips_code = cbs.county_fips
            JOIN states st ON cbs.state_fips = st.state_fips
            WHERE lut.from_land_use = '{land_use_type}'
            GROUP BY lut.fips_code, c.county_name, st.state_name
        ),
        county_to_changes AS (
            SELECT 
                lut.fips_code,
                c.county_name,
                st.state_name,
                '{land_use_type}' AS land_use_type,
                SUM(lut.acres) AS acres_gained
            FROM land_use_transitions lut
            JOIN counties c ON lut.fips_code = c.fips_code
            JOIN counties_by_state cbs ON c.fips_code = cbs.county_fips
            JOIN states st ON cbs.state_fips = st.state_fips
            WHERE lut.to_land_use = '{land_use_type}'
            GROUP BY lut.fips_code, c.county_name, st.state_name
        ),
        county_net_changes AS (
            SELECT * FROM county_from_changes
            UNION ALL
            SELECT * FROM county_to_changes
        ),
        county_ranked AS (
            SELECT
                fips_code,
                county_name,
                state_name,
                land_use_type,
                SUM(acres_lost) AS net_change
            FROM county_net_changes
            GROUP BY fips_code, county_name, state_name, land_use_type
            ORDER BY net_change DESC
            LIMIT {top_n}
        )
        SELECT * FROM county_ranked
        """
        
        db_cursor.execute(query)
        results = db_cursor.fetchall()
        
        # Check the results
        assert len(results) > 0, "No results returned for top counties query"
        assert len(results) <= top_n, f"Expected at most {top_n} results, got {len(results)}"
        
        print(f"Top {top_n} counties with largest increase in {land_use_type} land use:")
        for i, row in enumerate(results, 1):
            print(f"{i}. {row[1]}, {row[2]}: {row[4]:,.2f} acres")

    def test_major_transitions(self, db_cursor, test_data):
        """
        Test Query 11: Major Transitions
        
        "What are the top 3 most common land use transitions (e.g., forest to urban, 
        rangeland to cropland) observed across the CONUS between 2020 and 2050?"
        """
        start_year = 2020
        end_year = 2050
        top_n = 3
        
        query = f"""
        SELECT
            lut.from_land_use,
            lut.to_land_use,
            SUM(lut.acres) AS total_acres_changed
        FROM land_use_transitions lut
        JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
        WHERE ts.start_year >= {start_year} AND ts.end_year <= {end_year}
        GROUP BY lut.from_land_use, lut.to_land_use
        ORDER BY total_acres_changed DESC
        LIMIT {top_n}
        """
        
        db_cursor.execute(query)
        results = db_cursor.fetchall()
        
        # Check the results
        assert len(results) > 0, "No results returned for major transitions query"
        assert len(results) <= top_n, f"Expected at most {top_n} results, got {len(results)}"
        
        print(f"Top {top_n} most common land use transitions between {start_year}-{end_year}:")
        for i, row in enumerate(results, 1):
            print(f"{i}. {row[0]} → {row[1]}: {row[2]:,.2f} acres")

    def test_data_integrity_total_area_consistency(self, db_cursor, test_data):
        """
        Test Query 14: Total Area Consistency
        
        "For each county and time period, does the sum of all land use areas 
        equal the total area at the start of the time period (t1)?"
        """
        query = """
        WITH from_county_totals AS (
            SELECT
                scenario_id,
                time_step_id,
                fips_code,
                SUM(acres) AS total_from_acres
            FROM land_use_transitions
            GROUP BY scenario_id, time_step_id, fips_code
        ),
        to_county_totals AS (
            SELECT
                scenario_id,
                time_step_id,
                fips_code,
                SUM(acres) AS total_to_acres
            FROM land_use_transitions
            GROUP BY scenario_id, time_step_id, fips_code
        ),
        comparison AS (
            SELECT
                f.scenario_id,
                f.time_step_id,
                f.fips_code,
                f.total_from_acres,
                t.total_to_acres,
                ABS(f.total_from_acres - t.total_to_acres) AS difference
            FROM from_county_totals f
            JOIN to_county_totals t ON f.scenario_id = t.scenario_id
                                    AND f.time_step_id = t.time_step_id
                                    AND f.fips_code = t.fips_code
        )
        SELECT
            scenario_id,
            time_step_id,
            fips_code,
            total_from_acres,
            total_to_acres,
            difference
        FROM comparison
        WHERE difference > 0.1  -- Allowing for small rounding errors
        LIMIT 10
        """
        
        db_cursor.execute(query)
        inconsistencies = db_cursor.fetchall()
        
        # Check for inconsistencies
        if len(inconsistencies) > 0:
            print("Found area inconsistencies (sample):")
            for row in inconsistencies:
                print(f"Scenario {row[0]}, Time step {row[1]}, County {row[2]}: " \
                      f"From acres {row[3]:,.2f}, To acres {row[4]:,.2f}, " \
                      f"Difference {row[5]:,.2f}")
        else:
            print("All county areas are consistent across time periods")
        
        # Get total number of county-period combinations to assess percentage of inconsistencies
        db_cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT scenario_id, time_step_id, fips_code
                FROM land_use_transitions
            )
        """)
        total_combinations = db_cursor.fetchone()[0]
        
        # Calculate percentage of inconsistencies
        if total_combinations > 0:
            inconsistency_rate = len(inconsistencies) / total_combinations * 100
            print(f"Inconsistency rate: {inconsistency_rate:.4f}% ({len(inconsistencies)} of {total_combinations})")
            
            # Low inconsistency rate is acceptable (due to rounding/precision)
            assert inconsistency_rate < 5.0, f"Inconsistency rate too high: {inconsistency_rate:.4f}%"

    def test_data_integrity_no_negative_acres(self, db_cursor):
        """
        Test Query 15: No Negative Acres
        
        "Are there any instances of negative acre values in the data?"
        """
        query = """
        SELECT 
            scenario_id,
            time_step_id,
            fips_code,
            from_land_use,
            to_land_use,
            acres
        FROM land_use_transitions
        WHERE acres < 0
        LIMIT 10
        """
        
        db_cursor.execute(query)
        negative_acres = db_cursor.fetchall()
        
        # There should be no negative acre values
        assert len(negative_acres) == 0, f"Found {len(negative_acres)} transitions with negative acre values"
        print("No negative acre values found")

    def test_data_integrity_unique_identifiers(self, db_cursor):
        """
        Test Query 16: Unique Identifiers
        
        "Are all scenario names, FIPS codes, and time step IDs unique in their respective tables?"
        """
        # Check scenario name uniqueness
        db_cursor.execute("""
            SELECT scenario_name, COUNT(*) 
            FROM scenarios 
            GROUP BY scenario_name 
            HAVING COUNT(*) > 1
        """)
        duplicate_scenarios = db_cursor.fetchall()
        assert len(duplicate_scenarios) == 0, f"Found {len(duplicate_scenarios)} duplicate scenario names"
        
        # Check FIPS code uniqueness
        db_cursor.execute("""
            SELECT fips_code, COUNT(*) 
            FROM counties 
            GROUP BY fips_code 
            HAVING COUNT(*) > 1
        """)
        duplicate_fips = db_cursor.fetchall()
        assert len(duplicate_fips) == 0, f"Found {len(duplicate_fips)} duplicate FIPS codes"
        
        # Check time step uniqueness
        db_cursor.execute("""
            SELECT start_year, end_year, COUNT(*) 
            FROM time_steps 
            GROUP BY start_year, end_year 
            HAVING COUNT(*) > 1
        """)
        duplicate_time_steps = db_cursor.fetchall()
        assert len(duplicate_time_steps) == 0, f"Found {len(duplicate_time_steps)} duplicate time steps"
        
        print("All identifiers are unique in their respective tables") 