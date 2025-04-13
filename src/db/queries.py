"""
Land use data query module for RPA land use viewer.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from .database import DatabaseConnection

logger = logging.getLogger(__name__)

class LandUseQueries:
    """Implementation of common land use change analysis queries."""
    
    @staticmethod
    def analyze_county_transitions(
        county_fips: str,
        start_year: int,
        end_year: int,
        scenario_name: str,
        land_use_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Analyze land use transitions for a specific county.
        
        Args:
            county_fips: County FIPS code
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            scenario_name: Scenario name
            land_use_type: Optional land use type to filter by
            
        Returns:
            List of dictionaries containing transition details
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            cursor = conn.cursor()
            
            # First get the scenario ID
            cursor.execute(
                "SELECT scenario_id FROM scenarios WHERE scenario_name = ?", 
                (scenario_name,)
            )
            scenario_id = cursor.fetchone()
            if not scenario_id:
                raise ValueError(f"Scenario not found: {scenario_name}")
            scenario_id = scenario_id[0]
            
            # Get the time step IDs
            cursor.execute(
                "SELECT time_step_id FROM time_steps WHERE start_year >= ? AND end_year <= ?", 
                (start_year, end_year)
            )
            time_step_ids = [row[0] for row in cursor.fetchall()]
            if not time_step_ids:
                raise ValueError(f"No time steps found between {start_year} and {end_year}")
            
            # Build the query
            query = """
                SELECT 
                    t.from_land_use,
                    t.to_land_use,
                    SUM(t.acres) as acres_changed,
                    (SUM(t.acres) / 
                        (SELECT SUM(acres) FROM land_use_transitions 
                         WHERE scenario_id = ? AND time_step_id IN ({}) 
                         AND fips_code = ? AND from_land_use = t.from_land_use)
                    ) * 100 as percentage_of_source_loss
                FROM 
                    land_use_transitions t
                WHERE 
                    t.scenario_id = ? 
                    AND t.time_step_id IN ({})
                    AND t.fips_code = ?
                    {}
                GROUP BY 
                    t.from_land_use, t.to_land_use
                ORDER BY 
                    acres_changed DESC
            """.format(
                ','.join(['?'] * len(time_step_ids)),
                ','.join(['?'] * len(time_step_ids)),
                "AND t.from_land_use = ?" if land_use_type else ""
            )
            
            # Prepare the parameters
            params = [scenario_id] + time_step_ids + [county_fips, scenario_id] + time_step_ids + [county_fips]
            if land_use_type:
                params.append(land_use_type)
            
            # Execute the query
            cursor.execute(query, params)
            results = []
            for row in cursor.fetchall():
                results.append({
                    'from_land_use': row[0],
                    'to_land_use': row[1],
                    'acres_changed': row[2],
                    'percentage_of_source_loss': row[3]
                })
            
            return results
        except Exception as e:
            logger.error(f"Error analyzing county transitions: {e}")
            raise
        finally:
            if conn and conn != DatabaseConnection._connection:
                conn.close()
    
    @staticmethod
    def compare_scenarios(
        start_year: int,
        end_year: int,
        land_use_type: str,
        scenario_1: str,
        scenario_2: str
    ) -> List[Dict[str, Any]]:
        """
        Compare land use changes between two scenarios.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            land_use_type: Land use type to analyze
            scenario_1: First scenario name
            scenario_2: Second scenario name
            
        Returns:
            List of dictionaries containing comparison results
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            cursor = conn.cursor()
            
            # Get scenario details
            scenarios = []
            for scenario_name in [scenario_1, scenario_2]:
                cursor.execute(
                    """
                    SELECT scenario_id, scenario_name, gcm, rcp, ssp 
                    FROM scenarios 
                    WHERE scenario_name = ?
                    """, 
                    (scenario_name,)
                )
                row = cursor.fetchone()
                if not row:
                    raise ValueError(f"Scenario not found: {scenario_name}")
                scenarios.append({
                    'scenario_id': row[0],
                    'scenario_name': row[1],
                    'gcm': row[2],
                    'rcp': row[3],
                    'ssp': row[4]
                })
            
            # Calculate net change for each scenario
            results = []
            for scenario in scenarios:
                # Get time steps
                cursor.execute(
                    "SELECT time_step_id FROM time_steps WHERE start_year >= ? AND end_year <= ?", 
                    (start_year, end_year)
                )
                time_step_ids = [row[0] for row in cursor.fetchall()]
                
                # Calculate net change
                query = """
                    SELECT 
                        SUM(CASE WHEN to_land_use = ? THEN acres ELSE 0 END) - 
                        SUM(CASE WHEN from_land_use = ? THEN acres ELSE 0 END) AS net_change
                    FROM 
                        land_use_transitions
                    WHERE 
                        scenario_id = ? 
                        AND time_step_id IN ({})
                """.format(','.join(['?'] * len(time_step_ids)))
                
                params = [land_use_type, land_use_type, scenario['scenario_id']] + time_step_ids
                cursor.execute(query, params)
                net_change = cursor.fetchone()[0]
                
                # Calculate annual rate
                total_years = end_year - start_year
                annual_rate = net_change / total_years if total_years > 0 else 0
                
                # Add to results
                scenario['net_change'] = net_change
                scenario['annual_rate'] = annual_rate
                results.append(scenario)
            
            return results
        except Exception as e:
            logger.error(f"Error comparing scenarios: {e}")
            raise
        finally:
            if conn and conn != DatabaseConnection._connection:
                conn.close()
    
    @staticmethod
    def major_transitions(
        start_year: int,
        end_year: int,
        scenario_name: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Identify major land use transitions.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            scenario_name: Scenario name
            limit: Number of transitions to return
            
        Returns:
            List of dictionaries containing major transitions
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            cursor = conn.cursor()
            
            # Get scenario ID
            cursor.execute(
                "SELECT scenario_id FROM scenarios WHERE scenario_name = ?", 
                (scenario_name,)
            )
            scenario_id = cursor.fetchone()
            if not scenario_id:
                raise ValueError(f"Scenario not found: {scenario_name}")
            scenario_id = scenario_id[0]
            
            # Get time step IDs
            cursor.execute(
                "SELECT time_step_id FROM time_steps WHERE start_year >= ? AND end_year <= ?", 
                (start_year, end_year)
            )
            time_step_ids = [row[0] for row in cursor.fetchall()]
            
            # Build the query
            query = """
                WITH total_changes AS (
                    SELECT SUM(acres) as total 
                    FROM land_use_transitions 
                    WHERE scenario_id = ? AND time_step_id IN ({})
                )
                SELECT 
                    t.from_land_use,
                    t.to_land_use,
                    SUM(t.acres) as acres_changed,
                    (SUM(t.acres) / (SELECT total FROM total_changes)) * 100 as percentage_of_all_changes
                FROM 
                    land_use_transitions t
                WHERE 
                    t.scenario_id = ? 
                    AND t.time_step_id IN ({})
                    AND t.from_land_use != t.to_land_use
                GROUP BY 
                    t.from_land_use, t.to_land_use
                ORDER BY 
                    acres_changed DESC
                LIMIT ?
            """.format(
                ','.join(['?'] * len(time_step_ids)),
                ','.join(['?'] * len(time_step_ids))
            )
            
            # Prepare parameters
            params = [scenario_id] + time_step_ids + [scenario_id] + time_step_ids + [limit]
            
            # Execute query
            cursor.execute(query, params)
            results = []
            for row in cursor.fetchall():
                results.append({
                    'from_land_use': row[0],
                    'to_land_use': row[1],
                    'acres_changed': row[2],
                    'percentage_of_all_changes': row[3]
                })
            
            return results
        except Exception as e:
            logger.error(f"Error identifying major transitions: {e}")
            raise
        finally:
            if conn and conn != DatabaseConnection._connection:
                conn.close()
    
    @staticmethod
    def top_counties_by_change(
        start_year: int,
        end_year: int,
        land_use_type: str,
        scenario_name: str,
        limit: int = 10,
        direction: str = 'decrease'
    ) -> List[Dict[str, Any]]:
        """
        Find counties with highest land use changes.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            land_use_type: Land use type to analyze
            scenario_name: Scenario name
            limit: Number of counties to return
            direction: 'increase' or 'decrease' to find counties with highest increase or decrease
            
        Returns:
            List of dictionaries containing county information
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            cursor = conn.cursor()
            
            # Get scenario ID
            cursor.execute(
                "SELECT scenario_id FROM scenarios WHERE scenario_name = ?", 
                (scenario_name,)
            )
            scenario_id = cursor.fetchone()
            if not scenario_id:
                raise ValueError(f"Scenario not found: {scenario_name}")
            scenario_id = scenario_id[0]
            
            # Get time step IDs
            cursor.execute(
                "SELECT time_step_id FROM time_steps WHERE start_year >= ? AND end_year <= ?", 
                (start_year, end_year)
            )
            time_step_ids = [row[0] for row in cursor.fetchall()]
            
            # Determine order direction
            order_dir = "ASC" if direction == 'decrease' else "DESC"
            
            # Build the query
            query = """
                SELECT 
                    c.fips_code,
                    c.county_name,
                    SUM(CASE WHEN t.to_land_use = ? THEN t.acres ELSE 0 END) - 
                    SUM(CASE WHEN t.from_land_use = ? THEN t.acres ELSE 0 END) AS net_change
                FROM 
                    land_use_transitions t
                JOIN
                    counties c ON t.fips_code = c.fips_code
                WHERE 
                    t.scenario_id = ? 
                    AND t.time_step_id IN ({})
                    AND (t.from_land_use = ? OR t.to_land_use = ?)
                GROUP BY 
                    c.fips_code, c.county_name
                ORDER BY 
                    net_change {}
                LIMIT ?
            """.format(
                ','.join(['?'] * len(time_step_ids)),
                order_dir
            )
            
            # Prepare parameters
            params = [land_use_type, land_use_type, scenario_id] + time_step_ids + [land_use_type, land_use_type, limit]
            
            # Execute query
            cursor.execute(query, params)
            results = []
            for row in cursor.fetchall():
                results.append({
                    'fips_code': row[0],
                    'county_name': row[1],
                    'net_change': row[2]
                })
            
            return results
        except Exception as e:
            logger.error(f"Error finding top counties by change: {e}")
            raise
        finally:
            if conn and conn != DatabaseConnection._connection:
                conn.close()
    
    @staticmethod
    def check_data_integrity(scenario_name: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Perform basic data integrity checks.
        
        Args:
            scenario_name: Scenario name to check
            
        Returns:
            Dictionary with lists of inconsistencies found
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            cursor = conn.cursor()
            
            # Get scenario ID
            cursor.execute(
                "SELECT scenario_id FROM scenarios WHERE scenario_name = ?", 
                (scenario_name,)
            )
            scenario_id = cursor.fetchone()
            if not scenario_id:
                raise ValueError(f"Scenario not found: {scenario_name}")
            scenario_id = scenario_id[0]
            
            # Initialize results
            results = {
                "area_inconsistencies": [],
                "negative_acres": []
            }
            
            # Check for negative acre values
            cursor.execute("""
                SELECT 
                    t.fips_code,
                    ts.start_year,
                    ts.end_year,
                    t.from_land_use,
                    t.to_land_use,
                    t.acres
                FROM 
                    land_use_transitions t
                JOIN
                    time_steps ts ON t.time_step_id = ts.time_step_id
                WHERE 
                    t.scenario_id = ? AND t.acres < 0
            """, (scenario_id,))
            
            for row in cursor.fetchall():
                results["negative_acres"].append({
                    "fips_code": row[0],
                    "start_year": row[1],
                    "end_year": row[2],
                    "from_land_use": row[3],
                    "to_land_use": row[4],
                    "acres": row[5]
                })
            
            # Check for area inconsistencies
            # (This is a simplified check - would need more complex logic for complete check)
            cursor.execute("""
                WITH county_areas AS (
                    SELECT 
                        fips_code,
                        ts.start_year,
                        SUM(CASE WHEN from_land_use != to_land_use THEN acres ELSE 0 END) as area_changed
                    FROM 
                        land_use_transitions t
                    JOIN
                        time_steps ts ON t.time_step_id = ts.time_step_id
                    WHERE 
                        t.scenario_id = ?
                    GROUP BY 
                        fips_code, ts.start_year
                )
                SELECT 
                    ca1.fips_code,
                    ca1.start_year,
                    ABS(ca1.area_changed - ca2.area_changed) as area_difference
                FROM 
                    county_areas ca1
                JOIN
                    county_areas ca2 ON ca1.fips_code = ca2.fips_code AND ca1.start_year < ca2.start_year
                WHERE 
                    ABS(ca1.area_changed - ca2.area_changed) > 0.1 * ca1.area_changed
                LIMIT 100  -- Limit to prevent too many results
            """, (scenario_id,))
            
            for row in cursor.fetchall():
                results["area_inconsistencies"].append({
                    "fips_code": row[0],
                    "start_year": row[1],
                    "area_difference": row[2]
                })
            
            return results
        except Exception as e:
            logger.error(f"Error checking data integrity: {e}")
            raise
        finally:
            if conn and conn != DatabaseConnection._connection:
                conn.close() 