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
    def _find_matching_time_periods(cursor, start_year, end_year):
        """
        Helper method to find time periods that match or overlap with the requested range.
        If no overlapping periods are found, falls back to closest period.
        
        Args:
            cursor: Database cursor
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            
        Returns:
            List of time_step_ids that match or overlap with the requested range
        """
        # Try overlapping periods first
        cursor.execute(
            """
            SELECT time_step_id 
            FROM time_steps 
            WHERE NOT (end_year <= ? OR start_year >= ?)
            ORDER BY start_year
            """, 
            (start_year, end_year)
        )
        time_step_ids = [row[0] for row in cursor.fetchall()]
        
        if not time_step_ids:
            # Find closest time period as fallback
            cursor.execute(
                """
                SELECT time_step_id, start_year, end_year,
                       ABS(? - start_year) + ABS(? - end_year) as distance
                FROM time_steps
                ORDER BY distance
                LIMIT 1
                """, 
                (start_year, end_year)
            )
            closest_period = cursor.fetchone()
            if closest_period:
                time_step_ids = [closest_period[0]]
                logger.info(f"No overlapping time periods found between {start_year} and {end_year}. "
                           f"Using closest period: {closest_period[1]}-{closest_period[2]}")
            else:
                raise ValueError("No time periods found in the database")
        
        return time_step_ids
    
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
            
            # Get the time step IDs using our helper method
            time_step_ids = LandUseQueries._find_matching_time_periods(cursor, start_year, end_year)
            
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
            if conn:
                DatabaseConnection.close_connection(conn)
    
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
            
            # Get time step IDs using our helper method
            time_step_ids = LandUseQueries._find_matching_time_periods(cursor, start_year, end_year)
            
            # Calculate net change for each scenario
            results = []
            for scenario in scenarios:
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
            if conn:
                DatabaseConnection.close_connection(conn)
    
    @staticmethod
    def major_transitions(
        start_year: int,
        end_year: int,
        scenario_name: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get major land use transitions for a specific scenario.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            scenario_name: Scenario name
            limit: Max number of transitions to return
            
        Returns:
            List of dictionaries containing transition details
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
            
            # Get time step IDs using our helper method
            time_step_ids = LandUseQueries._find_matching_time_periods(cursor, start_year, end_year)
            
            # Get major transitions
            query = """
                SELECT 
                    from_land_use,
                    to_land_use,
                    SUM(acres) as acres_changed
                FROM 
                    land_use_transitions
                WHERE 
                    scenario_id = ? 
                    AND time_step_id IN ({})
                    AND from_land_use != to_land_use
                GROUP BY 
                    from_land_use, to_land_use
                ORDER BY 
                    acres_changed DESC
                LIMIT ?
            """.format(','.join(['?'] * len(time_step_ids)))
            
            params = [scenario_id] + time_step_ids + [limit]
            cursor.execute(query, params)
            
            # Process results
            results = []
            for row in cursor.fetchall():
                results.append({
                    'from_land_use': row[0],
                    'to_land_use': row[1],
                    'acres_changed': row[2]
                })
            
            return results
        except Exception as e:
            logger.error(f"Error getting major transitions: {e}")
            raise
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)
    
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
        Find counties with the highest change in a specific land use type.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            land_use_type: Land use type to analyze
            scenario_name: Scenario name
            limit: Maximum number of counties to return
            direction: 'increase' or 'decrease'
            
        Returns:
            List of dictionaries containing county details and change data
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
            
            # Get time step IDs using our helper method
            time_step_ids = LandUseQueries._find_matching_time_periods(cursor, start_year, end_year)
            
            # Calculate net change by county
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
                GROUP BY 
                    c.fips_code, c.county_name
                HAVING 
                    net_change {} 0
                ORDER BY 
                    {}
                LIMIT ?
            """.format(
                ','.join(['?'] * len(time_step_ids)),
                ">" if direction == 'increase' else "<",
                "net_change DESC" if direction == 'increase' else "net_change ASC"
            )
            
            params = [land_use_type, land_use_type, scenario_id] + time_step_ids + [limit]
            cursor.execute(query, params)
            
            # Process results
            results = []
            for row in cursor.fetchall():
                results.append({
                    'fips_code': row[0],
                    'county_name': row[1],
                    'net_change': abs(row[2]) if direction == 'decrease' else row[2]
                })
            
            return results
        except Exception as e:
            logger.error(f"Error finding top counties by change: {e}")
            raise
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)
    
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
            if conn:
                DatabaseConnection.close_connection(conn)
    
    @staticmethod
    def rank_scenarios_by_forest_loss(
        target_year: int,
        climate_model: str = None
    ) -> List[Dict[str, Any]]:
        """
        Rank scenarios by forest loss through the target year.
        
        Args:
            target_year: The year to analyze (will use 2012 to target_year)
            climate_model: Optional climate model to filter by
            
        Returns:
            List of dictionaries containing scenario rankings
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            cursor = conn.cursor()
            
            # Build the query base
            query = """
                SELECT 
                    s.scenario_name,
                    s.gcm,
                    s.rcp,
                    s.ssp,
                    SUM(CASE WHEN t.from_land_use = 'Forest' THEN t.acres ELSE 0 END) AS forest_loss,
                    SUM(CASE WHEN t.to_land_use = 'Forest' THEN t.acres ELSE 0 END) AS forest_gain,
                    SUM(CASE WHEN t.from_land_use = 'Forest' THEN t.acres ELSE 0 END) -
                    SUM(CASE WHEN t.to_land_use = 'Forest' THEN t.acres ELSE 0 END) AS net_forest_loss
                FROM 
                    land_use_transitions t
                JOIN 
                    scenarios s ON t.scenario_id = s.scenario_id
                JOIN 
                    time_steps ts ON t.time_step_id = ts.time_step_id
                WHERE 
                    ts.end_year <= ?
            """
            
            params = [target_year]
            
            # Add climate model filter if provided
            if climate_model:
                query += " AND s.gcm = ?"
                params.append(climate_model)
                
            # Complete the query
            query += """
                GROUP BY 
                    s.scenario_name, s.gcm, s.rcp, s.ssp
                ORDER BY 
                    net_forest_loss DESC
            """
            
            # Execute the query
            cursor.execute(query, params)
            
            # Process results with added fields for visualization
            results = []
            rank = 1
            for row in cursor.fetchall():
                results.append({
                    'scenario_name': row[0],
                    'gcm': row[1],
                    'rcp': row[2],
                    'ssp': row[3],
                    'forest_loss': row[4],
                    'forest_gain': row[5],
                    'net_forest_loss': row[6],
                    'emissions_forcing': row[2],  # Using RCP as emissions forcing
                    'socioeconomic_pathway': row[3],  # Using SSP as socioeconomic pathway
                    'loss_rank': rank
                })
                rank += 1
            
            return results
        except Exception as e:
            logger.error(f"Error ranking scenarios by forest loss: {e}")
            raise
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)
    
    @staticmethod
    def total_net_change(
        start_year: int,
        end_year: int,
        scenario_name: str
    ) -> List[Dict[str, Any]]:
        """
        Calculate total net change for all land use types.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            scenario_name: Scenario name
            
        Returns:
            List of dictionaries containing net change for each land use type
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
            
            # Get time step IDs using our helper method
            time_step_ids = LandUseQueries._find_matching_time_periods(cursor, start_year, end_year)
            
            # Get distinct land use types
            cursor.execute(
                """
                SELECT DISTINCT from_land_use AS land_use FROM land_use_transitions
                UNION
                SELECT DISTINCT to_land_use AS land_use FROM land_use_transitions
                """
            )
            land_use_types = [row[0] for row in cursor.fetchall()]
            
            # Calculate net change for each land use type
            results = []
            for land_use in land_use_types:
                query = """
                    SELECT 
                        ? AS land_use,
                        SUM(CASE WHEN to_land_use = ? THEN acres ELSE 0 END) - 
                        SUM(CASE WHEN from_land_use = ? THEN acres ELSE 0 END) AS net_change
                    FROM 
                        land_use_transitions
                    WHERE 
                        scenario_id = ? 
                        AND time_step_id IN ({})
                """.format(','.join(['?'] * len(time_step_ids)))
                
                params = [land_use, land_use, land_use, scenario_id] + time_step_ids
                cursor.execute(query, params)
                row = cursor.fetchone()
                
                if row and row[1] is not None:
                    results.append({
                        'land_use': row[0],
                        'net_change': row[1]
                    })
            
            # Sort results by absolute net change (descending)
            results.sort(key=lambda x: abs(x['net_change']), reverse=True)
            
            return results
        except Exception as e:
            logger.error(f"Error calculating total net change: {e}")
            raise
        finally:
            if conn:
                DatabaseConnection.close_connection(conn) 