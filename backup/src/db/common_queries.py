"""
Implementation of common queries from the common_queries.md document.
These functions implement the core query functionality needed for the RPA Land Use application.
"""

import logging
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple, Union
from .database import DatabaseConnection

logger = logging.getLogger(__name__)

class CommonQueries:
    """Implementation of common land use change analysis queries from documentation."""
    
    @staticmethod
    def total_net_change_by_land_use_type(
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        scenario_id: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Query 1: Total Net Change by Land Use Type
        
        Calculate the total net change (gain or loss in acres) for each land use type
        across all counties over the specified period.
        
        Args:
            start_year: Optional starting year for the analysis
            end_year: Optional ending year for the analysis
            scenario_id: Optional scenario ID to filter results
            
        Returns:
            DataFrame with land use types and their net changes
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            
            # Build the main query without filters first
            base_query = """
            WITH from_sums AS (
                SELECT 
                    from_land_use AS land_use_type,
                    -SUM(acres) AS net_change
                FROM land_use_transitions 
                {filters}
                GROUP BY from_land_use
            ),
            to_sums AS (
                SELECT 
                    to_land_use AS land_use_type,
                    SUM(acres) AS net_change
                FROM land_use_transitions 
                {filters}
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
            
            # Build the filters
            filter_parts = []
            params = []
            
            if scenario_id:
                filter_parts.append("scenario_id = ?")
                params.append(scenario_id)
            
            if start_year and end_year:
                filter_parts.append("time_step_id IN (SELECT time_step_id FROM time_steps WHERE start_year >= ? AND end_year <= ?)")
                params.extend([start_year, end_year])
            
            # Create the WHERE clause if we have filters
            filter_sql = ""
            if filter_parts:
                filter_sql = "WHERE " + " AND ".join(filter_parts)
            
            # Insert the filter into the query
            query = base_query.format(filters=filter_sql)
            
            # Use the same parameters for both parts of the query
            all_params = params + params if params else []
            
            # Execute and return results as DataFrame
            df = pd.read_sql_query(query, conn, params=all_params)
            return df
            
        except Exception as e:
            logger.error(f"Error in total_net_change_by_land_use_type: {e}")
            raise
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)

    @staticmethod
    def annualized_change_rate(
        scenario_id: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Query 2: Annualized Change Rate
        
        Calculate the average annual rate of change (in acres per year) for each land use type
        during each projection period.
        
        Args:
            scenario_id: Optional scenario ID to filter results
            
        Returns:
            DataFrame with time periods, land use types, and annual change rates
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            
            # Build the query with filter
            scenario_filter = ""
            scenario_params = []
            
            if scenario_id:
                scenario_filter = "WHERE lut.scenario_id = ?"
                scenario_params.append(scenario_id)
            
            query = f"""
            WITH period_changes AS (
                SELECT 
                    ts.start_year,
                    ts.end_year,
                    lut.from_land_use AS land_use_type,
                    -SUM(lut.acres) AS acres_lost
                FROM land_use_transitions lut
                JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
                {scenario_filter}
                GROUP BY ts.start_year, ts.end_year, lut.from_land_use
                
                UNION ALL
                
                SELECT 
                    ts.start_year,
                    ts.end_year,
                    lut.to_land_use AS land_use_type,
                    SUM(lut.acres) AS acres_gained
                FROM land_use_transitions lut
                JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
                {scenario_filter}
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
            
            # Duplicate the parameters for the second part of the UNION
            params = scenario_params + scenario_params if scenario_id else []
            
            # Execute and return results as DataFrame
            df = pd.read_sql_query(query, conn, params=params)
            return df
            
        except Exception as e:
            logger.error(f"Error in annualized_change_rate: {e}")
            raise
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)

    @staticmethod
    def peak_change_time_period(
        scenario_id: Optional[int] = None,
        land_use_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Query 3: Peak Change Time Period
        
        Find during which time period the largest net change was observed for each land use type.
        
        Args:
            scenario_id: Optional scenario ID to filter results
            land_use_type: Optional land use type to filter results
            
        Returns:
            DataFrame with land use types and their peak change periods
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            
            # Build the query with filters
            filters = []
            params = []
            
            if scenario_id:
                filters.append("lut.scenario_id = ?")
                params.append(scenario_id)
            
            if land_use_type:
                filters.append("(lut.from_land_use = ? OR lut.to_land_use = ?)")
                params.extend([land_use_type, land_use_type])
            
            where_clause = "WHERE " + " AND ".join(filters) if filters else ""
            
            # Duplicate parameters for the second part of the UNION
            union_params = params.copy() if params else []
            
            query = f"""
            WITH period_changes AS (
                SELECT 
                    ts.start_year,
                    ts.end_year,
                    lut.from_land_use AS land_use_type,
                    -SUM(lut.acres) AS acres_lost
                FROM land_use_transitions lut
                JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
                {where_clause}
                GROUP BY ts.start_year, ts.end_year, lut.from_land_use
                
                UNION ALL
                
                SELECT 
                    ts.start_year,
                    ts.end_year,
                    lut.to_land_use AS land_use_type,
                    SUM(lut.acres) AS acres_gained
                FROM land_use_transitions lut
                JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
                {where_clause}
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
            
            # Combine parameters for both parts of the UNION
            all_params = params + union_params
            
            # Execute and return results as DataFrame
            df = pd.read_sql_query(query, conn, params=all_params)
            return df
            
        except Exception as e:
            logger.error(f"Error in peak_change_time_period: {e}")
            raise
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)

    @staticmethod
    def change_by_state(
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        scenario_id: Optional[int] = None,
        land_use_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Query 4: Change by State
        
        Calculate the net change for each land use type within each state.
        
        Args:
            start_year: Optional starting year for the analysis
            end_year: Optional ending year for the analysis
            scenario_id: Optional scenario ID to filter results
            land_use_type: Optional land use type to filter results
            
        Returns:
            DataFrame with states, land use types, and their net changes
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            
            # Build the query with filters
            filters = []
            params = []
            
            if scenario_id:
                filters.append("lut.scenario_id = ?")
                params.append(scenario_id)
            
            if start_year and end_year:
                filters.append("ts.start_year >= ? AND ts.end_year <= ?")
                params.extend([start_year, end_year])
            
            if land_use_type:
                filters.append("(lut.from_land_use = ? OR lut.to_land_use = ?)")
                params.extend([land_use_type, land_use_type])
            
            where_clause = "WHERE " + " AND ".join(filters) if filters else ""
            
            # Duplicate parameters for the second part of the UNION
            union_params = params.copy() if params else []
            
            query = f"""
            WITH state_from_changes AS (
                SELECT 
                    SUBSTR(lut.fips_code, 1, 2) AS state_fips,
                    s.state_name,
                    lut.from_land_use AS land_use_type,
                    -SUM(lut.acres) AS acres_lost
                FROM land_use_transitions lut
                JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
                JOIN counties_by_state c ON lut.fips_code = c.county_fips
                JOIN states s ON c.state_fips = s.state_fips
                {where_clause}
                GROUP BY SUBSTR(lut.fips_code, 1, 2), s.state_name, lut.from_land_use
            ),
            state_to_changes AS (
                SELECT 
                    SUBSTR(lut.fips_code, 1, 2) AS state_fips,
                    s.state_name,
                    lut.to_land_use AS land_use_type,
                    SUM(lut.acres) AS acres_gained
                FROM land_use_transitions lut
                JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
                JOIN counties_by_state c ON lut.fips_code = c.county_fips
                JOIN states s ON c.state_fips = s.state_fips
                {where_clause}
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
            
            # Combine parameters for both parts of the UNION
            all_params = params + union_params
            
            # Execute and return results as DataFrame
            df = pd.read_sql_query(query, conn, params=all_params)
            return df
            
        except Exception as e:
            logger.error(f"Error in change_by_state: {e}")
            raise
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)

    @staticmethod
    def top_counties_by_change(
        land_use_type: str,
        limit: int = 10,
        direction: str = 'increase',
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        scenario_id: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Query 5: Top N Counties by Change
        
        Find the top N counties with the largest increase or decrease in a specific land use type.
        
        Args:
            land_use_type: Land use type to analyze
            limit: Number of counties to return
            direction: 'increase' or 'decrease'
            start_year: Optional starting year for the analysis
            end_year: Optional ending year for the analysis
            scenario_id: Optional scenario ID to filter results
            
        Returns:
            DataFrame with top counties and their changes
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            
            # Build the query with filters
            filters = []
            params = []
            
            if scenario_id:
                filters.append("lut.scenario_id = ?")
                params.append(scenario_id)
            
            if start_year and end_year:
                filters.append("ts.start_year >= ? AND ts.end_year <= ?")
                params.extend([start_year, end_year])
            
            # Determine sort direction
            sort_direction = "DESC" if direction == 'increase' else "ASC"
            
            # Add base params for the first part of the query
            base_params = [land_use_type] + params.copy() + [land_use_type]
            
            # Add base params for the second part of the query
            base_params2 = [land_use_type] + params.copy() + [land_use_type]
            
            # Combine all params
            all_params = base_params + base_params2 + [limit]
            
            where_clause = " AND ".join(filters)
            if where_clause:
                where_clause = "WHERE " + where_clause
            
            query = f"""
            WITH county_from_changes AS (
                SELECT 
                    lut.fips_code,
                    c.county_name,
                    st.state_name,
                    ? AS land_use_type,
                    -SUM(lut.acres) AS acres_lost
                FROM land_use_transitions lut
                JOIN counties c ON lut.fips_code = c.fips_code
                JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
                JOIN counties_by_state cbs ON c.fips_code = cbs.county_fips
                JOIN states st ON cbs.state_fips = st.state_fips
                {where_clause}
                {" AND " if where_clause else "WHERE "}lut.from_land_use = ?
                GROUP BY lut.fips_code, c.county_name, st.state_name
            ),
            county_to_changes AS (
                SELECT 
                    lut.fips_code,
                    c.county_name,
                    st.state_name,
                    ? AS land_use_type,
                    SUM(lut.acres) AS acres_gained
                FROM land_use_transitions lut
                JOIN counties c ON lut.fips_code = c.fips_code
                JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
                JOIN counties_by_state cbs ON c.fips_code = cbs.county_fips
                JOIN states st ON cbs.state_fips = st.state_fips
                {where_clause}
                {" AND " if where_clause else "WHERE "}lut.to_land_use = ?
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
                ORDER BY net_change {sort_direction}
                LIMIT ?
            )
            SELECT * FROM county_ranked
            """
            
            # Execute and return results as DataFrame
            df = pd.read_sql_query(query, conn, params=all_params)
            return df
            
        except Exception as e:
            logger.error(f"Error in top_counties_by_change: {e}")
            raise
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)

    @staticmethod
    def major_transitions(
        start_year: int,
        end_year: int,
        scenario_id: Optional[int] = None,
        limit: int = 10
    ) -> pd.DataFrame:
        """
        Query 11: Major Transitions
        
        Find the most common land use transitions observed across the region.
        
        Args:
            start_year: Starting year for the analysis
            end_year: Ending year for the analysis
            scenario_id: Optional scenario ID to filter results
            limit: Number of transitions to return
            
        Returns:
            DataFrame with top transitions and their total acres
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            
            # Build the query with filters
            filters = ["ts.start_year >= ? AND ts.end_year <= ?"]
            params = [start_year, end_year]
            
            if scenario_id:
                filters.append("lut.scenario_id = ?")
                params.append(scenario_id)
            
            where_clause = "WHERE " + " AND ".join(filters)
            
            # Add limit parameter
            params.append(limit)
            
            query = f"""
            SELECT
                lut.from_land_use,
                lut.to_land_use,
                SUM(lut.acres) AS total_acres_changed
            FROM land_use_transitions lut
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            {where_clause}
            GROUP BY lut.from_land_use, lut.to_land_use
            ORDER BY total_acres_changed DESC
            LIMIT ?
            """
            
            # Execute and return results as DataFrame
            df = pd.read_sql_query(query, conn, params=params)
            return df
            
        except Exception as e:
            logger.error(f"Error in major_transitions: {e}")
            raise
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)

    @staticmethod
    def check_data_integrity() -> Dict[str, pd.DataFrame]:
        """
        Queries 14-16: Data Integrity Checks
        
        Perform various data integrity checks:
        1. Total area consistency
        2. No negative acres
        3. Unique identifiers
        
        Returns:
            Dictionary with check names and their results as DataFrames
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            results = {}
            
            # Check 1: Total area consistency
            query1 = """
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
            WHERE difference > 0.1
            LIMIT 100
            """
            results['area_consistency'] = pd.read_sql_query(query1, conn)
            
            # Check 2: No negative acres
            query2 = """
            SELECT 
                scenario_id,
                time_step_id,
                fips_code,
                from_land_use,
                to_land_use,
                acres
            FROM land_use_transitions
            WHERE acres < 0
            LIMIT 100
            """
            results['negative_acres'] = pd.read_sql_query(query2, conn)
            
            # Check 3a: Unique scenario names
            query3a = """
            SELECT scenario_name, COUNT(*) as count
            FROM scenarios 
            GROUP BY scenario_name 
            HAVING COUNT(*) > 1
            """
            results['duplicate_scenarios'] = pd.read_sql_query(query3a, conn)
            
            # Check 3b: Unique FIPS codes
            query3b = """
            SELECT fips_code, COUNT(*) as count
            FROM counties 
            GROUP BY fips_code 
            HAVING COUNT(*) > 1
            """
            results['duplicate_fips'] = pd.read_sql_query(query3b, conn)
            
            # Check 3c: Unique time steps
            query3c = """
            SELECT start_year, end_year, COUNT(*) as count
            FROM time_steps 
            GROUP BY start_year, end_year 
            HAVING COUNT(*) > 1
            """
            results['duplicate_time_steps'] = pd.read_sql_query(query3c, conn)
            
            return results
            
        except Exception as e:
            logger.error(f"Error in check_data_integrity: {e}")
            raise
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)

    @staticmethod
    def compare_climate_models(
        year: int,
        land_use_type: str,
        climate_model1: str,
        climate_model2: str,
        ssp: str = None,
        rcp: str = None
    ) -> pd.DataFrame:
        """
        Query 8: Impact of Climate Models
        
        Compare projections between different climate models.
        
        Args:
            year: Target year for comparison
            land_use_type: Land use type to analyze
            climate_model1: First climate model (GCM)
            climate_model2: Second climate model (GCM)
            ssp: Optional SSP scenario to filter by
            rcp: Optional RCP scenario to filter by
            
        Returns:
            DataFrame comparing the two climate models
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            
            # Build the query with filters
            ssp_filter = ""
            rcp_filter = ""
            params = [year, year, land_use_type, climate_model1, climate_model2]
            
            if ssp:
                ssp_filter = "AND s.ssp = ?"
                params.append(ssp)
            
            if rcp:
                rcp_filter = "AND s.rcp = ?"
                params.append(rcp)
            
            query = f"""
            WITH net_changes AS (
                SELECT 
                    lut.scenario_id,
                    lut.fips_code,
                    lut.from_land_use,
                    -SUM(lut.acres) as net_change
                FROM land_use_transitions lut
                JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
                WHERE ts.end_year <= ?
                GROUP BY lut.scenario_id, lut.fips_code, lut.from_land_use
                
                UNION ALL
                
                SELECT 
                    lut.scenario_id,
                    lut.fips_code,
                    lut.to_land_use,
                    SUM(lut.acres) as net_change
                FROM land_use_transitions lut
                JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
                WHERE ts.end_year <= ?
                GROUP BY lut.scenario_id, lut.fips_code, lut.to_land_use
            ),
            model_summaries AS (
                SELECT
                    s.scenario_id,
                    s.scenario_name,
                    s.gcm,
                    s.rcp,
                    s.ssp,
                    nc.from_land_use AS land_use_type,
                    SUM(nc.net_change) AS total_change
                FROM net_changes nc
                JOIN scenarios s ON nc.scenario_id = s.scenario_id
                WHERE nc.from_land_use = ?
                AND s.gcm IN (?, ?)
                {ssp_filter}
                {rcp_filter}
                GROUP BY s.scenario_id, s.scenario_name, s.gcm, s.rcp, s.ssp, nc.from_land_use
            )
            SELECT 
                gcm,
                scenario_name,
                rcp,
                ssp,
                land_use_type,
                total_change,
                total_change / (SELECT SUM(total_change) FROM model_summaries) * 100 AS percentage
            FROM model_summaries
            ORDER BY gcm
            """
            
            # Execute and return results as DataFrame
            df = pd.read_sql_query(query, conn, params=params)
            return df
            
        except Exception as e:
            logger.error(f"Error in compare_climate_models: {e}")
            raise
        finally:
            if conn:
                DatabaseConnection.close_connection(conn) 