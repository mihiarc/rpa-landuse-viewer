"""
Functions for working with RPA Assessment regions in the land use database.
"""

import logging
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from .database import DatabaseConnection

logger = logging.getLogger(__name__)

class RPARegions:
    """Provides methods for working with RPA Assessment regions."""
    
    @staticmethod
    def get_all_regions():
        """Get a list of all RPA regions."""
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT region_id, region_name FROM rpa_regions ORDER BY region_name")
            regions = [{'id': row[0], 'name': row[1]} for row in cursor.fetchall()]
            return regions
        except Exception as e:
            logger.error(f"Error getting RPA regions: {e}")
            return []
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)
    
    @staticmethod
    def get_subregions(region_id=None):
        """
        Get a list of RPA subregions, optionally filtered by parent region.
        
        Args:
            region_id: Optional parent region ID to filter by
            
        Returns:
            List of dictionaries with subregion information
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT s.subregion_id, s.subregion_name, s.parent_region_id, r.region_name 
                FROM rpa_subregions s
                JOIN rpa_regions r ON s.parent_region_id = r.region_id
            """
            
            params = []
            if region_id:
                query += " WHERE s.parent_region_id = ?"
                params.append(region_id)
            
            query += " ORDER BY r.region_name, s.subregion_name"
            
            cursor.execute(query, params)
            subregions = [{
                'id': row[0], 
                'name': row[1], 
                'parent_id': row[2],
                'parent_name': row[3]
            } for row in cursor.fetchall()]
            
            return subregions
        except Exception as e:
            logger.error(f"Error getting RPA subregions: {e}")
            return []
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)
    
    @staticmethod
    def get_states_by_region(region_id=None, subregion_id=None):
        """
        Get states belonging to a specific RPA region or subregion.
        
        Args:
            region_id: Optional RPA region ID to filter by
            subregion_id: Optional RPA subregion ID to filter by
            
        Returns:
            List of dictionaries with state information
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT s.state_fips, s.state_name, s.state_abbr, 
                       rs.subregion_id, rs.subregion_name, 
                       r.region_id, r.region_name
                FROM states s
                JOIN rpa_state_mapping rsm ON s.state_fips = rsm.state_fips
                JOIN rpa_subregions rs ON rsm.subregion_id = rs.subregion_id
                JOIN rpa_regions r ON rs.parent_region_id = r.region_id
                WHERE 1=1
            """
            
            params = []
            if region_id:
                query += " AND r.region_id = ?"
                params.append(region_id)
            
            if subregion_id:
                query += " AND rs.subregion_id = ?"
                params.append(subregion_id)
            
            query += " ORDER BY r.region_name, rs.subregion_name, s.state_name"
            
            cursor.execute(query, params)
            states = [{
                'fips': row[0],
                'name': row[1],
                'abbr': row[2],
                'subregion_id': row[3],
                'subregion_name': row[4],
                'region_id': row[5],
                'region_name': row[6]
            } for row in cursor.fetchall()]
            
            return states
        except Exception as e:
            logger.error(f"Error getting states by RPA region: {e}")
            return []
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)
    
    @staticmethod
    def get_counties_by_region(region_id=None, subregion_id=None, state_fips=None):
        """
        Get counties belonging to a specific RPA region, subregion, or state.
        
        Args:
            region_id: Optional RPA region ID to filter by
            subregion_id: Optional RPA subregion ID to filter by
            state_fips: Optional state FIPS code to filter by
            
        Returns:
            List of dictionaries with county information
        """
        conn = None
        try:
            conn = DatabaseConnection.get_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT c.fips_code, c.county_name, s.state_fips, s.state_name, s.state_abbr,
                       rs.subregion_id, rs.subregion_name, 
                       r.region_id, r.region_name
                FROM counties c
                JOIN states s ON SUBSTR(c.fips_code, 1, 2) = s.state_fips
                JOIN rpa_state_mapping rsm ON s.state_fips = rsm.state_fips
                JOIN rpa_subregions rs ON rsm.subregion_id = rs.subregion_id
                JOIN rpa_regions r ON rs.parent_region_id = r.region_id
                WHERE 1=1
            """
            
            params = []
            if region_id:
                query += " AND r.region_id = ?"
                params.append(region_id)
            
            if subregion_id:
                query += " AND rs.subregion_id = ?"
                params.append(subregion_id)
            
            if state_fips:
                query += " AND s.state_fips = ?"
                params.append(state_fips)
            
            query += " ORDER BY r.region_name, rs.subregion_name, s.state_name, c.county_name"
            
            cursor.execute(query, params)
            counties = [{
                'fips': row[0],
                'name': row[1],
                'state_fips': row[2],
                'state_name': row[3],
                'state_abbr': row[4],
                'subregion_id': row[5],
                'subregion_name': row[6],
                'region_id': row[7],
                'region_name': row[8]
            } for row in cursor.fetchall()]
            
            return counties
        except Exception as e:
            logger.error(f"Error getting counties by RPA region: {e}")
            return []
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)
    
    @staticmethod
    def get_land_use_by_region(scenario_id, start_year=None, end_year=None, 
                               region_id=None, subregion_id=None, land_use_type=None):
        """
        Get land use data aggregated by RPA region.
        
        Args:
            scenario_id: Scenario ID to filter by
            start_year: Optional start year to filter by
            end_year: Optional end year to filter by
            region_id: Optional RPA region ID to filter by
            subregion_id: Optional RPA subregion ID to filter by
            land_use_type: Optional land use type to filter by
            
        Returns:
            DataFrame with land use data by region
        """
        conn = None
        try:
            # Keep the connection for queries that don't return DataFrames
            conn = DatabaseConnection.get_connection()
            
            filters = []
            params = []
            
            if region_id:
                filters.append("rpa_region_id = ?")
                params.append(region_id)
            
            if subregion_id:
                filters.append("subregion_id = ?")
                params.append(subregion_id)
            
            if start_year:
                filters.append("start_year >= ?")
                params.append(start_year)
            
            if end_year:
                filters.append("end_year <= ?")
                params.append(end_year)
            
            if land_use_type:
                filters.append("(from_land_use = ? OR to_land_use = ?)")
                params.extend([land_use_type, land_use_type])
            
            # Start with base query
            query = "SELECT * FROM rpa_region_land_use WHERE scenario_id = ?"
            params.insert(0, scenario_id)
            
            # Add any additional filters
            if filters:
                query += " AND " + " AND ".join(filters)
            
            # Add ordering
            query += " ORDER BY start_year, rpa_region_name, subregion_name, from_land_use, to_land_use"
            
            # Use the utility method to execute the query with pandas
            return DatabaseConnection.execute_pandas_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting land use by RPA region: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                DatabaseConnection.close_connection(conn)
    
    @staticmethod
    def get_land_use_by_subregion(scenario_id, start_year=None, end_year=None, 
                                 subregion_id=None, land_use_type=None):
        """
        Get land use data aggregated by RPA subregion.
        
        Args:
            scenario_id: Scenario ID to filter by
            start_year: Optional start year to filter by
            end_year: Optional end year to filter by
            subregion_id: Optional RPA subregion ID to filter by
            land_use_type: Optional land use type to filter by
            
        Returns:
            DataFrame with land use data by subregion
        """
        conn = None
        try:
            # Keep the connection for queries that don't return DataFrames
            conn = DatabaseConnection.get_connection()
            
            filters = []
            params = []
            
            if subregion_id:
                filters.append("subregion_id = ?")
                params.append(subregion_id)
            
            if start_year:
                filters.append("start_year >= ?")
                params.append(start_year)
            
            if end_year:
                filters.append("end_year <= ?")
                params.append(end_year)
            
            if land_use_type:
                filters.append("(from_land_use = ? OR to_land_use = ?)")
                params.extend([land_use_type, land_use_type])
            
            # Start with base query
            query = "SELECT * FROM rpa_subregion_land_use WHERE scenario_id = ?"
            params.insert(0, scenario_id)
            
            # Add any additional filters
            if filters:
                query += " AND " + " AND ".join(filters)
            
            # Add ordering
            query += " ORDER BY start_year, parent_region_name, subregion_name, from_land_use, to_land_use"
            
            # Use the utility method to execute the query with pandas
            return DatabaseConnection.execute_pandas_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting land use by RPA subregion: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                DatabaseConnection.close_connection(conn) 