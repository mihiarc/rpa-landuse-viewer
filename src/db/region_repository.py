"""
Region repository for accessing geographic data.

This repository provides methods to access and manipulate region data, 
including counties, states, and RPA regions.
"""

import logging
from typing import Dict, List, Any, Optional, Union
import pandas as pd
from .base_repository import BaseRepository

logger = logging.getLogger(__name__)

class RegionRepository(BaseRepository):
    """Repository for accessing region data."""
    
    @classmethod
    def get_counties(cls) -> pd.DataFrame:
        """
        Get all counties.
        
        Returns:
            DataFrame with county information
        """
        query = """
        SELECT 
            fips_code,
            county_name
        FROM 
            counties
        ORDER BY 
            county_name
        """
        return cls.query_to_df(query)
    
    @classmethod
    def get_states(cls) -> pd.DataFrame:
        """
        Get all states.
        
        Returns:
            DataFrame with state information
        """
        query = """
        SELECT 
            state_fips,
            state_name,
            state_abbr
        FROM 
            states
        ORDER BY 
            state_name
        """
        return cls.query_to_df(query)
    
    @classmethod
    def get_rpa_regions(cls) -> pd.DataFrame:
        """
        Get all RPA regions.
        
        Returns:
            DataFrame with RPA region information
        """
        query = """
        SELECT 
            region_id,
            region_name
        FROM 
            rpa_regions
        ORDER BY 
            region_name
        """
        return cls.query_to_df(query)
    
    @classmethod
    def get_rpa_subregions(cls) -> pd.DataFrame:
        """
        Get all RPA subregions.
        
        Returns:
            DataFrame with RPA subregion information
        """
        query = """
        SELECT 
            subregion_id,
            region_id,
            subregion_name
        FROM 
            rpa_subregions
        ORDER BY 
            subregion_name
        """
        return cls.query_to_df(query)
    
    @classmethod
    def get_counties_by_state(cls, state_fips: str) -> pd.DataFrame:
        """
        Get counties for a specific state.
        
        Args:
            state_fips: State FIPS code
            
        Returns:
            DataFrame with county information
        """
        query = """
        SELECT 
            c.fips_code,
            c.county_name,
            s.state_fips,
            s.state_name,
            s.state_abbr
        FROM 
            counties c
        JOIN 
            county_state_mapping csm ON c.fips_code = csm.county_fips
        JOIN
            states s ON csm.state_fips = s.state_fips
        WHERE 
            s.state_fips = ?
        ORDER BY 
            c.county_name
        """
        return cls.query_to_df(query, [state_fips])
    
    @classmethod
    def get_counties_by_region(cls, region_id: int) -> pd.DataFrame:
        """
        Get counties for a specific RPA region.
        
        Args:
            region_id: RPA region ID
            
        Returns:
            DataFrame with county information
        """
        query = """
        SELECT 
            c.fips_code,
            c.county_name,
            r.region_id,
            r.region_name
        FROM 
            counties c
        JOIN 
            county_region_mapping crm ON c.fips_code = crm.county_fips
        JOIN
            rpa_regions r ON crm.region_id = r.region_id
        WHERE 
            r.region_id = ?
        ORDER BY 
            c.county_name
        """
        return cls.query_to_df(query, [region_id])
    
    @classmethod
    def get_counties_by_subregion(cls, subregion_id: int) -> pd.DataFrame:
        """
        Get counties for a specific RPA subregion.
        
        Args:
            subregion_id: RPA subregion ID
            
        Returns:
            DataFrame with county information
        """
        query = """
        SELECT 
            c.fips_code,
            c.county_name,
            sr.subregion_id,
            sr.subregion_name,
            r.region_id,
            r.region_name
        FROM 
            counties c
        JOIN 
            county_subregion_mapping csrm ON c.fips_code = csrm.county_fips
        JOIN
            rpa_subregions sr ON csrm.subregion_id = sr.subregion_id
        JOIN
            rpa_regions r ON sr.region_id = r.region_id
        WHERE 
            sr.subregion_id = ?
        ORDER BY 
            c.county_name
        """
        return cls.query_to_df(query, [subregion_id])
    
    @classmethod
    def get_region_mapping(cls) -> pd.DataFrame:
        """
        Get a complete mapping of counties to states, regions, and subregions.
        
        Returns:
            DataFrame with complete region mapping
        """
        query = """
        SELECT 
            c.fips_code,
            c.county_name,
            s.state_fips,
            s.state_name,
            s.state_abbr,
            r.region_id,
            r.region_name,
            sr.subregion_id,
            sr.subregion_name
        FROM 
            counties c
        LEFT JOIN 
            county_state_mapping csm ON c.fips_code = csm.county_fips
        LEFT JOIN
            states s ON csm.state_fips = s.state_fips
        LEFT JOIN
            county_region_mapping crm ON c.fips_code = crm.county_fips
        LEFT JOIN
            rpa_regions r ON crm.region_id = r.region_id
        LEFT JOIN
            county_subregion_mapping csrm ON c.fips_code = csrm.county_fips
        LEFT JOIN
            rpa_subregions sr ON csrm.subregion_id = sr.subregion_id
        ORDER BY 
            s.state_name,
            c.county_name
        """
        return cls.query_to_df(query) 