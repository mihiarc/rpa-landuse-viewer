"""
Advanced database queries for the RPA Land Use Viewer.
These queries provide additional analytical capabilities beyond basic data retrieval.
"""

import streamlit as st
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
from src.db.database import DatabaseConnection

class AdvancedQueries:
    """Implementation of advanced land use change analysis queries."""
    
    @st.cache_data
    def peak_change_period(
        land_use_type: str,
        scenario_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Identify the time period with largest net change for a land use type.
        
        Args:
            land_use_type: Type of land use to analyze
            scenario_id: Optional scenario ID to filter by
            
        Returns:
            Dictionary containing period and change amount
        """
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        
        query = """
            WITH period_changes AS (
                SELECT 
                    ts.start_year,
                    ts.end_year,
                    ts.time_step_id,
                    SUM(CASE WHEN t.to_land_use = ? THEN t.acres ELSE 0 END) as gained,
                    SUM(CASE WHEN t.from_land_use = ? THEN t.acres ELSE 0 END) as lost
                FROM land_use_transitions t
                JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                WHERE 1=1
                {scenario_filter}
                GROUP BY ts.start_year, ts.end_year, ts.time_step_id
            )
            SELECT 
                time_step_id,
                start_year,
                end_year,
                gained - lost as net_change,
                (gained - lost) / (end_year - start_year) as annual_rate
            FROM period_changes
            ORDER BY ABS(gained - lost) DESC
            LIMIT 1
        """
        
        params = [land_use_type, land_use_type]
        
        if scenario_id:
            query = query.replace("{scenario_filter}", "AND t.scenario_id = ?")
            params.append(scenario_id)
        else:
            query = query.replace("{scenario_filter}", "")
        
        cursor.execute(query, params)
        result = cursor.fetchone()
        
        DatabaseConnection.close_connection(conn)
        
        if result:
            return {
                'time_step_id': result[0],
                'start_year': result[1],
                'end_year': result[2],
                'net_change': result[3],
                'annual_rate': result[4]
            }
        return None

    @st.cache_data
    def major_transitions(
        start_year: int,
        end_year: int,
        scenario_id: int,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Identify major land use transitions during the specified period.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            scenario_id: Scenario ID to analyze
            limit: Number of transitions to return
            
        Returns:
            List of dictionaries containing transition details
        """
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                t.from_land_use,
                t.to_land_use,
                SUM(t.acres) as acres_changed
            FROM land_use_transitions t
            JOIN time_steps ts ON t.time_step_id = ts.time_step_id
            WHERE t.scenario_id = ?
            AND ts.start_year >= ? AND ts.end_year <= ?
            GROUP BY t.from_land_use, t.to_land_use
            ORDER BY acres_changed DESC
            LIMIT ?
        """
        
        cursor.execute(query, [scenario_id, start_year, end_year, limit])
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'from_land_use': row[0],
                'to_land_use': row[1],
                'acres_changed': row[2]
            })
        
        DatabaseConnection.close_connection(conn)
        return results

    @st.cache_data
    def compare_scenarios(
        start_year: int,
        end_year: int,
        land_use_type: str,
        scenario1_id: int,
        scenario2_id: int
    ) -> Dict[str, Any]:
        """
        Compare two scenarios for a specific land use type.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            land_use_type: Type of land use to analyze
            scenario1_id: First scenario ID
            scenario2_id: Second scenario ID
            
        Returns:
            Dictionary with comparison results
        """
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        
        query = """
            WITH scenario_changes AS (
                SELECT 
                    s.scenario_id,
                    s.scenario_name,
                    SUM(CASE WHEN t.to_land_use = ? THEN t.acres ELSE 0 END) as gained,
                    SUM(CASE WHEN t.from_land_use = ? THEN t.acres ELSE 0 END) as lost
                FROM land_use_transitions t
                JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                JOIN scenarios s ON t.scenario_id = s.scenario_id
                WHERE ts.start_year >= ? AND ts.end_year <= ?
                AND s.scenario_id IN (?, ?)
                GROUP BY s.scenario_id, s.scenario_name
            )
            SELECT 
                scenario_id,
                scenario_name,
                gained - lost as net_change,
                (gained - lost) / (? - ?) as annual_rate
            FROM scenario_changes
            ORDER BY scenario_id
        """
        
        params = [
            land_use_type, land_use_type,
            start_year, end_year,
            scenario1_id, scenario2_id,
            end_year, start_year
        ]
        
        cursor.execute(query, params)
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'scenario_id': row[0],
                'scenario_name': row[1],
                'net_change': row[2],
                'annual_rate': row[3]
            })
        
        DatabaseConnection.close_connection(conn)
        
        # Calculate difference between scenarios
        if len(results) == 2:
            difference = {
                'net_change_diff': results[0]['net_change'] - results[1]['net_change'],
                'annual_rate_diff': results[0]['annual_rate'] - results[1]['annual_rate'],
                'percent_diff': ((results[0]['net_change'] - results[1]['net_change']) / 
                                abs(results[1]['net_change'])) * 100 if results[1]['net_change'] != 0 else 0
            }
            return {
                'scenarios': results,
                'difference': difference
            }
        
        return {'scenarios': results}

    @st.cache_data
    def top_counties_by_change(
        start_year: int,
        end_year: int,
        land_use_type: str,
        scenario_id: int,
        limit: int = 10,
        change_type: str = 'increase'  # 'increase' or 'decrease'
    ) -> List[Dict[str, Any]]:
        """
        Identify top N counties with largest change in specified land use.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            land_use_type: Type of land use to analyze
            scenario_id: Scenario ID to analyze
            limit: Number of counties to return
            change_type: Whether to look for increases or decreases
            
        Returns:
            List of dictionaries containing county info and change amount
        """
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        
        query = """
            WITH county_changes AS (
                SELECT 
                    t.county_fips,
                    c.county_name,
                    c.state_fips,
                    SUM(CASE WHEN t.to_land_use = ? THEN t.acres ELSE 0 END) as gained,
                    SUM(CASE WHEN t.from_land_use = ? THEN t.acres ELSE 0 END) as lost
                FROM land_use_transitions t
                JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                JOIN counties c ON t.county_fips = c.county_fips
                WHERE ts.start_year >= ? AND ts.end_year <= ?
                AND t.scenario_id = ?
                GROUP BY t.county_fips, c.county_name, c.state_fips
            )
            SELECT 
                county_fips,
                county_name,
                state_fips,
                gained - lost as net_change
            FROM county_changes
            ORDER BY 
                CASE WHEN ? = 'increase' THEN gained - lost
                     ELSE lost - gained END DESC
            LIMIT ?
        """
        
        params = [
            land_use_type, land_use_type,
            start_year, end_year, scenario_id,
            change_type, limit
        ]
        
        cursor.execute(query, params)
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'county_fips': row[0],
                'county_name': row[1],
                'state_fips': row[2],
                'net_change': row[3]
            })
        
        DatabaseConnection.close_connection(conn)
        return results 