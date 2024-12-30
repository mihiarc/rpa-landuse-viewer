from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
from .database import DatabaseConnection
from .models import LandUseTransition, ScenarioInfo

class LandUseQueries:
    """Implementation of common land use change analysis queries."""
    
    @staticmethod
    def total_net_change(
        start_year: int,
        end_year: int,
        scenario_name: Optional[str] = None
    ) -> List[Dict[str, float]]:
        """
        Query I.1: Total net change by land use type across all counties.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            scenario_name: Optional scenario to filter by
            
        Returns:
            List of dictionaries containing land use type and net change in acres
        """
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            
            query = """
                WITH gains AS (
                    SELECT 
                        t.to_land_use as land_use,
                        SUM(t.acres) as gained_acres
                    FROM land_use_transitions t
                    JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                    JOIN scenarios s ON t.scenario_id = s.scenario_id
                    WHERE ts.start_year >= %s AND ts.end_year <= %s
                    AND s.scenario_name = %s
                    GROUP BY t.to_land_use
                ),
                losses AS (
                    SELECT 
                        t.from_land_use as land_use,
                        SUM(t.acres) as lost_acres
                    FROM land_use_transitions t
                    JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                    JOIN scenarios s ON t.scenario_id = s.scenario_id
                    WHERE ts.start_year >= %s AND ts.end_year <= %s
                    AND s.scenario_name = %s
                    GROUP BY t.from_land_use
                ),
                all_land_uses AS (
                    SELECT land_use FROM gains
                    UNION
                    SELECT land_use FROM losses
                )
                SELECT 
                    a.land_use,
                    COALESCE(g.gained_acres, 0) - COALESCE(l.lost_acres, 0) as net_change
                FROM all_land_uses a
                LEFT JOIN gains g ON a.land_use = g.land_use
                LEFT JOIN losses l ON a.land_use = l.land_use
                ORDER BY net_change DESC
            """
            
            params = [start_year, end_year, scenario_name] * 2
            
            cursor.execute(query, params)
            return cursor.fetchall()

    @staticmethod
    def annualized_change_rate(
        period_start: int,
        period_end: int,
        scenario_name: Optional[str] = None
    ) -> List[Dict[str, float]]:
        """
        Query I.2: Average annual rate of change for each land use type.
        
        Args:
            period_start: Start year of the period
            period_end: End year of the period
            scenario_name: Optional scenario to filter by
            
        Returns:
            List of dictionaries containing land use type and annual change rate
        """
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            
            query = """
                WITH gains AS (
                    SELECT 
                        t.to_land_use as land_use,
                        SUM(t.acres) as gained_acres
                    FROM land_use_transitions t
                    JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                    JOIN scenarios s ON t.scenario_id = s.scenario_id
                    WHERE ts.start_year >= %s AND ts.end_year <= %s
                    AND s.scenario_name = %s
                    GROUP BY t.to_land_use
                ),
                losses AS (
                    SELECT 
                        t.from_land_use as land_use,
                        SUM(t.acres) as lost_acres
                    FROM land_use_transitions t
                    JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                    JOIN scenarios s ON t.scenario_id = s.scenario_id
                    WHERE ts.start_year >= %s AND ts.end_year <= %s
                    AND s.scenario_name = %s
                    GROUP BY t.from_land_use
                ),
                all_land_uses AS (
                    SELECT land_use FROM gains
                    UNION
                    SELECT land_use FROM losses
                )
                SELECT 
                    a.land_use,
                    (COALESCE(g.gained_acres, 0) - COALESCE(l.lost_acres, 0)) / (%s - %s) as annual_rate
                FROM all_land_uses a
                LEFT JOIN gains g ON a.land_use = g.land_use
                LEFT JOIN losses l ON a.land_use = l.land_use
                ORDER BY annual_rate DESC
            """
            
            params = [
                period_start, period_end, scenario_name,  # For gains
                period_start, period_end, scenario_name,  # For losses
                period_end, period_start  # For rate calculation
            ]
            
            cursor.execute(query, params)
            return cursor.fetchall()

    @staticmethod
    def peak_change_period(
        land_use_type: str,
        scenario_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Query I.3: Identify the 10-year period with largest net change.
        
        Args:
            land_use_type: Type of land use to analyze
            scenario_name: Optional scenario to filter by
            
        Returns:
            Dictionary containing period and change amount
        """
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            
            query = """
                WITH period_changes AS (
                    SELECT 
                        ts.start_year,
                        ts.end_year,
                        CONCAT(ts.start_year, '-', ts.end_year) as time_period,
                        SUM(CASE WHEN t.to_land_use = %s THEN t.acres ELSE 0 END) as gained,
                        SUM(CASE WHEN t.from_land_use = %s THEN t.acres ELSE 0 END) as lost
                    FROM land_use_transitions t
                    JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                    JOIN scenarios s ON t.scenario_id = s.scenario_id
                    WHERE s.scenario_name = %s
                    GROUP BY ts.start_year, ts.end_year, time_period
                )
                SELECT 
                    time_period,
                    start_year,
                    end_year,
                    gained - lost as net_change,
                    (gained - lost) / (end_year - start_year) as annual_rate
                FROM period_changes
                ORDER BY ABS(gained - lost) DESC
                LIMIT 1
            """
            
            params = [land_use_type, land_use_type, scenario_name]
            cursor.execute(query, params)
            return cursor.fetchone()

    @staticmethod
    def top_counties_by_change(
        start_year: int,
        end_year: int,
        land_use_type: str,
        scenario_name: str,
        limit: int = 10,
        change_type: str = 'increase'  # 'increase' or 'decrease'
    ) -> List[Dict[str, Any]]:
        """
        Query II.5: Identify top N counties with largest change in specified land use.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            land_use_type: Type of land use to analyze
            scenario_name: Scenario to analyze
            limit: Number of counties to return
            change_type: Whether to look for increases or decreases
            
        Returns:
            List of dictionaries containing county info and change amount
        """
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            
            query = """
                WITH county_changes AS (
                    SELECT 
                        t.fips_code,
                        c.county_name,
                        SUBSTRING(t.fips_code, 1, 2) as state_fips,
                        SUM(CASE WHEN t.to_land_use = %s THEN t.acres ELSE 0 END) as gained,
                        SUM(CASE WHEN t.from_land_use = %s THEN t.acres ELSE 0 END) as lost
                    FROM land_use_transitions t
                    JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                    JOIN scenarios s ON t.scenario_id = s.scenario_id
                    JOIN counties c ON t.fips_code = c.fips_code
                    WHERE ts.start_year >= %s AND ts.end_year <= %s
                    AND s.scenario_name = %s
                    GROUP BY t.fips_code, c.county_name, state_fips
                )
                SELECT 
                    fips_code,
                    county_name,
                    state_fips,
                    gained - lost as net_change
                FROM county_changes
                ORDER BY 
                    CASE WHEN %s = 'increase' THEN gained - lost
                         ELSE lost - gained END DESC
                LIMIT %s
            """
            
            params = [
                land_use_type, land_use_type,
                start_year, end_year, scenario_name,
                change_type, limit
            ]
            
            cursor.execute(query, params)
            return cursor.fetchall()

    # Geographic Analysis Queries (Section II)
    @staticmethod
    def change_by_state(
        start_year: int,
        end_year: int,
        land_use_type: Optional[str] = None,
        scenario_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Query II.4: Net change by state.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            land_use_type: Optional land use type to filter by
            scenario_name: Optional scenario to filter by
            
        Returns:
            List of dictionaries containing state and net change
        """
        # Implementation coming soon
        pass

    @staticmethod
    def get_region_for_state(state_fips: str) -> str:
        """Helper method to map state FIPS codes to Census regions with modified Western regions."""
        # Regions:
        # Northeast: New England (CT,ME,MA,NH,RI,VT) and Middle Atlantic (NJ,NY,PA)
        # Midwest: East North Central (IL,IN,MI,OH,WI) and West North Central (IA,KS,MN,MO,NE,ND,SD)
        # South: South Atlantic (DE,FL,GA,MD,NC,SC,VA,WV,DC), East South Central (AL,KY,MS,TN), 
        #        West South Central (AR,LA,OK,TX)
        # Pacific Coast: CA, OR, WA
        # Rocky Mountain & Plains: Mountain (AZ,CO,ID,MT,NV,NM,UT,WY) plus AK, HI, and Plains states
        
        regions = {
            # Northeast
            "09": "Northeast", "23": "Northeast", "25": "Northeast", "33": "Northeast", 
            "44": "Northeast", "50": "Northeast", "34": "Northeast", "36": "Northeast", 
            "42": "Northeast",
            
            # Midwest
            "17": "Midwest", "18": "Midwest", "26": "Midwest", "39": "Midwest", 
            "55": "Midwest", "19": "Midwest", "20": "Midwest", "27": "Midwest", 
            "29": "Midwest", "31": "Midwest", "38": "Midwest", "46": "Midwest",
            
            # South
            "10": "South", "12": "South", "13": "South", "24": "South", "37": "South", 
            "45": "South", "51": "South", "54": "South", "11": "South", "01": "South", 
            "21": "South", "28": "South", "47": "South", "05": "South", "22": "South", 
            "40": "South", "48": "South",
            
            # Pacific Coast
            "06": "Pacific Coast",  # CA
            "41": "Pacific Coast",  # OR
            "53": "Pacific Coast",  # WA
            
            # Rocky Mountain & Plains
            "02": "Rocky Mountain & Plains",  # AK
            "04": "Rocky Mountain & Plains",  # AZ
            "08": "Rocky Mountain & Plains",  # CO
            "15": "Rocky Mountain & Plains",  # HI
            "16": "Rocky Mountain & Plains",  # ID
            "30": "Rocky Mountain & Plains",  # MT
            "32": "Rocky Mountain & Plains",  # NV
            "35": "Rocky Mountain & Plains",  # NM
            "49": "Rocky Mountain & Plains",  # UT
            "56": "Rocky Mountain & Plains",  # WY
            "20": "Rocky Mountain & Plains",  # KS
            "31": "Rocky Mountain & Plains",  # NE
            "38": "Rocky Mountain & Plains",  # ND
            "46": "Rocky Mountain & Plains"   # SD
        }
        return regions.get(state_fips, "Other")

    @staticmethod
    def change_by_region(
        start_year: int,
        end_year: int,
        scenario_name: str,
        land_use_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Query for net change by Census region.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            scenario_name: Scenario to analyze
            land_use_type: Optional land use type to filter by
            
        Returns:
            List of dictionaries containing region and net change
        """
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            
            # Base query with region calculation
            query = """
                WITH gains AS (
                    SELECT 
                        SUBSTRING(t.fips_code, 1, 2) as state_fips,
                        SUM(t.acres) as gained_acres
                    FROM land_use_transitions t
                    JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                    JOIN scenarios s ON t.scenario_id = s.scenario_id
                    WHERE ts.start_year >= %s AND ts.end_year <= %s
                    AND s.scenario_name = %s
                    AND t.to_land_use = %s
                    GROUP BY state_fips
                ),
                losses AS (
                    SELECT 
                        SUBSTRING(t.fips_code, 1, 2) as state_fips,
                        SUM(t.acres) as lost_acres
                    FROM land_use_transitions t
                    JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                    JOIN scenarios s ON t.scenario_id = s.scenario_id
                    WHERE ts.start_year >= %s AND ts.end_year <= %s
                    AND s.scenario_name = %s
                    AND t.from_land_use = %s
                    GROUP BY state_fips
                ),
                all_states AS (
                    SELECT state_fips FROM gains
                    UNION
                    SELECT state_fips FROM losses
                )
                SELECT 
                    a.state_fips,
                    COALESCE(g.gained_acres, 0) - COALESCE(l.lost_acres, 0) as net_change
                FROM all_states a
                LEFT JOIN gains g ON a.state_fips = g.state_fips
                LEFT JOIN losses l ON a.state_fips = l.state_fips
                ORDER BY a.state_fips
            """
            
            if not land_use_type:
                raise ValueError("Land use type is required for regional analysis")
            
            # Prepare parameters
            params = [
                start_year, end_year, scenario_name, land_use_type,  # For gains
                start_year, end_year, scenario_name, land_use_type   # For losses
            ]
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Group results by region
            region_changes = {}
            for row in results:
                region = LandUseQueries.get_region_for_state(row['state_fips'])
                if region not in region_changes:
                    region_changes[region] = {
                        'region': region,
                        'net_change': 0
                    }
                region_changes[region]['net_change'] += row['net_change']
            
            return sorted(
                region_changes.values(),
                key=lambda x: abs(x['net_change']),
                reverse=True
            )

    @staticmethod
    def analyze_county_transitions(
        county_fips: str,
        start_year: int,
        end_year: int,
        scenario_name: str,
        to_land_use: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Analyze land use transitions for a specific county.
        
        Args:
            county_fips: County FIPS code
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            scenario_name: Scenario to analyze
            to_land_use: Optional filter for destination land use type
            
        Returns:
            List of dictionaries containing transition details
        """
        with DatabaseConnection.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            
            query = """
                WITH transitions AS (
                    SELECT 
                        t.from_land_use,
                        t.to_land_use,
                        SUM(t.acres) as acres_changed
                    FROM land_use_transitions t
                    JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                    JOIN scenarios s ON t.scenario_id = s.scenario_id
                    WHERE t.fips_code = %s
                    AND ts.start_year >= %s AND ts.end_year <= %s
                    AND s.scenario_name = %s
                    AND t.from_land_use != t.to_land_use  -- Exclude transitions to same land use
                    {to_filter}
                    GROUP BY t.from_land_use, t.to_land_use
                ),
                source_totals AS (
                    SELECT 
                        from_land_use,
                        SUM(acres_changed) as total_source_loss
                    FROM transitions
                    GROUP BY from_land_use
                )
                SELECT 
                    t.from_land_use,
                    t.to_land_use,
                    t.acres_changed,
                    (t.acres_changed / st.total_source_loss) * 100 as percentage_of_source_loss
                FROM transitions t
                JOIN source_totals st ON t.from_land_use = st.from_land_use
                ORDER BY t.acres_changed DESC
            """
            
            # Add filter for destination land use if specified
            to_filter = "AND t.to_land_use = %s" if to_land_use else ""
            query = query.format(to_filter=to_filter)
            
            # Prepare parameters
            params = [county_fips, start_year, end_year, scenario_name]
            if to_land_use:
                params.append(to_land_use)
            
            cursor.execute(query, params)
            return cursor.fetchall()

    # More query methods will be added for each section... 