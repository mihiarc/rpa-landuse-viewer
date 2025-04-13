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
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        
        query = """
            WITH gains AS (
                SELECT 
                    t.to_land_use as land_use,
                    SUM(t.acres) as gained_acres
                FROM land_use_transitions t
                JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                JOIN scenarios s ON t.scenario_id = s.scenario_id
                WHERE ts.start_year >= ? AND ts.end_year <= ?
                AND s.scenario_name = ?
                GROUP BY t.to_land_use
            ),
            losses AS (
                SELECT 
                    t.from_land_use as land_use,
                    SUM(t.acres) as lost_acres
                FROM land_use_transitions t
                JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                JOIN scenarios s ON t.scenario_id = s.scenario_id
                WHERE ts.start_year >= ? AND ts.end_year <= ?
                AND s.scenario_name = ?
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
        result = [dict(row) for row in cursor.fetchall()]
        return result

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
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        
        query = """
            WITH gains AS (
                SELECT 
                    t.to_land_use as land_use,
                    SUM(t.acres) as gained_acres
                FROM land_use_transitions t
                JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                JOIN scenarios s ON t.scenario_id = s.scenario_id
                WHERE ts.start_year >= ? AND ts.end_year <= ?
                AND s.scenario_name = ?
                GROUP BY t.to_land_use
            ),
            losses AS (
                SELECT 
                    t.from_land_use as land_use,
                    SUM(t.acres) as lost_acres
                FROM land_use_transitions t
                JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                JOIN scenarios s ON t.scenario_id = s.scenario_id
                WHERE ts.start_year >= ? AND ts.end_year <= ?
                AND s.scenario_name = ?
                GROUP BY t.from_land_use
            ),
            all_land_uses AS (
                SELECT land_use FROM gains
                UNION
                SELECT land_use FROM losses
            )
            SELECT 
                a.land_use,
                (COALESCE(g.gained_acres, 0) - COALESCE(l.lost_acres, 0)) / (? - ?) as annual_rate
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
        result = [dict(row) for row in cursor.fetchall()]
        return result

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
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        
        query = """
            WITH period_changes AS (
                SELECT 
                    ts.start_year,
                    ts.end_year,
                    ts.start_year || '-' || ts.end_year as time_period,
                    SUM(CASE WHEN t.to_land_use = ? THEN t.acres ELSE 0 END) as gained,
                    SUM(CASE WHEN t.from_land_use = ? THEN t.acres ELSE 0 END) as lost
                FROM land_use_transitions t
                JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                JOIN scenarios s ON t.scenario_id = s.scenario_id
                WHERE s.scenario_name = ?
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
        result = dict(cursor.fetchone()) if cursor.fetchone() else None
        return result

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
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        
        query = """
            WITH county_changes AS (
                SELECT 
                    t.fips_code,
                    c.county_name,
                    SUBSTR(t.fips_code, 1, 2) as state_fips,
                    SUM(CASE WHEN t.to_land_use = ? THEN t.acres ELSE 0 END) as gained,
                    SUM(CASE WHEN t.from_land_use = ? THEN t.acres ELSE 0 END) as lost
                FROM land_use_transitions t
                JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                JOIN scenarios s ON t.scenario_id = s.scenario_id
                JOIN counties c ON t.fips_code = c.fips_code
                WHERE ts.start_year >= ? AND ts.end_year <= ?
                AND s.scenario_name = ?
                GROUP BY t.fips_code, c.county_name, state_fips
            )
            SELECT 
                fips_code,
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
            start_year, end_year, scenario_name,
            change_type, limit
        ]
        
        cursor.execute(query, params)
        result = [dict(row) for row in cursor.fetchall()]
        return result

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
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        
        # SQLite version - using subquery with CASE for regions
        query = """
        WITH county_regions AS (
            SELECT 
                fips_code,
                CASE 
                    WHEN SUBSTR(fips_code, 1, 2) IN ('09','23','25','33','44','50','36','34','42') THEN 'Northeast'
                    WHEN SUBSTR(fips_code, 1, 2) IN ('17','18','26','39','55','19','20','27','29','31','38','46') THEN 'Midwest'
                    WHEN SUBSTR(fips_code, 1, 2) IN ('10','11','12','13','24','37','45','51','54','01','21','28','47','05','22','40','48') THEN 'South'
                    WHEN SUBSTR(fips_code, 1, 2) IN ('04','08','16','35','30','49','32','56','02','06','15','41','53') THEN 'West'
                    ELSE 'Other'
                END as region
            FROM counties
        ),
        gains AS (
            SELECT 
                cr.region,
                t.to_land_use as land_use,
                SUM(t.acres) as gained_acres
            FROM land_use_transitions t
            JOIN time_steps ts ON t.time_step_id = ts.time_step_id
            JOIN scenarios s ON t.scenario_id = s.scenario_id
            JOIN county_regions cr ON t.fips_code = cr.fips_code
            WHERE ts.start_year >= ? AND ts.end_year <= ?
            AND s.scenario_name = ?
            AND (? IS NULL OR t.to_land_use = ?)
            GROUP BY cr.region, t.to_land_use
        ),
        losses AS (
            SELECT 
                cr.region,
                t.from_land_use as land_use,
                SUM(t.acres) as lost_acres
            FROM land_use_transitions t
            JOIN time_steps ts ON t.time_step_id = ts.time_step_id
            JOIN scenarios s ON t.scenario_id = s.scenario_id
            JOIN county_regions cr ON t.fips_code = cr.fips_code
            WHERE ts.start_year >= ? AND ts.end_year <= ?
            AND s.scenario_name = ?
            AND (? IS NULL OR t.from_land_use = ?)
            GROUP BY cr.region, t.from_land_use
        ),
        all_land_region AS (
            SELECT region, land_use FROM gains
            UNION
            SELECT region, land_use FROM losses
        )
        SELECT 
            alr.region,
            alr.land_use,
            COALESCE(g.gained_acres, 0) as gains,
            COALESCE(l.lost_acres, 0) as losses,
            COALESCE(g.gained_acres, 0) - COALESCE(l.lost_acres, 0) as net_change
        FROM all_land_region alr
        LEFT JOIN gains g ON alr.region = g.region AND alr.land_use = g.land_use
        LEFT JOIN losses l ON alr.region = l.region AND alr.land_use = l.land_use
        ORDER BY alr.region, net_change DESC
        """
        
        params = [
            start_year, end_year, scenario_name, 
            land_use_type, land_use_type,
            start_year, end_year, scenario_name,
            land_use_type, land_use_type
        ]
        
        cursor.execute(query, params)
        result = [dict(row) for row in cursor.fetchall()]
        return result

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

    @staticmethod
    def compare_scenarios(
        start_year: int,
        end_year: int,
        land_use_type: str,
        scenario1: str,
        scenario2: str
    ) -> Dict[str, Any]:
        """
        Query III.8/9: Compare land use changes between two scenarios.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            land_use_type: Type of land use to analyze
            scenario1: First scenario name
            scenario2: Second scenario name
            
        Returns:
            Dictionary containing comparison results
        """
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        
        query = """
            WITH scenario_changes AS (
                SELECT 
                    s.scenario_name,
                    s.gcm,
                    s.rcp,
                    s.ssp,
                    SUM(CASE WHEN t.to_land_use = ? THEN t.acres ELSE 0 END) as gained,
                    SUM(CASE WHEN t.from_land_use = ? THEN t.acres ELSE 0 END) as lost
                FROM land_use_transitions t
                JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                JOIN scenarios s ON t.scenario_id = s.scenario_id
                WHERE ts.start_year >= ? AND ts.end_year <= ?
                AND s.scenario_name IN (?, ?)
                GROUP BY s.scenario_name, s.gcm, s.rcp, s.ssp
            )
            SELECT 
                scenario_name,
                gcm,
                rcp,
                ssp,
                gained - lost as net_change,
                (gained - lost) / (? - ?) as annual_rate
            FROM scenario_changes
            ORDER BY scenario_name
        """
        
        params = [
            land_use_type, land_use_type,
            start_year, end_year,
            scenario1, scenario2,
            end_year, start_year
        ]
        
        cursor.execute(query, params)
        result = [dict(row) for row in cursor.fetchall()]
        return result

    @staticmethod
    def major_transitions(
        start_year: int,
        end_year: int,
        scenario_name: str,
        limit: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Query IV.11: Identify major land use transitions.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            scenario_name: Scenario to analyze
            limit: Number of top transitions to return
            
        Returns:
            List of dictionaries containing transition details
        """
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        
        query = """
            WITH transitions AS (
                SELECT 
                    t.from_land_use,
                    t.to_land_use,
                    SUM(t.acres) as acres_changed
                FROM land_use_transitions t
                JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                JOIN scenarios s ON t.scenario_id = s.scenario_id
                WHERE ts.start_year >= ? AND ts.end_year <= ?
                AND s.scenario_name = ?
                AND t.from_land_use != t.to_land_use
                GROUP BY t.from_land_use, t.to_land_use
            )
            SELECT 
                from_land_use,
                to_land_use,
                acres_changed,
                acres_changed / (
                    SELECT SUM(acres_changed) FROM transitions
                ) * 100 as percentage_of_all_changes
            FROM transitions
            ORDER BY acres_changed DESC
            LIMIT ?
        """
        
        params = [start_year, end_year, scenario_name, limit]
        cursor.execute(query, params)
        result = [dict(row) for row in cursor.fetchall()]
        return result

    @staticmethod
    def check_data_integrity(
        scenario_name: str
    ) -> Dict[str, Any]:
        """
        Query V.14/15/16: Perform data integrity checks.
        
        Args:
            scenario_name: Scenario to check
            
        Returns:
            Dictionary containing check results
        """
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        
        # Check 1: Total area consistency
        area_check_query = """
            WITH time_step_totals AS (
                SELECT 
                    t.fips_code,
                    ts.time_step_id,
                    ts.start_year,
                    SUM(t.acres) as total_acres
                FROM land_use_transitions t
                JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                JOIN scenarios s ON t.scenario_id = s.scenario_id
                WHERE s.scenario_name = ?
                GROUP BY t.fips_code, ts.time_step_id, ts.start_year
            ),
            area_differences AS (
                SELECT 
                    t1.fips_code,
                    t1.time_step_id,
                    t1.start_year,
                    ABS(t1.total_acres - t2.total_acres) as area_difference
                FROM time_step_totals t1
                JOIN time_step_totals t2 
                ON t1.fips_code = t2.fips_code 
                AND t1.start_year > t2.start_year
                AND NOT EXISTS (
                    SELECT 1 FROM time_step_totals t3
                    WHERE t3.fips_code = t1.fips_code
                    AND t3.start_year > t2.start_year 
                    AND t3.start_year < t1.start_year
                )
            )
            SELECT * FROM area_differences
            WHERE area_difference > 0.01
            LIMIT 5
        """
        
        # Check 2: Negative acres
        negative_check_query = """
            SELECT 
                t.fips_code,
                t.from_land_use,
                t.to_land_use,
                t.acres,
                ts.start_year,
                ts.end_year
            FROM land_use_transitions t
            JOIN time_steps ts ON t.time_step_id = ts.time_step_id
            JOIN scenarios s ON t.scenario_id = s.scenario_id
            WHERE s.scenario_name = ?
            AND t.acres < 0
            LIMIT 5
        """
        
        # Execute checks
        cursor.execute(area_check_query, [scenario_name])
        area_inconsistencies = [dict(row) for row in cursor.fetchall()]
        
        cursor.execute(negative_check_query, [scenario_name])
        negative_acres = [dict(row) for row in cursor.fetchall()]
        
        return {
            'area_inconsistencies': area_inconsistencies,
            'negative_acres': negative_acres
        }

    @staticmethod
    def rank_scenarios_by_forest_loss(
        target_year: int,
        climate_model: str = 'NorESM1_M'
    ) -> List[Dict[str, Any]]:
        """
        Query III.10: Rank emissions scenarios based on projected forest loss.
        
        Args:
            target_year: Target year for analysis (e.g., 2070)
            climate_model: Climate model to analyze (defaults to 'middle' model)
            
        Returns:
            List of dictionaries containing scenario info and forest loss metrics
        """
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        
        query = """
            WITH forest_changes AS (
                SELECT 
                    s.scenario_name,
                    s.rcp as emissions_forcing,
                    s.ssp as socioeconomic_pathway,
                    SUM(CASE WHEN t.from_land_use = 'Forest' THEN t.acres ELSE 0 END) as forest_loss,
                    SUM(CASE WHEN t.to_land_use = 'Forest' THEN t.acres ELSE 0 END) as forest_gain
                FROM land_use_transitions t
                JOIN scenarios s ON t.scenario_id = s.scenario_id
                JOIN time_steps ts ON t.time_step_id = ts.time_step_id
                WHERE s.gcm = ?
                AND ts.end_year <= ?
                GROUP BY s.scenario_name, s.rcp, s.ssp
            )
            SELECT 
                scenario_name,
                emissions_forcing,
                socioeconomic_pathway,
                forest_loss,
                forest_gain,
                forest_loss - forest_gain as net_forest_loss,
                RANK() OVER (ORDER BY (forest_loss - forest_gain) DESC) as loss_rank
            FROM forest_changes
            ORDER BY loss_rank ASC
        """
        
        cursor.execute(query, [climate_model, target_year])
        result = [dict(row) for row in cursor.fetchall()]
        return result

    # More query methods will be added for each section... 