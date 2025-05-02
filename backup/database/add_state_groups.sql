-- Add state grouping capabilities to RPA Land Use database
-- This file adds state table and views for aggregating county-level data to state level

-- Create states table
CREATE TABLE IF NOT EXISTS states (
    state_fips VARCHAR PRIMARY KEY,
    state_name VARCHAR NOT NULL,
    state_abbr VARCHAR NOT NULL
);

-- Create relationship table linking counties to states
-- No need for a separate table since the first 2 digits of county FIPS is the state FIPS
-- Instead, create a view to express this relationship
CREATE VIEW IF NOT EXISTS county_state_map AS
    SELECT 
        c.fips_code AS county_fips,
        c.county_name,
        SUBSTRING(c.fips_code, 1, 2) AS state_fips,
        s.state_name,
        s.state_abbr
    FROM counties c
    LEFT JOIN states s ON SUBSTRING(c.fips_code, 1, 2) = s.state_fips;

-- Create view for state-level land use transitions
CREATE VIEW IF NOT EXISTS state_land_use_transitions AS
    WITH county_transitions AS (
        SELECT 
            lut.scenario_id,
            lut.time_step_id,
            SUBSTRING(lut.fips_code, 1, 2) AS state_fips,
            lut.from_land_use,
            lut.to_land_use,
            SUM(lut.acres) AS acres
        FROM land_use_transitions lut
        GROUP BY 
            lut.scenario_id,
            lut.time_step_id,
            state_fips,
            lut.from_land_use,
            lut.to_land_use
    )
    SELECT 
        ct.scenario_id,
        s.scenario_name,
        ct.time_step_id,
        ts.start_year,
        ts.end_year,
        ct.state_fips,
        st.state_name,
        st.state_abbr,
        ct.from_land_use,
        ct.to_land_use,
        ct.acres
    FROM county_transitions ct
    JOIN scenarios s ON ct.scenario_id = s.scenario_id
    JOIN time_steps ts ON ct.time_step_id = ts.time_step_id
    LEFT JOIN states st ON ct.state_fips = st.state_fips;

-- Index to retrieve state FIPS code from county FIPS
CREATE INDEX IF NOT EXISTS idx_counties_state_fips ON counties(SUBSTRING(fips_code, 1, 2));

-- Recursive Common Table Expression for hierarchical queries
-- This CTE can be used to traverse from counties to states to regions, etc.
CREATE VIEW IF NOT EXISTS region_hierarchy AS
    WITH RECURSIVE region_tree(region_id, region_name, region_type, parent_id, level) AS (
        -- Base case: counties with their parent states
        SELECT 
            c.fips_code AS region_id,
            c.county_name AS region_name,
            'COUNTY' AS region_type,
            SUBSTRING(c.fips_code, 1, 2) AS parent_id,
            0 AS level
        FROM counties c
        
        UNION ALL
        
        -- States level
        SELECT 
            s.state_fips AS region_id,
            s.state_name AS region_name,
            'STATE' AS region_type,
            NULL AS parent_id, -- For now, states have no parent. Will be updated when sub-regions are added
            1 AS level
        FROM states s
    )
    SELECT * FROM region_tree;

-- Helper function to get all counties in a state
CREATE VIEW IF NOT EXISTS counties_by_state AS
    SELECT 
        SUBSTRING(c.fips_code, 1, 2) AS state_fips,
        s.state_name,
        s.state_abbr,
        c.fips_code AS county_fips,
        c.county_name
    FROM counties c
    LEFT JOIN states s ON SUBSTRING(c.fips_code, 1, 2) = s.state_fips
    ORDER BY state_name, county_name;

-- Create a view for state aggregated metrics
CREATE VIEW IF NOT EXISTS state_land_use_summary AS
    SELECT 
        scenario_id,
        time_step_id,
        state_fips,
        state_name,
        state_abbr,
        from_land_use,
        to_land_use,
        SUM(acres) AS total_acres,
        COUNT(DISTINCT SUBSTRING(fips_code, 3)) AS county_count
    FROM land_use_transitions lut
    JOIN counties_by_state cbs ON lut.fips_code = cbs.county_fips
    GROUP BY 
        scenario_id,
        time_step_id,
        state_fips,
        state_name,
        state_abbr,
        from_land_use,
        to_land_use; 