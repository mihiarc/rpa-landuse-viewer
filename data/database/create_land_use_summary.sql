-- Create land_use_summary table
-- This view summarizes land use data for visualization in the RPA Land Use Viewer

-- Drop the table if it already exists
DROP TABLE IF EXISTS land_use_summary;

-- Create the land use summary table
CREATE TABLE land_use_summary AS
SELECT 
    lut.scenario_id,
    s.scenario_name,
    ts.start_year as year,
    st.state_fips as state,
    st.state_name,
    c.fips_code as county,
    c.county_name,
    -- Calculate acres for each land use type
    SUM(CASE WHEN lut.from_land_use = 'Cropland' THEN lut.acres ELSE 0 END) as cropland_acres,
    SUM(CASE WHEN lut.from_land_use = 'Pasture' THEN lut.acres ELSE 0 END) as pasture_acres,
    SUM(CASE WHEN lut.from_land_use = 'Range' THEN lut.acres ELSE 0 END) as range_acres,
    SUM(CASE WHEN lut.from_land_use = 'Forest' THEN lut.acres ELSE 0 END) as forest_acres,
    SUM(CASE WHEN lut.from_land_use = 'Urban' THEN lut.acres ELSE 0 END) as urban_acres,
    -- Calculate total acres
    SUM(lut.acres) as total_acres
FROM 
    land_use_transitions lut
JOIN 
    scenarios s ON lut.scenario_id = s.scenario_id
JOIN 
    time_steps ts ON lut.time_step_id = ts.time_step_id
JOIN 
    counties c ON lut.fips_code = c.fips_code
JOIN 
    states st ON SUBSTRING(c.fips_code, 1, 2) = st.state_fips
GROUP BY 
    lut.scenario_id, 
    s.scenario_name, 
    ts.start_year,
    st.state_fips,
    st.state_name,
    c.fips_code,
    c.county_name;

-- Create an index for better query performance
CREATE INDEX idx_land_use_summary_scenario_year ON land_use_summary(scenario_id, year);
CREATE INDEX idx_land_use_summary_state ON land_use_summary(state);
CREATE INDEX idx_land_use_summary_county ON land_use_summary(county);

-- Verify the table was created
SELECT COUNT(*) FROM land_use_summary; 