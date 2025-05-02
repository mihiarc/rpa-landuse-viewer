-- Create average scenario view
-- This script creates an average scenario by calculating the average land use transition
-- across all scenarios to provide an "ensemble average" view.

-- First, create a special scenario record in the scenarios table if it doesn't exist
INSERT INTO scenarios (scenario_id, scenario_name, gcm, rcp, ssp)
SELECT 
    (SELECT MAX(scenario_id) FROM scenarios) + 1,
    'Average (All Scenarios)',
    'ENSEMBLE',
    'ENSEMBLE',
    'ENSEMBLE'
WHERE NOT EXISTS (
    SELECT 1 FROM scenarios WHERE scenario_name = 'Average (All Scenarios)'
);

-- Delete any existing average data (in case this is a re-run)
DELETE FROM land_use_transitions 
WHERE scenario_id = (SELECT scenario_id FROM scenarios WHERE scenario_name = 'Average (All Scenarios)');

DELETE FROM land_use_summary
WHERE scenario_id = (SELECT scenario_id FROM scenarios WHERE scenario_name = 'Average (All Scenarios)');

-- Insert averaged land use transitions
INSERT INTO land_use_transitions (scenario_id, time_step_id, fips_code, from_land_use, to_land_use, acres)
SELECT 
    (SELECT scenario_id FROM scenarios WHERE scenario_name = 'Average (All Scenarios)') AS scenario_id,
    lut.time_step_id,
    lut.fips_code,
    lut.from_land_use,
    lut.to_land_use,
    AVG(lut.acres) AS acres
FROM 
    land_use_transitions lut
WHERE 
    lut.scenario_id != (SELECT scenario_id FROM scenarios WHERE scenario_name = 'Average (All Scenarios)')
GROUP BY 
    lut.time_step_id,
    lut.fips_code,
    lut.from_land_use,
    lut.to_land_use;

-- Populate the land_use_summary table with the average scenario data
INSERT INTO land_use_summary 
SELECT 
    (SELECT scenario_id FROM scenarios WHERE scenario_name = 'Average (All Scenarios)') AS scenario_id,
    'Average (All Scenarios)' AS scenario_name,
    ts.start_year AS year,
    st.state_fips AS state,
    st.state_name,
    c.fips_code AS county,
    c.county_name,
    -- Calculate acres for each land use type
    SUM(CASE WHEN lut.from_land_use = 'Cropland' THEN lut.acres ELSE 0 END) AS cropland_acres,
    SUM(CASE WHEN lut.from_land_use = 'Pasture' THEN lut.acres ELSE 0 END) AS pasture_acres,
    SUM(CASE WHEN lut.from_land_use = 'Range' THEN lut.acres ELSE 0 END) AS range_acres,
    SUM(CASE WHEN lut.from_land_use = 'Forest' THEN lut.acres ELSE 0 END) AS forest_acres,
    SUM(CASE WHEN lut.from_land_use = 'Urban' THEN lut.acres ELSE 0 END) AS urban_acres,
    -- Calculate total acres
    SUM(lut.acres) AS total_acres
FROM 
    land_use_transitions lut
JOIN 
    time_steps ts ON lut.time_step_id = ts.time_step_id
JOIN 
    counties c ON lut.fips_code = c.fips_code
JOIN 
    states st ON SUBSTRING(c.fips_code, 1, 2) = st.state_fips
WHERE
    lut.scenario_id = (SELECT scenario_id FROM scenarios WHERE scenario_name = 'Average (All Scenarios)')
GROUP BY 
    lut.scenario_id,
    'Average (All Scenarios)',
    ts.start_year,
    st.state_fips,
    st.state_name,
    c.fips_code,
    c.county_name;

-- Verify data was created
SELECT COUNT(*) FROM land_use_summary WHERE scenario_name = 'Average (All Scenarios)'; 