USE rpa_mysql_db;

-- Insert scenarios
INSERT INTO scenarios (scenario_name, gcm, rcp, ssp) VALUES
('rcp85_ssp3', 'NorESM1_M', 'rcp85', 'ssp3'),
('rcp45_ssp1', 'NorESM1_M', 'rcp45', 'ssp1'),
('rcp85_ssp5', 'NorESM1_M', 'rcp85', 'ssp5');

-- Insert time steps
INSERT INTO time_steps (start_year, end_year) VALUES
(2020, 2030),
(2030, 2040),
(2040, 2050);

-- Insert sample counties
INSERT INTO counties (fips_code, county_name) VALUES
('06037', 'Los Angeles County'),
('36061', 'New York County'),
('17031', 'Cook County');

-- Insert sample land use transitions
INSERT INTO land_use_transitions 
(scenario_id, time_step_id, fips_code, from_land_use, to_land_use, acres) VALUES
-- Scenario 1 (rcp85_ssp3)
(1, 1, '06037', 'Forest', 'Urban', 1000),
(1, 1, '06037', 'Forest', 'Crop', 500),
(1, 1, '36061', 'Forest', 'Urban', 800),
(1, 2, '06037', 'Forest', 'Urban', 1200),
(1, 2, '17031', 'Forest', 'Crop', 600),

-- Scenario 2 (rcp45_ssp1)
(2, 1, '06037', 'Forest', 'Urban', 800),
(2, 1, '36061', 'Forest', 'Urban', 600),
(2, 2, '06037', 'Forest', 'Urban', 900),
(2, 2, '17031', 'Forest', 'Crop', 400),

-- Scenario 3 (rcp85_ssp5)
(3, 1, '06037', 'Forest', 'Urban', 1500),
(3, 1, '36061', 'Forest', 'Urban', 1000),
(3, 2, '06037', 'Forest', 'Urban', 1800),
(3, 2, '17031', 'Forest', 'Crop', 900); 