-- Initialize database schema for RPA land use transitions database
-- This schema holds land use transitions across different scenarios and time steps

-- Create scenarios table
CREATE TABLE IF NOT EXISTS scenarios (
    scenario_id INTEGER PRIMARY KEY,
    scenario_name VARCHAR UNIQUE NOT NULL,
    gcm VARCHAR NOT NULL,
    rcp VARCHAR NOT NULL,
    ssp VARCHAR NOT NULL
);

-- Create time steps table
CREATE TABLE IF NOT EXISTS time_steps (
    time_step_id INTEGER PRIMARY KEY,
    start_year INTEGER NOT NULL,
    end_year INTEGER NOT NULL,
    UNIQUE(start_year, end_year)
);

-- Create counties table
CREATE TABLE IF NOT EXISTS counties (
    fips_code VARCHAR PRIMARY KEY,
    county_name VARCHAR
);

-- Create land use transitions table
CREATE TABLE IF NOT EXISTS land_use_transitions (
    transition_id INTEGER PRIMARY KEY,
    scenario_id INTEGER NOT NULL,
    time_step_id INTEGER NOT NULL,
    fips_code VARCHAR NOT NULL,
    from_land_use VARCHAR NOT NULL,
    to_land_use VARCHAR NOT NULL,
    acres DOUBLE NOT NULL,
    FOREIGN KEY (scenario_id) REFERENCES scenarios(scenario_id),
    FOREIGN KEY (time_step_id) REFERENCES time_steps(time_step_id),
    FOREIGN KEY (fips_code) REFERENCES counties(fips_code)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_land_use_transitions ON land_use_transitions (scenario_id, time_step_id, fips_code);
CREATE INDEX IF NOT EXISTS idx_from_land_use ON land_use_transitions (from_land_use);
CREATE INDEX IF NOT EXISTS idx_to_land_use ON land_use_transitions (to_land_use);
