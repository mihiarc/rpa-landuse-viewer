-- RPA Land Use Viewer Database initialization
-- Creates the core schema for the land use projections data

-- Land use scenarios metadata
CREATE TABLE IF NOT EXISTS scenarios (
    scenario_id INTEGER PRIMARY KEY,
    scenario_name TEXT UNIQUE NOT NULL,
    gcm TEXT NOT NULL, -- Global Climate Model: CNRM_CM5, HadGEM2_ES365, etc.
    rcp TEXT NOT NULL, -- Representative Concentration Pathway: rcp45, rcp85
    ssp TEXT NOT NULL, -- Shared Socioeconomic Pathway: ssp1, ssp2, etc.
    description TEXT
);

-- Time steps
CREATE TABLE IF NOT EXISTS time_steps (
    time_step_id INTEGER PRIMARY KEY,
    time_step_name TEXT UNIQUE NOT NULL, -- e.g., "2012-2020"
    start_year INTEGER NOT NULL,
    end_year INTEGER NOT NULL
);

-- US Counties metadata
CREATE TABLE IF NOT EXISTS counties (
    fips_code TEXT PRIMARY KEY,
    county_name TEXT,
    state_name TEXT,
    state_fips TEXT,
    region TEXT
);

-- Land use categories
CREATE TABLE IF NOT EXISTS land_use_categories (
    category_code TEXT PRIMARY KEY, -- 'cr', 'ps', 'rg', 'fr', 'ur'
    category_name TEXT NOT NULL,    -- 'cropland', 'pasture', etc.
    description TEXT
);

-- Land use transition data (main data table)
CREATE TABLE IF NOT EXISTS land_use_transitions (
    transition_id INTEGER PRIMARY KEY,
    scenario_id INTEGER NOT NULL,
    time_step_id INTEGER NOT NULL,
    fips_code TEXT NOT NULL,
    from_land_use TEXT NOT NULL,
    to_land_use TEXT NOT NULL,
    area_hundreds_acres DOUBLE NOT NULL,
    FOREIGN KEY (scenario_id) REFERENCES scenarios(scenario_id),
    FOREIGN KEY (time_step_id) REFERENCES time_steps(time_step_id),
    FOREIGN KEY (fips_code) REFERENCES counties(fips_code),
    FOREIGN KEY (from_land_use) REFERENCES land_use_categories(category_code),
    FOREIGN KEY (to_land_use) REFERENCES land_use_categories(category_code)
);

-- Add basic indexes (additional ones will be created by SchemaManager)
CREATE INDEX IF NOT EXISTS idx_land_use_transitions ON land_use_transitions (scenario_id, time_step_id, fips_code);
CREATE INDEX IF NOT EXISTS idx_from_land_use ON land_use_transitions (from_land_use);
CREATE INDEX IF NOT EXISTS idx_to_land_use ON land_use_transitions (to_land_use);

-- Insert land use categories
INSERT OR IGNORE INTO land_use_categories (category_code, category_name, description) VALUES
    ('cr', 'Cropland', 'Agricultural cropland'),
    ('ps', 'Pasture', 'Pasture land'),
    ('rg', 'Rangeland', 'Rangeland'),
    ('fr', 'Forest', 'Forest land'),
    ('ur', 'Urban', 'Urban developed land'),
    ('t1', 'Total', 'Total area at starting year'),
    ('t2', 'Total', 'Total area at ending year'); 