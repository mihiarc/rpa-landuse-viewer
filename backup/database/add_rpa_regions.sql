-- Add RPA (Resource Planning Act) Assessment regions to the database
-- These regions are used for summarizing land use transitions at different geographical levels

-- Create RPA region tables
CREATE TABLE IF NOT EXISTS rpa_regions (
    region_id VARCHAR PRIMARY KEY,
    region_name VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS rpa_subregions (
    subregion_id VARCHAR PRIMARY KEY,
    subregion_name VARCHAR NOT NULL,
    parent_region_id VARCHAR NOT NULL,
    FOREIGN KEY (parent_region_id) REFERENCES rpa_regions(region_id)
);

CREATE TABLE IF NOT EXISTS rpa_state_mapping (
    state_fips VARCHAR NOT NULL,
    subregion_id VARCHAR NOT NULL,
    PRIMARY KEY (state_fips, subregion_id),
    FOREIGN KEY (state_fips) REFERENCES states(state_fips),
    FOREIGN KEY (subregion_id) REFERENCES rpa_subregions(subregion_id)
);

-- Insert RPA regions
INSERT OR REPLACE INTO rpa_regions (region_id, region_name)
VALUES
    ('NORTH', 'North Region'),
    ('SOUTH', 'South Region'),
    ('ROCKY', 'Rocky Mountain Region'),
    ('PACIF', 'Pacific Coast Region');

-- Insert RPA subregions
INSERT OR REPLACE INTO rpa_subregions (subregion_id, subregion_name, parent_region_id)
VALUES
    ('NORTHEAST', 'Northeast', 'NORTH'),
    ('NCENTRAL', 'North Central', 'NORTH'),
    ('SEAST', 'Southeast', 'SOUTH'), 
    ('SCENTRAL', 'South Central', 'SOUTH'),
    ('GPRAIRIE', 'Great Plains', 'SOUTH'),
    ('RMTNNORTH', 'Rocky Mountain North', 'ROCKY'),
    ('RMTNSOUTH', 'Rocky Mountain South', 'ROCKY'),
    ('PACNW', 'Pacific Northwest', 'PACIF'),
    ('PACCOAS', 'Pacific Coast', 'PACIF'),
    ('ALASKA', 'Alaska', 'PACIF'),
    ('HAWAII', 'Hawaii', 'PACIF'),
    ('CARIBBEAN', 'Caribbean', 'SOUTH'),
    ('OTHRPAC', 'Other Pacific Islands', 'PACIF');

-- Map states to subregions
-- This reflects the RPA Assessment mapping of states to regions
INSERT OR REPLACE INTO rpa_state_mapping (state_fips, subregion_id)
VALUES
    -- Northeast
    ('09', 'NORTHEAST'), -- Connecticut
    ('23', 'NORTHEAST'), -- Maine
    ('25', 'NORTHEAST'), -- Massachusetts
    ('33', 'NORTHEAST'), -- New Hampshire
    ('34', 'NORTHEAST'), -- New Jersey
    ('36', 'NORTHEAST'), -- New York
    ('42', 'NORTHEAST'), -- Pennsylvania
    ('44', 'NORTHEAST'), -- Rhode Island
    ('50', 'NORTHEAST'), -- Vermont
    
    -- North Central
    ('17', 'NCENTRAL'), -- Illinois
    ('18', 'NCENTRAL'), -- Indiana
    ('19', 'NCENTRAL'), -- Iowa
    ('26', 'NCENTRAL'), -- Michigan
    ('27', 'NCENTRAL'), -- Minnesota
    ('29', 'NCENTRAL'), -- Missouri
    ('39', 'NCENTRAL'), -- Ohio
    ('55', 'NCENTRAL'), -- Wisconsin
    
    -- Southeast
    ('10', 'SEAST'), -- Delaware
    ('11', 'SEAST'), -- District of Columbia
    ('12', 'SEAST'), -- Florida
    ('13', 'SEAST'), -- Georgia
    ('24', 'SEAST'), -- Maryland
    ('37', 'SEAST'), -- North Carolina
    ('45', 'SEAST'), -- South Carolina
    ('51', 'SEAST'), -- Virginia
    ('54', 'SEAST'), -- West Virginia
    
    -- South Central
    ('01', 'SCENTRAL'), -- Alabama
    ('05', 'SCENTRAL'), -- Arkansas
    ('21', 'SCENTRAL'), -- Kentucky
    ('22', 'SCENTRAL'), -- Louisiana
    ('28', 'SCENTRAL'), -- Mississippi
    ('47', 'SCENTRAL'), -- Tennessee
    
    -- Great Plains
    ('20', 'GPRAIRIE'), -- Kansas
    ('31', 'GPRAIRIE'), -- Nebraska
    ('38', 'GPRAIRIE'), -- North Dakota
    ('40', 'GPRAIRIE'), -- Oklahoma
    ('46', 'GPRAIRIE'), -- South Dakota
    ('48', 'GPRAIRIE'), -- Texas
    
    -- Rocky Mountain North
    ('16', 'RMTNNORTH'), -- Idaho
    ('30', 'RMTNNORTH'), -- Montana
    ('56', 'RMTNNORTH'), -- Wyoming
    
    -- Rocky Mountain South
    ('04', 'RMTNSOUTH'), -- Arizona
    ('08', 'RMTNSOUTH'), -- Colorado
    ('35', 'RMTNSOUTH'), -- New Mexico
    ('49', 'RMTNSOUTH'), -- Utah
    
    -- Pacific Northwest
    ('41', 'PACNW'), -- Oregon
    ('53', 'PACNW'), -- Washington
    
    -- Pacific Coast
    ('06', 'PACCOAS'), -- California
    ('32', 'PACCOAS'), -- Nevada
    
    -- Alaska
    ('02', 'ALASKA'), -- Alaska
    
    -- Hawaii
    ('15', 'HAWAII'), -- Hawaii
    
    -- Caribbean
    ('72', 'CARIBBEAN'), -- Puerto Rico
    ('78', 'CARIBBEAN'); -- Virgin Islands

-- Create a comprehensive hierarchy view that connects counties to states to regions
CREATE VIEW IF NOT EXISTS rpa_hierarchy AS
WITH RECURSIVE hierarchy(region_id, region_name, region_type, parent_id, level, subregion_id, subregion_name, rpa_region_id, rpa_region_name) AS (
    -- Base case: counties
    SELECT 
        c.fips_code AS region_id,
        c.county_name AS region_name,
        'COUNTY' AS region_type,
        SUBSTRING(c.fips_code, 1, 2) AS parent_id,
        0 AS level,
        rsm.subregion_id,
        rs.subregion_name,
        rs.parent_region_id AS rpa_region_id,
        rr.region_name AS rpa_region_name
    FROM counties c
    LEFT JOIN states s ON SUBSTRING(c.fips_code, 1, 2) = s.state_fips
    LEFT JOIN rpa_state_mapping rsm ON s.state_fips = rsm.state_fips
    LEFT JOIN rpa_subregions rs ON rsm.subregion_id = rs.subregion_id
    LEFT JOIN rpa_regions rr ON rs.parent_region_id = rr.region_id
    
    UNION ALL
    
    -- States
    SELECT 
        s.state_fips AS region_id,
        s.state_name AS region_name,
        'STATE' AS region_type,
        rsm.subregion_id AS parent_id,
        1 AS level,
        rsm.subregion_id,
        rs.subregion_name,
        rs.parent_region_id AS rpa_region_id,
        rr.region_name AS rpa_region_name
    FROM states s
    LEFT JOIN rpa_state_mapping rsm ON s.state_fips = rsm.state_fips
    LEFT JOIN rpa_subregions rs ON rsm.subregion_id = rs.subregion_id
    LEFT JOIN rpa_regions rr ON rs.parent_region_id = rr.region_id
    
    UNION ALL
    
    -- Subregions
    SELECT 
        rs.subregion_id AS region_id,
        rs.subregion_name AS region_name,
        'SUBREGION' AS region_type,
        rs.parent_region_id AS parent_id,
        2 AS level,
        rs.subregion_id,
        rs.subregion_name,
        rs.parent_region_id AS rpa_region_id,
        rr.region_name AS rpa_region_name
    FROM rpa_subregions rs
    LEFT JOIN rpa_regions rr ON rs.parent_region_id = rr.region_id
    
    UNION ALL
    
    -- RPA Regions
    SELECT 
        rr.region_id AS region_id,
        rr.region_name AS region_name,
        'RPA_REGION' AS region_type,
        NULL AS parent_id,
        3 AS level,
        NULL AS subregion_id,
        NULL AS subregion_name,
        rr.region_id AS rpa_region_id,
        rr.region_name AS rpa_region_name
    FROM rpa_regions rr
)
SELECT * FROM hierarchy;

-- Create a view for RPA region land use transitions
CREATE VIEW IF NOT EXISTS rpa_region_land_use AS
SELECT 
    lut.scenario_id,
    s.scenario_name,
    ts.start_year,
    ts.end_year,
    rh.rpa_region_id,
    rh.rpa_region_name,
    rh.subregion_id,
    rh.subregion_name,
    lut.from_land_use,
    lut.to_land_use,
    SUM(lut.acres) AS acres
FROM land_use_transitions lut
JOIN counties c ON lut.fips_code = c.fips_code
JOIN rpa_hierarchy rh ON c.fips_code = rh.region_id AND rh.region_type = 'COUNTY'
JOIN scenarios s ON lut.scenario_id = s.scenario_id
JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
WHERE rh.rpa_region_id IS NOT NULL
GROUP BY
    lut.scenario_id,
    s.scenario_name,
    ts.start_year,
    ts.end_year,
    rh.rpa_region_id,
    rh.rpa_region_name,
    rh.subregion_id,
    rh.subregion_name,
    lut.from_land_use,
    lut.to_land_use;

-- Create a view for RPA subregion land use transitions
CREATE VIEW IF NOT EXISTS rpa_subregion_land_use AS
SELECT 
    lut.scenario_id,
    s.scenario_name,
    ts.start_year,
    ts.end_year,
    rh.subregion_id,
    rh.subregion_name,
    rh.rpa_region_id AS parent_region_id,
    rh.rpa_region_name AS parent_region_name,
    lut.from_land_use,
    lut.to_land_use,
    SUM(lut.acres) AS acres
FROM land_use_transitions lut
JOIN counties c ON lut.fips_code = c.fips_code
JOIN rpa_hierarchy rh ON c.fips_code = rh.region_id AND rh.region_type = 'COUNTY'
JOIN scenarios s ON lut.scenario_id = s.scenario_id
JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
WHERE rh.subregion_id IS NOT NULL
GROUP BY
    lut.scenario_id,
    s.scenario_name,
    ts.start_year,
    ts.end_year,
    rh.subregion_id,
    rh.subregion_name,
    rh.rpa_region_id,
    rh.rpa_region_name,
    lut.from_land_use,
    lut.to_land_use; 