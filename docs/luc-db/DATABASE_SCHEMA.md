# RPA Land Use Database Schema

This document provides a comprehensive overview of the database schema used in the RPA Land Use Viewer project. The schema is implemented in DuckDB, a high-performance analytical database system that offers excellent capabilities for this application.

## Database Overview

- **Database File**: `data/database/rpa.db`
- **Total Size**: ~319MB
- **Records**: 5,432,198 land use transitions across 3,068 counties with 20 scenarios and 6 time steps

## Entity Relationship Diagram

```mermaid
erDiagram
    SCENARIOS ||--o{ LAND_USE_TRANSITIONS : has
    TIME_STEPS ||--o{ LAND_USE_TRANSITIONS : contains
    COUNTIES ||--o{ LAND_USE_TRANSITIONS : includes
    STATES ||--o{ COUNTIES : contains
    STATES ||--o{ RPA_STATE_MAPPING : "belongs to"
    RPA_REGIONS ||--o{ RPA_SUBREGIONS : contains
    RPA_SUBREGIONS ||--o{ RPA_STATE_MAPPING : includes
    
    SCENARIOS {
        int scenario_id PK
        string scenario_name
        string gcm
        string rcp
        string ssp
    }
    
    TIME_STEPS {
        int time_step_id PK
        int start_year
        int end_year
    }
    
    STATES {
        string state_fips PK
        string state_name
        string state_abbr
    }
    
    COUNTIES {
        string fips_code PK
        string county_name
    }
    
    LAND_USE_TRANSITIONS {
        int transition_id PK
        int scenario_id FK
        int time_step_id FK
        string fips_code FK
        string from_land_use
        string to_land_use
        float acres
    }
    
    RPA_REGIONS {
        string region_id PK
        string region_name
    }
    
    RPA_SUBREGIONS {
        string subregion_id PK
        string subregion_name
        string parent_region_id FK
    }
    
    RPA_STATE_MAPPING {
        string state_fips FK
        string subregion_id FK
    }
```

## Table Descriptions

### 1. `scenarios` (20 records)

Stores information about the climate scenarios used in the land use projections.

| Column | Type | Description |
|--------|------|-------------|
| scenario_id | INTEGER PRIMARY KEY AUTOINCREMENT | Unique identifier for the scenario |
| scenario_name | TEXT UNIQUE NOT NULL | Name of the scenario (e.g., "CNRM_CM5_rcp45_ssp1") |
| gcm | TEXT NOT NULL | Global Climate Model (e.g., "CNRM_CM5", "HadGEM2_ES365") |
| rcp | TEXT NOT NULL | Representative Concentration Pathway (e.g., "rcp45", "rcp85") |
| ssp | TEXT NOT NULL | Shared Socioeconomic Pathway (e.g., "ssp1", "ssp2") |

### 2. `time_steps` (6 records)

Stores information about the time periods for land use projections.

| Column | Type | Description |
|--------|------|-------------|
| time_step_id | INTEGER PRIMARY KEY AUTOINCREMENT | Unique identifier for the time step |
| start_year | INTEGER NOT NULL | Start year of the time period (e.g., 2020) |
| end_year | INTEGER NOT NULL | End year of the time period (e.g., 2030) |

The table includes a UNIQUE constraint on (start_year, end_year) to prevent duplicate time steps.

### 3. `states` (56 records)

Stores information about US states and territories.

| Column | Type | Description |
|--------|------|-------------|
| state_fips | TEXT PRIMARY KEY | The 2-digit FIPS code for the state |
| state_name | TEXT NOT NULL | Full name of the state |
| state_abbr | TEXT NOT NULL | Two letter state abbreviation |

### 4. `counties` (3,068 records)

Stores information about US counties.

| Column | Type | Description |
|--------|------|-------------|
| fips_code | TEXT PRIMARY KEY | The 5-digit FIPS code for the county |
| county_name | TEXT | Name of the county |

The first two digits of the FIPS code represent the state FIPS code, establishing a hierarchical relationship between states and counties.

### 5. `land_use_transitions` (5,432,198 records)

Stores information about land use transitions between different land types.

| Column | Type | Description |
|--------|------|-------------|
| transition_id | INTEGER PRIMARY KEY AUTOINCREMENT | Unique identifier for the transition |
| scenario_id | INTEGER NOT NULL | Foreign key to scenarios.scenario_id |
| time_step_id | INTEGER NOT NULL | Foreign key to time_steps.time_step_id |
| fips_code | TEXT NOT NULL | Foreign key to counties.fips_code |
| from_land_use | TEXT NOT NULL | Original land use type (Crop, Forest, Pasture, Range, Urban) |
| to_land_use | TEXT NOT NULL | New land use type (Crop, Forest, Pasture, Range, Urban) |
| acres | REAL NOT NULL | Land area in hundreds of acres |

### 6. `rpa_regions` (4 records)

Stores information about RPA (Resource Planning Act) Assessment regions.

| Column | Type | Description |
|--------|------|-------------|
| region_id | TEXT PRIMARY KEY | Unique identifier for the RPA region (e.g., "NORTH", "SOUTH") |
| region_name | TEXT NOT NULL | Full name of the region (e.g., "North Region") |

### 7. `rpa_subregions` (13 records)

Stores information about RPA Assessment subregions.

| Column | Type | Description |
|--------|------|-------------|
| subregion_id | TEXT PRIMARY KEY | Unique identifier for the subregion (e.g., "NORTHEAST", "PACNW") |
| subregion_name | TEXT NOT NULL | Full name of the subregion (e.g., "Northeast", "Pacific Northwest") |
| parent_region_id | TEXT NOT NULL | Foreign key to rpa_regions.region_id |

### 8. `rpa_state_mapping` (50 records)

Maps states to RPA subregions to establish the hierarchical relationship.

| Column | Type | Description |
|--------|------|-------------|
| state_fips | TEXT NOT NULL | Foreign key to states.state_fips |
| subregion_id | TEXT NOT NULL | Foreign key to rpa_subregions.subregion_id |

The table includes a composite primary key of (state_fips, subregion_id) to prevent duplicate mappings.

## Views

For efficient querying of hierarchical geographical data, the database includes the following views:

### 1. `county_state_map`

Maps counties to their parent states using the FIPS code relationship.

| Column | Description |
|--------|-------------|
| county_fips | The county FIPS code |
| county_name | Name of the county |
| state_fips | The state FIPS code (first 2 digits of county FIPS) |
| state_name | Name of the state |
| state_abbr | State abbreviation |

### 2. `state_land_use_transitions`

Aggregates land use transitions at the state level.

| Column | Description |
|--------|-------------|
| scenario_id | ID of the scenario |
| scenario_name | Name of the scenario |
| time_step_id | ID of the time step |
| start_year | Start year of the time period |
| end_year | End year of the time period |
| state_fips | State FIPS code |
| state_name | Name of the state |
| state_abbr | State abbreviation |
| from_land_use | Original land use type |
| to_land_use | New land use type |
| acres | Total acres transitioned |

### 3. `region_hierarchy`

Recursive Common Table Expression (CTE) that provides a hierarchical view of geographical entities.

| Column | Description |
|--------|-------------|
| region_id | Identifier for the region (either county FIPS or state FIPS) |
| region_name | Name of the region |
| region_type | Type of region ('COUNTY' or 'STATE') |
| parent_id | ID of the parent region (state FIPS for counties, NULL for states) |
| level | Hierarchy level (0 for counties, 1 for states) |

### 4. `counties_by_state`

Helper view to easily get all counties within a state.

| Column | Description |
|--------|-------------|
| state_fips | State FIPS code |
| state_name | Name of the state |
| state_abbr | State abbreviation |
| county_fips | County FIPS code |
| county_name | Name of the county |

### 5. `state_land_use_summary`

Aggregated metrics for land use by state.

| Column | Description |
|--------|-------------|
| scenario_id | ID of the scenario |
| time_step_id | ID of the time step |
| state_fips | State FIPS code |
| state_name | Name of the state |
| state_abbr | State abbreviation |
| from_land_use | Original land use type |
| to_land_use | New land use type |
| total_acres | Total acres transitioned |
| county_count | Number of counties with this transition |

### 6. `rpa_hierarchy`

Provides a comprehensive hierarchical view of the RPA region structure from counties to regions.

| Column | Description |
|--------|-------------|
| region_id | Identifier for the region entity (could be county FIPS, state FIPS, subregion ID, or region ID) |
| region_name | Name of the region entity |
| region_type | Type of region ('COUNTY', 'STATE', 'SUBREGION', or 'RPA_REGION') |
| parent_id | ID of the parent entity in the hierarchy |
| level | Hierarchy level (0 for counties, 1 for states, 2 for subregions, 3 for regions) |
| subregion_id | Subregion ID that this entity belongs to (NULL for region level) |
| subregion_name | Name of the subregion |
| rpa_region_id | Region ID that this entity belongs to |
| rpa_region_name | Name of the RPA region |

### 7. `rpa_region_land_use`

Aggregates land use transitions at the RPA region level.

| Column | Description |
|--------|-------------|
| scenario_id | ID of the scenario |
| scenario_name | Name of the scenario |
| start_year | Start year of the time period |
| end_year | End year of the time period |
| rpa_region_id | ID of the RPA region |
| rpa_region_name | Name of the RPA region |
| subregion_id | ID of the subregion |
| subregion_name | Name of the subregion |
| from_land_use | Original land use type |
| to_land_use | New land use type |
| acres | Total acres transitioned |

### 8. `rpa_subregion_land_use`

Aggregates land use transitions at the RPA subregion level.

| Column | Description |
|--------|-------------|
| scenario_id | ID of the scenario |
| scenario_name | Name of the scenario |
| start_year | Start year of the time period |
| end_year | End year of the time period |
| subregion_id | ID of the subregion |
| subregion_name | Name of the subregion |
| parent_region_id | ID of the parent RPA region |
| parent_region_name | Name of the parent RPA region |
| from_land_use | Original land use type |
| to_land_use | New land use type |
| acres | Total acres transitioned |

## Indexes

The database uses the following indexes to improve query performance:

```sql
CREATE INDEX idx_land_use_transitions ON land_use_transitions (scenario_id, time_step_id, fips_code);
CREATE INDEX idx_from_land_use ON land_use_transitions (from_land_use);
CREATE INDEX idx_to_land_use ON land_use_transitions (to_land_use);
CREATE INDEX idx_counties_state_fips ON counties(SUBSTRING(fips_code, 1, 2));
```

The `idx_counties_state_fips` index enables fast filtering of counties by state FIPS code, which is extracted as the first two characters of the county FIPS code.

## DuckDB Best Practices

The schema follows several DuckDB best practices:

1. **Use of INTEGER PRIMARY KEY**: Primary keys are defined as INTEGER PRIMARY KEY for optimal performance.

2. **Foreign Key Constraints**: All relationships between tables are explicitly defined using foreign key constraints to maintain data integrity.

3. **NOT NULL Constraints**: Key fields are declared as NOT NULL to prevent missing data and ensure data integrity.

4. **Composite Indexes**: Strategic indexes are created on frequently queried combinations of columns to improve query performance.

5. **UNIQUE Constraints**: Applied where appropriate to prevent duplicate data (e.g., time_steps table).

6. **Parallelism**: DuckDB allows for multi-threaded query execution which significantly improves analytical query performance.

## Recommended Schema Improvements

Based on DuckDB best practices, the following improvements could enhance the current schema:

### 1. Use Proper Data Types

DuckDB supports a rich set of data types suited for analytics:

```sql
CREATE TABLE scenarios (
  scenario_id INTEGER PRIMARY KEY,
  scenario_name VARCHAR UNIQUE NOT NULL,
  gcm VARCHAR NOT NULL,
  rcp VARCHAR NOT NULL,
  ssp VARCHAR NOT NULL
);
```

### 2. Add Column Constraints

Add CHECK constraints for fields to enforce data quality:

```sql
CREATE TABLE counties (
  fips_code VARCHAR PRIMARY KEY CHECK(LENGTH(fips_code) = 5),
  county_name VARCHAR NOT NULL CHECK(LENGTH(county_name) < 128)
);
```

### 3. Implement Boolean Fields Properly

For any boolean fields, use the BOOLEAN type:

```sql
CREATE TABLE example (
  id INTEGER PRIMARY KEY,
  is_active BOOLEAN NOT NULL
);
```

### 4. Add Schema Version Management

Implement version tracking for schema changes:

```sql
CREATE TABLE schema_info (
  version INTEGER PRIMARY KEY,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  description VARCHAR NOT NULL
);

-- Initialize with current version
INSERT INTO schema_info (version, description) VALUES (1, 'Initial schema');
```

### 5. Leverage DuckDB's Analytical Features

Take advantage of DuckDB's specialized features for analytics:

```sql
-- Create a materialized view for common aggregations
CREATE MATERIALIZED VIEW region_land_use_summary AS
SELECT 
    r.region_id,
    r.region_name,
    lt.from_land_use,
    lt.to_land_use,
    SUM(lt.acres) as total_acres
FROM land_use_transitions lt
JOIN counties c ON lt.fips_code = c.fips_code
JOIN rpa_hierarchy rh ON c.fips_code = rh.region_id
JOIN rpa_regions r ON rh.rpa_region_id = r.region_id
GROUP BY r.region_id, r.region_name, lt.from_land_use, lt.to_land_use;
```

## Data Flow Diagram

```mermaid
flowchart TD
    A[Raw RPA Data: JSON] -->|src/data_setup/converter.py| B[Processed Data: Parquet]
    B -->|src/data_setup/db_loader.py| C[DuckDB Database]
    C -->|src/db/queries.py| D[Query Results]
    D -->|app.py| E[Streamlit UI]
    
    subgraph Database["Database Schema"]
        F[Scenarios] -.->|referenced by| I[Land Use Transitions]
        H[Time Steps] -.->|referenced by| I
        G[Counties] -.->|referenced by| I
        M[States] -.->|contains| G
        M -.->|mapped to| L[RPA State Mapping]
        J[RPA Regions] -.->|contains| K[RPA Subregions]
        K -.->|referenced by| L
    end
```

## Schema Version Control Strategy

To effectively version control this DuckDB schema:

1. **Schema as Code**: Maintain the complete schema definition in the `init.sql` file under version control

2. **Migration Scripts**: For each schema change, create numbered migration scripts:
   ```
   migrations/
   ├── 001_initial_schema.sql
   ├── 002_add_indexes.sql
   ├── 003_add_constraints.sql
   └── 004_add_rpa_regions.sql
   ```

3. **Schema Dump Command**: Use the following command to dump the current schema for version control:
   ```bash
   duckdb data/database/rpa.db ".schema" > docs/current_schema.sql
   ```

4. **Schema Verification**: Add a verification step to ensure the schema matches what's expected:
   ```bash
   duckdb :memory: < docs/current_schema.sql
   ```

5. **Schema Documentation**: Automatically update this documentation when schema changes:
   ```bash
   # Count records in each table
   echo "Table record counts:" > docs/schema_stats.md
   echo "-------------------" >> docs/schema_stats.md
   duckdb data/database/rpa.db "SELECT 'scenarios: ' || COUNT(*) FROM scenarios; SELECT 'time_steps: ' || COUNT(*) FROM time_steps; SELECT 'counties: ' || COUNT(*) FROM counties; SELECT 'land_use_transitions: ' || COUNT(*) FROM land_use_transitions; SELECT 'rpa_regions: ' || COUNT(*) FROM rpa_regions; SELECT 'rpa_subregions: ' || COUNT(*) FROM rpa_subregions; SELECT 'rpa_state_mapping: ' || COUNT(*) FROM rpa_state_mapping;" >> docs/schema_stats.md
   ```

## Query Performance Considerations

To optimize query performance in DuckDB, consider the following:

1. **Use Prepared Statements**: For repeatedly executed queries to take advantage of DuckDB's query planning.

2. **Enable Parallelism**: Set appropriate thread count with `PRAGMA threads=4` for parallel query execution.

3. **Use EXPLAIN**: To understand query execution plans and identify potential performance bottlenecks.

4. **Consider Materialized Views**: For frequently accessed aggregated data.

5. **Use COPY Instead of INSERT**: For bulk loading data as it's much faster.

6. **Configure Memory Limits**: Adjust memory limits based on system capabilities with `PRAGMA memory_limit`.

## Data Type Considerations

DuckDB supports a rich set of SQL data types:

- **INTEGER/BIGINT**: For whole number values
- **VARCHAR/TEXT**: For string data
- **DOUBLE/DECIMAL**: For floating-point values
- **BOOLEAN**: For true/false values
- **TIMESTAMP**: For date and time values

DuckDB also supports nested types like STRUCT, LIST, and MAP which can be useful for complex data.

## Maintenance Recommendations

1. **Regular Checkpointing**: Run `CHECKPOINT` periodically to save in-memory changes.

2. **Database Backups**: Implement regular database backups to prevent data loss.

3. **Schema Version Tracking**: Consider implementing a version tracking table to manage schema migrations:

```sql
CREATE TABLE schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description VARCHAR
);
```

4. **Query Optimization**: Use `PRAGMA explain_output='all'` to get detailed query plans for optimization.

## Usage Examples

Here are some example queries to work with the database:

### Get all land use transitions for a specific county

```sql
SELECT lt.transition_id, s.scenario_name, ts.start_year, ts.end_year, 
       lt.from_land_use, lt.to_land_use, lt.acres
FROM land_use_transitions lt
JOIN scenarios s ON lt.scenario_id = s.scenario_id
JOIN time_steps ts ON lt.time_step_id = ts.time_step_id
WHERE lt.fips_code = '36001' -- Albany County, NY
ORDER BY ts.start_year, lt.from_land_use, lt.to_land_use;
```

### Get net change in forest land by time period

```sql
SELECT ts.start_year, ts.end_year,
       SUM(CASE WHEN lt.to_land_use = 'Forest' THEN lt.acres ELSE 0 END) -
       SUM(CASE WHEN lt.from_land_use = 'Forest' THEN lt.acres ELSE 0 END) AS net_forest_change
FROM land_use_transitions lt
JOIN time_steps ts ON lt.time_step_id = ts.time_step_id
WHERE lt.scenario_id = 1
GROUP BY ts.start_year, ts.end_year
ORDER BY ts.start_year;
```

### Get land use transitions aggregated by RPA region

```sql
SELECT r.region_name, lt.from_land_use, lt.to_land_use, SUM(lt.acres) as total_acres
FROM land_use_transitions lt
JOIN counties c ON lt.fips_code = c.fips_code
JOIN states s ON SUBSTRING(c.fips_code, 1, 2) = s.state_fips
JOIN rpa_state_mapping rsm ON s.state_fips = rsm.state_fips
JOIN rpa_subregions sr ON rsm.subregion_id = sr.subregion_id
JOIN rpa_regions r ON sr.parent_region_id = r.region_id
WHERE lt.scenario_id = 1
GROUP BY r.region_name, lt.from_land_use, lt.to_land_use
ORDER BY r.region_name, lt.from_land_use, lt.to_land_use;
``` 