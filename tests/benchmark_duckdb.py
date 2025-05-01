import time
import sqlite3
import duckdb

# Complex query to test
query = """
WITH transitions_summary AS (
    SELECT 
        lut.scenario_id,
        s.scenario_name,
        lut.from_land_use as land_use_type,
        -SUM(lut.acres) as net_change
    FROM land_use_transitions lut
    JOIN scenarios s ON lut.scenario_id = s.scenario_id
    JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
    WHERE lut.scenario_id = 1
    GROUP BY lut.scenario_id, s.scenario_name, lut.from_land_use
    
    UNION ALL
    
    SELECT 
        lut.scenario_id,
        s.scenario_name,
        lut.to_land_use as land_use_type,
        SUM(lut.acres) as net_change
    FROM land_use_transitions lut
    JOIN scenarios s ON lut.scenario_id = s.scenario_id
    JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
    WHERE lut.scenario_id = 1
    GROUP BY lut.scenario_id, s.scenario_name, lut.to_land_use
)

SELECT 
    scenario_id,
    scenario_name,
    land_use_type,
    SUM(net_change) as total_acres
FROM transitions_summary
GROUP BY scenario_id, scenario_name, land_use_type
ORDER BY scenario_name, land_use_type
"""

# Test SQLite
print("Testing SQLite performance...")
sqlite_conn = sqlite3.connect('data/database/rpa_landuse.db')
start = time.time()
sqlite_cursor = sqlite_conn.cursor()
sqlite_cursor.execute(query)
results = sqlite_cursor.fetchall()
sqlite_time = time.time() - start
sqlite_conn.close()
print(f"Retrieved {len(results)} rows")

# Test DuckDB
print("Testing DuckDB performance...")
duck_conn = duckdb.connect('data/database/rpa_landuse_duck.db')
start = time.time()
results = duck_conn.execute(query).fetchall()
duck_time = time.time() - start
duck_conn.close()
print(f"Retrieved {len(results)} rows")

# Report results
print("\nPerformance Comparison:")
print(f"SQLite execution time: {sqlite_time:.4f} seconds")
print(f"DuckDB execution time: {duck_time:.4f} seconds")
print(f"Improvement factor: {sqlite_time/duck_time:.2f}x") 