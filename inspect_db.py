#!/usr/bin/env python
import duckdb
import sys

# Connect to the database
conn = duckdb.connect('data/database/rpa.db')

# Get a list of all tables
print("Tables in the database:")
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
for table in tables:
    print(f"  - {table[0]}")

# For each table, get its schema
print("\nTable schemas:")
for table in tables:
    table_name = table[0]
    print(f"\nSchema for table '{table_name}':")
    
    # Get column info
    columns = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    for col in columns:
        col_id, col_name, col_type, not_null, default_val, is_pk = col
        print(f"  - {col_name} ({col_type})" + (" PRIMARY KEY" if is_pk else "") + (" NOT NULL" if not_null else ""))
    
    # Get first row for sample data
    try:
        first_row = conn.execute(f"SELECT * FROM {table_name} LIMIT 1").fetchone()
        if first_row:
            print(f"\n  Sample data: {first_row}")
    except Exception as e:
        print(f"  Error getting sample data: {e}")

# Close the connection
conn.close() 