"""
Script to execute the common queries and save results to a markdown file.
"""

import sys
import os
import pandas as pd
from pathlib import Path

# Add the src directory to the path to access shared modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src.db.database import DatabaseConnection
from src.db.common_queries import CommonQueries

def execute_queries_and_generate_md():
    """Execute all common queries and write results to a markdown file."""
    
    output_file = "docs/query_results.md"
    
    # Get a database connection to fetch some sample parameters
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    # Get a sample scenario
    cursor.execute("SELECT scenario_id, scenario_name FROM scenarios LIMIT 1")
    scenario = cursor.fetchone()
    scenario_id, scenario_name = scenario
    
    # Get a sample time range
    cursor.execute("SELECT start_year, end_year FROM time_steps ORDER BY start_year LIMIT 1")
    first_period = cursor.fetchone()
    start_year = first_period[0]
    
    cursor.execute("SELECT start_year, end_year FROM time_steps ORDER BY end_year DESC LIMIT 1")
    last_period = cursor.fetchone()
    end_year = last_period[1]
    
    # Get sample land use types
    cursor.execute("SELECT DISTINCT from_land_use FROM land_use_transitions LIMIT 5")
    land_use_types = [row[0] for row in cursor.fetchall()]
    
    # Get climate models
    cursor.execute("SELECT DISTINCT gcm FROM scenarios LIMIT 2")
    climate_models = [row[0] for row in cursor.fetchall()]
    
    DatabaseConnection.close_connection(conn)
    
    # Start building the markdown file
    markdown = f"""# RPA Land Use Change Analysis Query Results

This document contains the results of executing the common queries defined in the documentation.
These queries demonstrate the core functionality of the RPA land use change analysis system.

Parameters used for these queries:
- Scenario ID: {scenario_id} ({scenario_name})
- Year range: {start_year} to {end_year}
- Land use types: {', '.join(land_use_types)}

## I. Overall Change and Trends

### 1. Total Net Change by Land Use Type

What is the total net change (gain or loss in acres) for each land use type across all counties 
over the entire projection period ({start_year}-{end_year})?

```
"""
    
    # Query 1: Total Net Change by Land Use Type
    try:
        results = CommonQueries.total_net_change_by_land_use_type(start_year, end_year, scenario_id)
        markdown += results.to_string(index=False)
    except Exception as e:
        markdown += f"Error executing query: {str(e)}"
    
    # Query 2: Annualized Change Rate
    markdown += f"""
```

### 2. Annualized Change Rate

What is the average annual rate of change (in acres per year) for each land use type during each projection period?

```
"""
    
    try:
        results = CommonQueries.annualized_change_rate(scenario_id)
        # Limit to first 10 rows for brevity
        markdown += results.head(10).to_string(index=False)
        markdown += "\n\n(showing first 10 rows)"
    except Exception as e:
        markdown += f"Error executing query: {str(e)}"
    
    # Query 3: Peak Change Time Period
    markdown += f"""
```

### 3. Peak Change Time Period

During which time period is the largest net change observed for each land use type?

```
"""
    
    try:
        results = CommonQueries.peak_change_time_period(scenario_id)
        markdown += results.to_string(index=False)
    except Exception as e:
        markdown += f"Error executing query: {str(e)}"
    
    # Query 4: Change by State
    markdown += f"""
```

## II. Geographic Analysis

### 4. Change by State

What is the net change for each land use type within each state over the entire projection period?

```
"""
    
    try:
        results = CommonQueries.change_by_state(start_year, end_year, scenario_id)
        # Limit to first 15 rows for brevity
        markdown += results.head(15).to_string(index=False)
        markdown += "\n\n(showing first 15 rows)"
    except Exception as e:
        markdown += f"Error executing query: {str(e)}"
    
    # Query 5: Top N Counties by Change
    markdown += f"""
```

### 5. Top Counties by Change

Which are the top 10 counties with the largest increase in {land_use_types[0]} land use?

```
"""
    
    try:
        results = CommonQueries.top_counties_by_change(
            land_use_type=land_use_types[0],
            limit=10,
            direction='increase',
            start_year=start_year, 
            end_year=end_year,
            scenario_id=scenario_id
        )
        markdown += results.to_string(index=False)
    except Exception as e:
        markdown += f"Error executing query: {str(e)}"
    
    # Query 11: Major Transitions
    markdown += f"""
```

## IV. Land Use Transition Analysis

### 11. Major Transitions

What are the top 10 most common land use transitions observed across the region between {start_year} and {end_year}?

```
"""
    
    try:
        results = CommonQueries.major_transitions(start_year, end_year, scenario_id, limit=10)
        markdown += results.to_string(index=False)
    except Exception as e:
        markdown += f"Error executing query: {str(e)}"
    
    # Query 8: Impact of Climate Models
    markdown += f"""
```

## III. Scenario Comparison

### 8. Impact of Climate Models

How does the projected change in {land_use_types[0]} differ between climate models by {end_year}?

```
"""
    
    try:
        if len(climate_models) >= 2:
            results = CommonQueries.compare_climate_models(
                year=end_year,
                land_use_type=land_use_types[0],
                climate_model1=climate_models[0],
                climate_model2=climate_models[1]
            )
            markdown += results.to_string(index=False)
        else:
            markdown += "Not enough climate models to compare"
    except Exception as e:
        markdown += f"Error executing query: {str(e)}"
    
    # Data Integrity Checks
    markdown += f"""
```

## V. Data Integrity Checks

### 14-16. Data Integrity Checks

Results of various data integrity checks:

```
"""
    
    try:
        results = CommonQueries.check_data_integrity()
        
        # Area consistency check
        markdown += "### Area Consistency Check (inconsistencies found):\n"
        if len(results['area_consistency']) > 0:
            markdown += results['area_consistency'].head(10).to_string(index=False)
            if len(results['area_consistency']) > 10:
                markdown += "\n(showing first 10 rows)"
        else:
            markdown += "No inconsistencies found."
        
        # Negative acres check
        markdown += "\n\n### Negative Acres Check:\n"
        if len(results['negative_acres']) > 0:
            markdown += results['negative_acres'].to_string(index=False)
        else:
            markdown += "No negative acre values found."
        
        # Duplicate identifiers check
        markdown += "\n\n### Duplicate Identifiers Check:\n"
        
        if len(results['duplicate_scenarios']) > 0:
            markdown += "Duplicate scenario names:\n"
            markdown += results['duplicate_scenarios'].to_string(index=False)
        else:
            markdown += "No duplicate scenario names found.\n"
        
        if len(results['duplicate_fips']) > 0:
            markdown += "\nDuplicate FIPS codes:\n"
            markdown += results['duplicate_fips'].to_string(index=False)
        else:
            markdown += "No duplicate FIPS codes found.\n"
        
        if len(results['duplicate_time_steps']) > 0:
            markdown += "\nDuplicate time steps:\n"
            markdown += results['duplicate_time_steps'].to_string(index=False)
        else:
            markdown += "No duplicate time steps found."
            
    except Exception as e:
        markdown += f"Error executing query: {str(e)}"
    
    markdown += f"""
```

## Summary

These query results demonstrate the core functionality of the RPA land use change analysis system.
The system can effectively answer a wide range of questions about land use changes over time,
across different geographic scales, and under different scenarios.
"""
    
    # Ensure the docs directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Write the markdown to a file
    with open(output_file, "w") as f:
        f.write(markdown)
    
    print(f"Query results written to {output_file}")

if __name__ == "__main__":
    execute_queries_and_generate_md() 