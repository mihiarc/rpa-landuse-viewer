"""
Functions for extracting data from DuckDB and creating PandasAI semantic layers.
"""

import os
import duckdb
import pandas as pd
from pandasai import SmartDataframe
from pandasai.llm import OpenAI
from dotenv import load_dotenv


def extract_data_from_duckdb(db_path="data/database/rpa.db", output_dir="land_use_parquet"):
    """
    Extract data from DuckDB database and save as Parquet files.
    
    Args:
        db_path (str): Path to the DuckDB database file
        output_dir (str): Directory to save Parquet files
        
    Returns:
        dict: Dictionary with paths to the created Parquet files
    """
    # Create directory for parquet files if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Connect to DuckDB
    print(f"Connecting to DuckDB database: {db_path}")
    conn = duckdb.connect(db_path)
    
    # Output file paths
    output_files = {
        "scenarios": f"{output_dir}/scenarios.parquet",
        "land_use_categories": f"{output_dir}/land_use_categories.parquet",
        "time_steps": f"{output_dir}/time_steps.parquet",
        "counties": f"{output_dir}/counties.parquet",
        "transitions_summary": f"{output_dir}/transitions_summary.parquet",
        "county_transitions": f"{output_dir}/county_transitions.parquet",
        "urbanization_trends": f"{output_dir}/urbanization_trends.parquet"
    }
    
    # Extract reference tables
    print("Extracting reference tables...")
    conn.sql("SELECT * FROM scenarios").write_parquet(output_files["scenarios"])
    conn.sql("SELECT * FROM land_use_categories").write_parquet(output_files["land_use_categories"])
    conn.sql("SELECT * FROM time_steps").write_parquet(output_files["time_steps"])
    conn.sql("SELECT * FROM counties").write_parquet(output_files["counties"])
    
    # Create an aggregated transitions view
    print("Creating transitions summary...")
    conn.sql("""
        SELECT 
            s.scenario_name,
            s.gcm, 
            s.rcp,
            s.ssp,
            ts.time_step_name,
            ts.start_year,
            ts.end_year,
            c1.category_name as from_category,
            c2.category_name as to_category,
            SUM(t.area_hundreds_acres) as total_area
        FROM land_use_transitions t
        JOIN scenarios s ON t.scenario_id = s.scenario_id
        JOIN time_steps ts ON t.time_step_id = ts.time_step_id
        JOIN land_use_categories c1 ON t.from_land_use = c1.category_code
        JOIN land_use_categories c2 ON t.to_land_use = c2.category_code
        GROUP BY s.scenario_name, s.gcm, s.rcp, s.ssp, 
                ts.time_step_name, ts.start_year, ts.end_year, 
                c1.category_name, c2.category_name
    """).write_parquet(output_files["transitions_summary"])
    
    # Create a county-level aggregation
    print("Creating county-level transitions...")
    conn.sql("""
        SELECT 
            co.fips_code,
            co.county_name,
            co.state_name,
            s.scenario_name,
            ts.time_step_name,
            c1.category_name as from_category,
            c2.category_name as to_category,
            SUM(t.area_hundreds_acres) as total_area
        FROM land_use_transitions t
        JOIN counties co ON t.fips_code = co.fips_code
        JOIN scenarios s ON t.scenario_id = s.scenario_id
        JOIN time_steps ts ON t.time_step_id = ts.time_step_id
        JOIN land_use_categories c1 ON t.from_land_use = c1.category_code
        JOIN land_use_categories c2 ON t.to_land_use = c2.category_code
        GROUP BY co.fips_code, co.county_name, co.state_name,
                s.scenario_name, ts.time_step_name, 
                c1.category_name, c2.category_name
    """).write_parquet(output_files["county_transitions"])
    
    # Create a time series view for analyzing trends
    print("Creating urbanization trends...")
    conn.sql("""
        SELECT 
            s.scenario_name,
            ts.time_step_name,
            ts.start_year,
            SUM(CASE WHEN t.from_land_use = 'fr' AND t.to_land_use = 'ur' 
                THEN t.area_hundreds_acres ELSE 0 END) as forest_to_urban,
            SUM(CASE WHEN t.from_land_use = 'cr' AND t.to_land_use = 'ur' 
                THEN t.area_hundreds_acres ELSE 0 END) as cropland_to_urban,
            SUM(CASE WHEN t.from_land_use = 'ps' AND t.to_land_use = 'ur' 
                THEN t.area_hundreds_acres ELSE 0 END) as pasture_to_urban
        FROM land_use_transitions t
        JOIN scenarios s ON t.scenario_id = s.scenario_id
        JOIN time_steps ts ON t.time_step_id = ts.time_step_id
        GROUP BY s.scenario_name, ts.time_step_name, ts.start_year
        ORDER BY s.scenario_name, ts.start_year
    """).write_parquet(output_files["urbanization_trends"])
    
    # Close the database connection
    conn.close()
    print("DuckDB connection closed.")
    
    return output_files


def create_semantic_layers(parquet_dir="land_use_parquet", org_path="rpa-landuse"):
    """
    Create semantic layers using PandasAI for the extracted data.
    
    Args:
        parquet_dir (str): Directory containing the Parquet files
        org_path (str): Organization path prefix for PandasAI datasets
        
    Returns:
        dict: Dictionary with the created semantic layer objects
    """
    # Ensure API key is set
    load_dotenv(dotenv_path=".env")
    api_key = os.getenv("PANDASAI_API_KEY")
    
    if not api_key:
        raise ValueError(
            "No PANDASAI_API_KEY found in .env file. "
            "Please create a .env file with your PandasAI API key."
        )
    
    # Initialize OpenAI LLM with the API key
    llm = OpenAI(api_token=api_key)
    
    # Paths for the parquet files
    transitions_parquet = f"{parquet_dir}/transitions_summary.parquet"
    county_parquet = f"{parquet_dir}/county_transitions.parquet"
    urbanization_parquet = f"{parquet_dir}/urbanization_trends.parquet"
    
    # Load the datasets
    print(f"Loading data from {transitions_parquet}")
    transitions_df = pd.read_parquet(transitions_parquet)
    
    print(f"Loading data from {county_parquet}")
    county_df = pd.read_parquet(county_parquet)
    
    print(f"Loading data from {urbanization_parquet}")
    urbanization_df = pd.read_parquet(urbanization_parquet)
    
    # Create SmartDataframes with the data
    print("Creating semantic layer for transitions summary...")
    transitions_smart_df = SmartDataframe(
        transitions_df,
        config={"llm": llm, "name": "Land Use Transitions"}
    )
    
    # Create county-level transitions semantic layer
    print("Creating semantic layer for county transitions...")
    county_smart_df = SmartDataframe(
        county_df,
        config={"llm": llm, "name": "County Land Use Transitions"}
    )
    
    # Create urbanization trends semantic layer
    print("Creating semantic layer for urbanization trends...")
    urbanization_smart_df = SmartDataframe(
        urbanization_df,
        config={"llm": llm, "name": "Urbanization Trends"}
    )
    
    # Return the created SmartDataframes
    return {
        "transitions": transitions_smart_df,
        "county": county_smart_df,
        "urbanization": urbanization_smart_df
    } 