import json
import pandas as pd
import numpy as np
from pathlib import Path

def load_json_data(file_path):
    """Load JSON file into Python."""
    print(f"Loading JSON file from {file_path}...")
    with open(file_path, 'r') as f:
        data = json.load(f)
    print("\nData structure loaded successfully!")
    return data

def extract_end_year(year_range):
    """Extract the end year from a year range string (e.g., '2012-2020' -> 2020)."""
    return int(year_range.split('-')[1])

def convert_matrix_to_df(matrix_data):
    """Convert the matrix data to a DataFrame in long format."""
    # Create mapping for column names
    col_map = {
        'cr': 'Crop',
        'ps': 'Pasture',
        'rg': 'Range',
        'fr': 'Forest',
        'ur': 'Urban',
        't1': 'Total'  # Total row sum
    }
    
    # Create mapping for row names
    row_map = {
        'cr': 'Crop',
        'ps': 'Pasture',
        'rg': 'Range',
        'fr': 'Forest',
        'ur': 'Urban',
        't2': 'Total'  # Total column sum
    }
    
    rows = []
    for row in matrix_data:
        from_type = row_map[row['_row']]
        if from_type != 'Total':  # Skip the total row
            for col, value in row.items():
                if col != '_row' and col != 't1':  # Skip the row identifier and total
                    to_type = col_map[col]
                    rows.append({
                        'From': from_type,
                        'To': to_type,
                        'Acres': float(value)
                    })
    
    return pd.DataFrame(rows)

def convert_to_dataframe(data):
    """Convert nested JSON structure to a pandas DataFrame."""
    print("Converting nested data structure to DataFrame...")
    all_data = []
    
    # Iterate through the nested structure
    for scenario_name, scenario_data in data.items():
        print(f"Processing scenario: {scenario_name}")
        
        for year_range, year_data in scenario_data.items():
            year = extract_end_year(year_range)
            print(f"Processing year range: {year_range} (end year: {year})")
            
            for fips, county_data in year_data.items():
                # Convert the matrix data to a DataFrame
                df = convert_matrix_to_df(county_data)
                
                # Add metadata columns
                df['Scenario'] = scenario_name
                df['Year'] = year
                df['YearRange'] = year_range
                df['FIPS'] = fips
                
                all_data.append(df)
                
                # Print progress for the first few counties
                if len(all_data) <= 3:
                    print(f"\nSample data for FIPS {fips}:")
                    print(df.head(2))
                    print(f"Shape: {df.shape}")
                    print()
    
    # Combine all data into a single DataFrame
    print("Concatenating all data...")
    final_df = pd.concat(all_data, ignore_index=True)
    
    # Remove any rows where Acres is 0
    print("Removing zero-acre transitions...")
    final_df = final_df[final_df['Acres'] > 0]
    
    return final_df

def save_to_parquet(df, output_path):
    """Save DataFrame to Parquet format."""
    print(f"Saving data to {output_path}...")
    df.to_parquet(output_path, index=False)
    print(f"File saved successfully! Size: {Path(output_path).stat().st_size / (1024*1024):.1f} MB")

def main():
    # Set up paths
    input_file = "RDS-2023-0026/Data/county_landuse_projections_RPA.json"
    output_file = "rpa_landuse_data.parquet"
    
    # Create output directory if it doesn't exist
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    # Process the data
    data = load_json_data(input_file)
    df = convert_to_dataframe(data)
    save_to_parquet(df, output_file)
    
    print("\nData conversion completed successfully!")
    print(f"Output saved to: {output_file}")
    print("\nDataFrame Info:")
    print(df.info())
    print("\nSample of unique values:")
    print("\nScenarios:", df['Scenario'].nunique())
    print("Years:", sorted(df['Year'].unique()))
    print("Number of counties:", df['FIPS'].nunique())
    print("\nAcres summary:")
    print(df['Acres'].describe())
    
    # Print transition summary
    print("\nTransition summary (average acres per transition type):")
    summary = df.groupby(['From', 'To'])['Acres'].mean().reset_index()
    summary = summary.pivot(index='From', columns='To', values='Acres')
    print(summary.round(2))

if __name__ == "__main__":
    main() 