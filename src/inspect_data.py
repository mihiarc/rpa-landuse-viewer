import json
from pprint import pprint

def inspect_data():
    # Load the JSON file
    print("Loading JSON file...")
    with open("RDS-2023-0026/Data/county_landuse_projections_RPA.json", 'r') as f:
        data = json.load(f)
    
    # Get a sample scenario
    scenario_name = next(iter(data))
    print(f"\nSample scenario: {scenario_name}")
    
    # Get a sample year range
    year_range = next(iter(data[scenario_name]))
    print(f"Sample year range: {year_range}")
    
    # Get a sample county
    county = next(iter(data[scenario_name][year_range]))
    print(f"Sample county: {county}")
    
    # Print the county data structure
    print("\nSample county data structure:")
    pprint(data[scenario_name][year_range][county])

if __name__ == "__main__":
    inspect_data() 