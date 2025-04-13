"""Script to demonstrate urban expansion visualization."""

import os
import geopandas as gpd
import matplotlib.pyplot as plt
from src.db.queries import LandUseQueries
import pandas as pd

def main():
    """Create and display urban expansion visualization."""
    # Parameters
    scenario = "CNRM_CM5_rcp45_ssp1"
    start_year = 2020
    end_year = 2050
    
    print("Creating static urban expansion map...")
    
    # Create outputs directory if it doesn't exist
    os.makedirs('outputs', exist_ok=True)
    
    # Get county-level changes
    counties = LandUseQueries.top_counties_by_change(
        start_year, end_year, "Urban", scenario, 
        limit=3000,
        change_type='increase'
    )
    
    # Convert to DataFrame
    df = pd.DataFrame(counties)
    
    # Read GeoJSON file
    gdf = gpd.read_file('data/county.geo.json')
    
    # Merge data with GeoJSON
    gdf = gdf.merge(df, left_on='GEOID10', right_on='fips_code', how='left')
    
    # Create figure and axis
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    
    # Plot choropleth
    gdf.plot(
        column='net_change',
        cmap='RdYlBu',
        legend=True,
        legend_kwds={'label': 'Net Change in Urban Land (acres)'},
        missing_kwds={'color': 'lightgrey'},
        ax=ax
    )
    
    # Customize the map
    ax.axis('off')
    plt.title(f"Urban Land Expansion ({start_year}-{end_year})")
    
    # Save the map
    output_file = "outputs/urban_expansion_static.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nStatic map has been saved to {output_file}")

if __name__ == "__main__":
    main() 