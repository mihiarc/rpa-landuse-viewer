"""Example script demonstrating land use change visualizations."""

from src.visualization.land_use_viz import LandUseVisualization

def main():
    """Create and save example visualizations."""
    # Common parameters
    scenario = "CNRM_CM5_rcp45_ssp1"
    scenario2 = "CNRM_CM5_rcp85_ssp5"
    start_year = 2012
    end_year = 2070
    
    print("Creating visualizations...")
    
    # 1. Create county choropleth map for urban expansion
    print("Creating urban expansion map...")
    urban_map = LandUseVisualization.create_county_choropleth(
        start_year, end_year, "Urban", scenario
    )
    urban_map.save("outputs/urban_expansion_map.html")
    
    # 2. Create time series of all land use changes
    print("Creating time series plot...")
    time_series = LandUseVisualization.create_time_series(
        start_year, end_year, scenario
    )
    time_series.write_html("outputs/land_use_changes_time_series.html")
    
    # 3. Create Sankey diagram of major transitions
    print("Creating Sankey diagram...")
    sankey = LandUseVisualization.create_sankey_diagram(
        start_year, end_year, scenario
    )
    sankey.write_html("outputs/land_use_transitions_sankey.html")
    
    # 4. Create scenario comparisons for different land use types
    for land_use in ["Urban", "Forest", "Crop"]:
        print(f"Creating scenario comparison for {land_use}...")
        comparison = LandUseVisualization.create_scenario_comparison(
            start_year, end_year, land_use, scenario, scenario2
        )
        comparison.write_html(f"outputs/{land_use.lower()}_scenario_comparison.html")
    
    print("\nAll visualizations have been created in the outputs directory.")
    print("Open the HTML files in a web browser to view the interactive visualizations.")

if __name__ == "__main__":
    main() 