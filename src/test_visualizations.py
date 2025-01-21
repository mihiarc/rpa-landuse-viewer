"""Script to demonstrate land use change visualizations."""

import os
from src.visualization.land_use_viz import LandUseVisualization

def main():
    """Run visualization examples."""
    # Test parameters
    scenario = {
        'name': 'rcp85_ssp3',  # High emissions, low growth
        'climate_model': 'NorESM1_M',  # Middle model
        'start_year': 2020,
        'end_year': 2050
    }
    
    # Create output directory if it doesn't exist
    os.makedirs('outputs', exist_ok=True)
    
    # 1. Scenario Ranking Plot
    print("Creating scenario ranking plot...")
    fig = LandUseVisualization.create_scenario_ranking_plot(
        target_year=scenario['end_year'],
        climate_model=scenario['climate_model']
    )
    fig.write_html('outputs/scenario_ranking.html')
    
    # 2. Time Series Plot
    print("Creating time series plot...")
    fig = LandUseVisualization.create_time_series(
        start_year=scenario['start_year'],
        end_year=scenario['end_year'],
        scenario_name=scenario['name']
    )
    fig.write_html('outputs/time_series.html')
    
    # 3. Sankey Diagram
    print("Creating Sankey diagram...")
    fig = LandUseVisualization.create_sankey_diagram(
        start_year=scenario['start_year'],
        end_year=scenario['end_year'],
        scenario_name=scenario['name']
    )
    fig.write_html('outputs/sankey_diagram.html')
    
    # 4. County Choropleth
    print("Creating county choropleth map...")
    m = LandUseVisualization.create_county_choropleth(
        start_year=scenario['start_year'],
        end_year=scenario['end_year'],
        land_use_type='Forest',
        scenario_name=scenario['name']
    )
    m.save('outputs/county_choropleth.html')
    
    print("\nVisualization files have been created in the 'outputs' directory:")
    print("1. outputs/scenario_ranking.html")
    print("2. outputs/time_series.html")
    print("3. outputs/sankey_diagram.html")
    print("4. outputs/county_choropleth.html")

if __name__ == '__main__':
    main() 