from src.api.queries import LandUseQueries

def print_section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)

def format_acres(acres: float) -> str:
    """Format acres with proper scaling (input is in 100s of acres)."""
    return f"{acres * 100:,.2f}"

def analyze_ag_transitions(county_fips: str, county_name: str, scenario: str):
    """Analyze agricultural transitions in a specific county."""
    print_section(f"Agricultural Transitions Analysis - {county_name} ({county_fips}) - {scenario}")
    try:
        # Get all transitions
        results = LandUseQueries.analyze_county_transitions(
            county_fips, 2012, 2070, scenario, None
        )
        
        # Filter for ag transitions
        crop_to_pasture = next((r for r in results 
            if r['from_land_use'] == 'Crop' and r['to_land_use'] == 'Pasture'), None)
        pasture_to_crop = next((r for r in results 
            if r['from_land_use'] == 'Pasture' and r['to_land_use'] == 'Crop'), None)
        
        # Calculate net conversion
        crop_to_pasture_acres = crop_to_pasture['acres_changed'] if crop_to_pasture else 0
        pasture_to_crop_acres = pasture_to_crop['acres_changed'] if pasture_to_crop else 0
        net_change = pasture_to_crop_acres - crop_to_pasture_acres
        
        print(f"Crop to Pasture: {format_acres(crop_to_pasture_acres)} acres")
        print(f"Pasture to Crop: {format_acres(pasture_to_crop_acres)} acres")
        print(f"Net Change (+ means gain in cropland): {format_acres(net_change)} acres")
        
        if crop_to_pasture:
            print(f"\nCrop to Pasture is {crop_to_pasture['percentage_of_source_loss']:.1f}% "
                  f"of total crop land conversion")
        if pasture_to_crop:
            print(f"Pasture to Crop is {pasture_to_crop['percentage_of_source_loss']:.1f}% "
                  f"of total pasture land conversion")
    except Exception as e:
        print(f"Error: {e}")

def analyze_scenario_differences():
    """Compare land use changes between different scenarios."""
    scenarios = [
        "CNRM_CM5_rcp45_ssp1",
        "CNRM_CM5_rcp85_ssp5"
    ]
    land_use_types = ["Urban", "Crop", "Forest"]
    
    for land_use in land_use_types:
        print_section(f"Scenario Comparison for {land_use} Land (2012-2070)")
        try:
            results = LandUseQueries.compare_scenarios(
                2012, 2070, land_use, scenarios[0], scenarios[1]
            )
            
            for r in results:
                print(f"\nScenario: {r['scenario_name']}")
                print(f"Climate Model: {r['gcm']}")
                print(f"RCP: {r['rcp']}")
                print(f"SSP: {r['ssp']}")
                print(f"Net Change: {format_acres(r['net_change'])} acres")
                print(f"Annual Rate: {format_acres(r['annual_rate'])} acres/year")
        except Exception as e:
            print(f"Error: {e}")

def analyze_major_transitions():
    """Identify and analyze major land use transitions."""
    scenario = "CNRM_CM5_rcp45_ssp1"
    periods = [(2012, 2030), (2030, 2050), (2050, 2070)]
    
    for start_year, end_year in periods:
        print_section(f"Major Land Use Transitions ({start_year}-{end_year}) - {scenario}")
        try:
            results = LandUseQueries.major_transitions(start_year, end_year, scenario, 5)
            
            for r in results:
                print(f"\n{r['from_land_use']} to {r['to_land_use']}:")
                print(f"  Acres Changed: {format_acres(r['acres_changed'])}")
                print(f"  Percentage of All Changes: {r['percentage_of_all_changes']:.1f}%")
        except Exception as e:
            print(f"Error: {e}")

def check_data_quality():
    """Run data integrity checks."""
    scenario = "CNRM_CM5_rcp45_ssp1"
    
    print_section(f"Data Integrity Checks - {scenario}")
    try:
        results = LandUseQueries.check_data_integrity(scenario)
        
        print("\nArea Consistency Check:")
        if not results['area_inconsistencies']:
            print("No area inconsistencies found.")
        else:
            print("Found inconsistencies in total area over time:")
            for r in results['area_inconsistencies']:
                print(f"County {r['fips_code']}, Year {r['start_year']}: "
                      f"Difference of {format_acres(r['area_difference'])} acres")
        
        print("\nNegative Acres Check:")
        if not results['negative_acres']:
            print("No negative acre values found.")
        else:
            print("Found negative acre values:")
            for r in results['negative_acres']:
                print(f"County {r['fips_code']}: {r['from_land_use']} to {r['to_land_use']}, "
                      f"{format_acres(r['acres'])} acres ({r['start_year']}-{r['end_year']})")
    except Exception as e:
        print(f"Error: {e}")

def main():
    # Run agricultural transitions analysis
    scenario = "CNRM_CM5_rcp45_ssp1"
    print_section(f"Top 10 Counties by Crop to Pasture Conversion (2012-2070) - {scenario}")
    crop_to_pasture_counties = LandUseQueries.top_counties_by_change(
        2012, 2070, "Crop", scenario, 10, 'decrease'
    )
    
    print_section(f"Top 10 Counties by Pasture to Crop Conversion (2012-2070) - {scenario}")
    pasture_to_crop_counties = LandUseQueries.top_counties_by_change(
        2012, 2070, "Pasture", scenario, 10, 'decrease'
    )
    
    # Combine and deduplicate counties
    counties = {}
    for county in crop_to_pasture_counties + pasture_to_crop_counties:
        if county['fips_code'] not in counties:
            counties[county['fips_code']] = county
    
    print("\nDetailed Analysis of Agricultural Transitions:")
    # Analyze transitions for each county
    for county in counties.values():
        analyze_ag_transitions(county['fips_code'], county['county_name'], scenario)
    
    # Run additional analyses
    analyze_scenario_differences()
    analyze_major_transitions()
    check_data_quality()

if __name__ == "__main__":
    main() 