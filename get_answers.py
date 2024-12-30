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

def main():
    scenario = "CNRM_CM5_rcp45_ssp1"

    # First find counties with significant ag transitions
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

if __name__ == "__main__":
    main() 