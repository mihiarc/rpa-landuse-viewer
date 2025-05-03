#!/usr/bin/env python3
"""
Regional Data Query Example.

This script demonstrates how to query the regional aggregation data
using the RegionRepository methods.
"""

import sys
import argparse
import logging
import pandas as pd
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.db.region_repository import RegionRepository

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Query regional land use transition data"
    )
    parser.add_argument(
        "--scenario", 
        type=int, 
        default=1,
        help="Scenario ID to query"
    )
    parser.add_argument(
        "--time-step", 
        type=int,
        default=None,
        help="Time step ID to query (optional)"
    )
    parser.add_argument(
        "--region", 
        type=str,
        help="Region name to filter by (optional)"
    )
    parser.add_argument(
        "--subregion", 
        type=str,
        help="Subregion name to filter by (optional)"
    )
    parser.add_argument(
        "--state", 
        type=str,
        help="State name to filter by (optional)"
    )
    parser.add_argument(
        "--level", 
        choices=["region", "subregion", "state"],
        default="region",
        help="Level of aggregation to query"
    )
    return parser.parse_args()

def display_summary(df, level):
    """Display summary of the data."""
    if df.empty:
        print("No data found for the specified query parameters.")
        return
    
    # Display basic info
    print(f"\nFound {len(df)} records at {level} level")
    print(f"Scenario: {df['scenario_name'].iloc[0]}")
    
    # Sum up areas by level
    if level == "region":
        summary = df.groupby(['region', 'from_land_use', 'to_land_use'])['total_area'].sum().reset_index()
        print("\nRegional transitions (top 10):")
        print(summary.sort_values('total_area', ascending=False).head(10))
        
        # Total by region
        region_total = df.groupby('region')['total_area'].sum().reset_index()
        print("\nTotal area by region:")
        print(region_total)
        
    elif level == "subregion":
        summary = df.groupby(['region', 'subregion', 'from_land_use', 'to_land_use'])['total_area'].sum().reset_index()
        print("\nSubregional transitions (top 10):")
        print(summary.sort_values('total_area', ascending=False).head(10))
        
        # Total by subregion
        subregion_total = df.groupby(['region', 'subregion'])['total_area'].sum().reset_index()
        print("\nTotal area by subregion:")
        print(subregion_total)
        
    elif level == "state":
        summary = df.groupby(['state_name', 'from_land_use', 'to_land_use'])['total_area'].sum().reset_index()
        print("\nState transitions (top 10):")
        print(summary.sort_values('total_area', ascending=False).head(10))
        
        # Total by state
        state_total = df.groupby('state_name')['total_area'].sum().reset_index()
        print("\nTotal area by state (top 10):")
        print(state_total.sort_values('total_area', ascending=False).head(10))
    
    # Total by land use transition type
    transition_total = df.groupby(['from_land_use', 'to_land_use'])['total_area'].sum().reset_index()
    print("\nTop 10 transition types:")
    print(transition_total.sort_values('total_area', ascending=False).head(10))

def main():
    """Main function to query regional data."""
    args = parse_args()
    
    logger.info(f"Querying {args.level}-level data for scenario {args.scenario}")
    
    try:
        # Query data based on the selected level
        if args.level == "region":
            df = RegionRepository.get_region_transitions(
                scenario_id=args.scenario,
                time_step_id=args.time_step,
                region=args.region
            )
        elif args.level == "subregion":
            df = RegionRepository.get_subregion_transitions(
                scenario_id=args.scenario,
                time_step_id=args.time_step,
                region=args.region,
                subregion=args.subregion
            )
        elif args.level == "state":
            df = RegionRepository.get_state_transitions(
                scenario_id=args.scenario,
                time_step_id=args.time_step,
                state_name=args.state,
                region=args.region,
                subregion=args.subregion
            )
        
        # Display summary of the data
        display_summary(df, args.level)
        
    except Exception as e:
        logger.error(f"Error querying regional data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main() 