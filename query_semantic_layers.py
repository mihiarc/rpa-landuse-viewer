#!/usr/bin/env python3
"""
Query semantic layers for RPA landuse data using the modular PandasAI implementation.
"""

import argparse
import traceback
from src.pandasai.query import (
    query_transitions, 
    query_transitions_changes,
    query_to_urban,
    query_from_forest,
    query_county_transitions, 
    query_county_transitions_changes,
    query_county_to_urban,
    query_county_from_forest,
    query_urbanization_trends, 
    multi_dataset_query
)


def main():
    """Main function to query the semantic layers."""
    parser = argparse.ArgumentParser(description="Query semantic layers for RPA landuse data")
    parser.add_argument(
        "--dataset", 
        type=str, 
        choices=[
            "transitions", "transitions_changes", 
            "to_urban", "from_forest",
            "county", "county_changes", 
            "county_to_urban", "county_from_forest",
            "urbanization", "multi"
        ],
        default="transitions_changes",
        help="Dataset to query (default: transitions_changes)"
    )
    parser.add_argument(
        "--query", 
        type=str, 
        required=True,
        help="Natural language query"
    )
    parser.add_argument(
        "--parquet-dir", 
        type=str, 
        default="land_use_parquet",
        help="Directory containing parquet files"
    )
    
    args = parser.parse_args()
    
    try:
        # Query the appropriate dataset
        if args.dataset == "transitions":
            print(f"Querying transitions dataset: '{args.query}'")
            response = query_transitions(args.query, parquet_dir=args.parquet_dir)
        elif args.dataset == "transitions_changes":
            print(f"Querying transitions dataset (changes only): '{args.query}'")
            response = query_transitions_changes(args.query, parquet_dir=args.parquet_dir)
        elif args.dataset == "to_urban":
            print(f"Querying transitions TO Urban dataset: '{args.query}'")
            response = query_to_urban(args.query, parquet_dir=args.parquet_dir)
        elif args.dataset == "from_forest":
            print(f"Querying transitions FROM Forest dataset: '{args.query}'")
            response = query_from_forest(args.query, parquet_dir=args.parquet_dir)
        elif args.dataset == "county":
            print(f"Querying county-level transitions dataset: '{args.query}'")
            response = query_county_transitions(args.query, parquet_dir=args.parquet_dir)
        elif args.dataset == "county_changes":
            print(f"Querying county-level transitions dataset (changes only): '{args.query}'")
            response = query_county_transitions_changes(args.query, parquet_dir=args.parquet_dir)
        elif args.dataset == "county_to_urban":
            print(f"Querying county-level transitions TO Urban dataset: '{args.query}'")
            response = query_county_to_urban(args.query, parquet_dir=args.parquet_dir)
        elif args.dataset == "county_from_forest":
            print(f"Querying county-level transitions FROM Forest dataset: '{args.query}'")
            response = query_county_from_forest(args.query, parquet_dir=args.parquet_dir)
        elif args.dataset == "urbanization":
            print(f"Querying urbanization trends dataset: '{args.query}'")
            response = query_urbanization_trends(args.query, parquet_dir=args.parquet_dir)
        elif args.dataset == "multi":
            print(f"Querying multiple datasets: '{args.query}'")
            response = multi_dataset_query(args.query, parquet_dir=args.parquet_dir)
        
        # Print the response
        print("\nResponse:")
        print(response)
        
    except Exception as e:
        print(f"Error querying dataset: {e}")
        print("\nFull traceback:")
        traceback.print_exc()
        return 1
    
    return 0


def example_queries():
    """Print some example queries for each dataset."""
    print("Example queries for each dataset:")
    print("\nTransitions dataset (all data):")
    print("  - Which scenario shows the most conversion from Forest to Urban land?")
    print("  - What is the total area converted from Cropland to Urban across all scenarios?")
    print("  - Create a bar chart showing the top 5 land use transitions by area")
    
    print("\nTransitions dataset (changes only):")
    print("  - What is the most common land use change?")
    print("  - Create a bar chart showing the top 5 land use changes by area")
    print("  - How much Forest was converted to Urban land use?")
    
    print("\nTransitions TO Urban dataset:")
    print("  - Which land use type contributes most to urban expansion?")
    print("  - Create a pie chart showing the proportion of different land uses converted to Urban")
    print("  - How does Forest to Urban conversion compare across different scenarios?")
    
    print("\nTransitions FROM Forest dataset:")
    print("  - What happens to most of the Forest that is lost?")
    print("  - Create a bar chart showing where Forest land is being converted to")
    print("  - Which decade shows the highest Forest loss?")
    
    print("\nCounty dataset (all data):")
    print("  - Which county shows the highest agricultural land conversion to urban areas?")
    print("  - How does forest loss in New York compare to Connecticut?")
    print("  - Show the counties with the most diverse land use transitions")
    
    print("\nCounty dataset (changes only):")
    print("  - Which county has the most cropland to urban conversion?")
    print("  - Show a chart of counties with the highest forest to urban conversion")
    print("  - Identify counties with significant rangeland to cropland transitions")
    
    print("\nCounty TO Urban dataset:")
    print("  - Which counties are experiencing the most urbanization?")
    print("  - What is the primary source of new urban land in coastal counties?")
    print("  - Create a chart of the top 10 counties with the most forest to urban conversion")
    
    print("\nCounty FROM Forest dataset:")
    print("  - Which counties are losing the most forest?")
    print("  - In which counties is forest mostly converted to urban rather than agriculture?")
    print("  - Create a map showing forest loss by county")
    
    print("\nUrbanization trends dataset:")
    print("  - Create a line chart showing forest to urban conversion over time")
    print("  - Which scenario has the highest cropland to urban conversion rate?")
    print("  - Compare pasture to urban conversion across all scenarios")
    
    print("\nMulti-dataset queries:")
    print("  - Compare Forest to Urban conversion across different time periods")
    print("  - Which counties have urbanization trends that match the overall pattern?")
    print("  - Show the relationship between county-level transitions and overall scenario trends")


if __name__ == "__main__":
    # Check if no arguments were provided
    import sys
    if len(sys.argv) == 1:
        # Print the help message and example queries
        example_queries()
        print("\nUse --help for more information on how to use this script.")
        sys.exit(0)
    
    # Otherwise, run the main function
    exit(main()) 