#!/usr/bin/env python3
"""
Query semantic layers for RPA landuse data using the modular PandasAI implementation.
"""

import argparse
from src.pandasai.query import (
    query_transitions, 
    query_county_transitions, 
    query_urbanization_trends, 
    multi_dataset_query
)


def main():
    """Main function to query the semantic layers."""
    parser = argparse.ArgumentParser(description="Query semantic layers for RPA landuse data")
    parser.add_argument(
        "--dataset", 
        type=str, 
        choices=["transitions", "county", "urbanization", "multi"],
        default="transitions",
        help="Dataset to query"
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
        elif args.dataset == "county":
            print(f"Querying county-level transitions dataset: '{args.query}'")
            response = query_county_transitions(args.query, parquet_dir=args.parquet_dir)
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
        return 1
    
    return 0


def example_queries():
    """Print some example queries for each dataset."""
    print("Example queries for each dataset:")
    print("\nTransitions dataset:")
    print("  - Which scenario shows the most conversion from Forest to Urban land?")
    print("  - What is the total area converted from Cropland to Urban across all scenarios?")
    print("  - Create a bar chart showing the top 5 land use transitions by area")
    
    print("\nCounty dataset:")
    print("  - Which county shows the highest agricultural land conversion to urban areas?")
    print("  - How does forest loss in New York compare to Connecticut?")
    print("  - Show the counties with the most diverse land use transitions")
    
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