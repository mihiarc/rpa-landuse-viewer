#!/usr/bin/env python3
"""
Create semantic layers for RPA landuse data using the modular PandasAI implementation.

This script calls the following modules:
- src.db.region_repository.py
- src.db.land_use_repository.py
- src.db.analysis_repository.py
- query_duckdb.py

The base analysis creates the following datasets:
- land_use_parquet
- land_use_parquet_with_geometry
- land_use_parquet_with_geometry_and_crs
- land_use_parquet_with_geometry_and_crs_and_bounds
- land_use_parquet_with_geometry_and_crs_and_bounds_and_centroids

"""

import os
import argparse
from src.pandasai.layers import extract_data_from_duckdb, create_semantic_layers


def main():
    """Main function to extract data and create semantic layers."""
    parser = argparse.ArgumentParser(description="Create semantic layers for RPA landuse data")
    parser.add_argument(
        "--db-path", 
        type=str, 
        default="data/database/rpa.db",
        help="Path to the DuckDB database file"
    )
    parser.add_argument(
        "--output-dir", 
        type=str, 
        default="semantic_layers/base_analysis",
        help="Directory to save Parquet files"
    )
    parser.add_argument(
        "--org-path", 
        type=str, 
        default="rpa-landuse",
        help="Organization path prefix for PandasAI datasets"
    )
    parser.add_argument(
        "--skip-extraction", 
        action="store_true",
        help="Skip data extraction and use existing Parquet files"
    )
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Extract data from DuckDB if not skipped
    if not args.skip_extraction:
        print("Extracting data from DuckDB...")
        output_files = extract_data_from_duckdb(
            db_path=args.db_path,
            output_dir=args.output_dir
        )
        print(f"Data extracted to {args.output_dir}/")
    else:
        print(f"Skipping data extraction, using existing files in {args.output_dir}/")
    
    # Create semantic layers
    print("Creating semantic layers...")
    try:
        datasets = create_semantic_layers(
            parquet_dir=args.output_dir,
            org_path=args.org_path
        )
        print("Semantic layers created successfully!")
        print(f"Created the following datasets:")
        for name in datasets.keys():
            print(f"- {name}")
    except Exception as e:
        print(f"Error creating semantic layers: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main()) 