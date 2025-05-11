#!/usr/bin/env python3
"""
Command-line interface for the RPA Land Use Viewer package.
"""

import argparse
import sys
from pathlib import Path

def run_app(args):
    """Run the Streamlit app."""
    import streamlit.web.bootstrap as bootstrap
    from streamlit.web.bootstrap import StreamlitApp

    # Find app.py in the package
    package_dir = Path(__file__).parent.parent.parent
    app_path = package_dir / "app.py"
    
    if not app_path.exists():
        print(f"Error: Could not find app.py at {app_path}")
        sys.exit(1)
    
    bootstrap.run(str(app_path), "", args, flag_options={})

def create_semantic_layers(args):
    """Create semantic layers for data analysis."""
    from rpa_landuse.pandasai.layers import extract_data_from_duckdb, create_semantic_layers
    import os
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    if not args.skip_extraction:
        extract_data_from_duckdb(
            db_path=args.db_path,
            output_dir=args.output_dir,
            scenario_ids=args.scenario_ids if not args.all_scenarios else None
        )
    
    create_semantic_layers(
        data_dir=args.output_dir,
        org_prefix=args.org_path
    )

def initialize_db(args):
    """Initialize the database schema and optionally import data."""
    from rpa_landuse.db.schema_manager import SchemaManager
    
    SchemaManager.initialize_database()
    SchemaManager.ensure_indexes()
    
    if args.import_data and args.data_path:
        from rpa_landuse.db.import_landuse_data import process_transitions
        process_transitions(args.data_path)

def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="RPA Land Use - Tools for analyzing RPA land use projections"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # App command
    app_parser = subparsers.add_parser("app", help="Run the Streamlit app")
    
    # Semantic layers command
    semantic_parser = subparsers.add_parser(
        "semantic-layers", help="Create semantic layers for data analysis"
    )
    semantic_parser.add_argument(
        "--db-path", 
        type=str, 
        default="data/database/rpa.db",
        help="Path to the DuckDB database file"
    )
    semantic_parser.add_argument(
        "--output-dir", 
        type=str, 
        default="semantic_layers/base_analysis",
        help="Directory to save Parquet files"
    )
    semantic_parser.add_argument(
        "--org-path", 
        type=str, 
        default="rpa-landuse",
        help="Organization path prefix for PandasAI datasets"
    )
    semantic_parser.add_argument(
        "--skip-extraction", 
        action="store_true",
        help="Skip data extraction and use existing Parquet files"
    )
    semantic_parser.add_argument(
        "--scenario-ids",
        type=int,
        nargs="+",
        default=[21, 22, 23, 24, 25],
        help="List of scenario IDs to include (default: 21-25, ensemble mean and RPA integrated scenarios)"
    )
    semantic_parser.add_argument(
        "--all-scenarios",
        action="store_true",
        help="Process all scenarios instead of only the default ensemble and RPA scenarios"
    )
    
    # DB init command
    db_parser = subparsers.add_parser("init-db", help="Initialize the database")
    db_parser.add_argument(
        "--import-data",
        action="store_true",
        help="Import data after initialization"
    )
    db_parser.add_argument(
        "--data-path",
        type=str,
        default="data/raw/county_landuse_projections_RPA.json",
        help="Path to the data file to import"
    )
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        return
    
    if args.command == "app":
        run_app([])
    elif args.command == "semantic-layers":
        create_semantic_layers(args)
    elif args.command == "init-db":
        initialize_db(args)

if __name__ == "__main__":
    main() 