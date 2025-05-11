"""
Command-line functionality for the RPA Land Use Viewer package.
"""

from rpa_landuse.commands.create_semantic_layers import main as create_semantic_layers_main
from rpa_landuse.commands.create_regional_layers import main as create_regional_layers_main
from rpa_landuse.commands.query_duckdb import main as query_duckdb_main
from rpa_landuse.commands.query_pandasai import main as query_pandasai_main

__all__ = [
    'create_semantic_layers_main',
    'create_regional_layers_main',
    'query_duckdb_main',
    'query_pandasai_main'
] 