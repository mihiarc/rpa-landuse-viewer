"""
Database module for the RPA Land Use Viewer application.

This module provides database access and query capabilities.
"""

from .database import DBManager
from .base_repository import BaseRepository
from .land_use_repository import LandUseRepository
from .region_repository import RegionRepository
from .analysis_repository import AnalysisRepository
from .schema_manager import SchemaManager

__version__ = "1.0.0"
__all__ = [
    'DBManager',
    'BaseRepository',
    'LandUseRepository',
    'RegionRepository',
    'AnalysisRepository',
    'SchemaManager'
] 