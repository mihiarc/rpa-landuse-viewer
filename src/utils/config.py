"""
Configuration utilities for the RPA Land Use Viewer.

This module provides functions for managing and retrieving configuration settings
for the application, including map settings, data paths, and visualization options.
"""

import json
import os
from typing import Dict, Any, List, Optional, Union
import yaml

# Default configuration values
DEFAULT_MAP_CONFIG = {
    "center": {"lat": 40.7128, "lon": -74.0060},  # New York City
    "zoom": 9,
    "mapbox_style": "carto-positron",
    "color_scale": "Viridis",
    "height": 600,
    "width": None,  # Set to None to use responsive width
    "map_type": "light"
}

DEFAULT_CHART_CONFIG = {
    "height": 400,
    "width": None,  # Set to None to use responsive width
    "color_scale": "Viridis",
    "template": "plotly",
    "show_grid": True,
    "title_font_size": 20,
    "axis_font_size": 14,
    "legend_font_size": 12,
    "margin": {"l": 50, "r": 20, "t": 50, "b": 50}
}

DEFAULT_DATA_CONFIG = {
    "data_dir": "data",
    "geo_dir": "data/geo",
    "cache_dir": "data/cache",
    "default_data_format": "csv",
    "encoding": "utf-8",
    "date_format": "%Y-%m-%d",
    "default_crs": "EPSG:4326"
}

DEFAULT_APP_CONFIG = {
    "app_name": "RPA Land Use Viewer",
    "debug": False,
    "theme": "light",
    "sidebar_width": 300,
    "default_page": "home",
    "cache_timeout": 3600,  # in seconds
    "max_upload_size_mb": 100
}

def _load_config_file(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from a file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Dictionary containing configuration values
    """
    if not os.path.exists(config_path):
        return {}
    
    file_ext = os.path.splitext(config_path)[1].lower()
    
    try:
        if file_ext == '.json':
            with open(config_path, 'r') as f:
                return json.load(f)
        elif file_ext in ['.yaml', '.yml']:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            raise ValueError(f"Unsupported configuration file format: {file_ext}")
    except Exception as e:
        print(f"Error loading configuration from {config_path}: {str(e)}")
        return {}

def get_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Get the complete application configuration.
    
    Args:
        config_path: Path to custom configuration file (optional)
        
    Returns:
        Dictionary containing the complete configuration
    """
    # Start with default configuration
    config = {
        "map": DEFAULT_MAP_CONFIG.copy(),
        "chart": DEFAULT_CHART_CONFIG.copy(),
        "data": DEFAULT_DATA_CONFIG.copy(),
        "app": DEFAULT_APP_CONFIG.copy()
    }
    
    # Load custom configuration if provided
    if config_path:
        custom_config = _load_config_file(config_path)
        
        # Update default configuration with custom values
        for section in config:
            if section in custom_config:
                config[section].update(custom_config[section])
    
    return config

def get_map_config() -> Dict[str, Any]:
    """
    Get map configuration settings.
    
    Returns:
        Dictionary containing map configuration values
    """
    return get_config()["map"]

def get_chart_config() -> Dict[str, Any]:
    """
    Get chart configuration settings.
    
    Returns:
        Dictionary containing chart configuration values
    """
    return get_config()["chart"]

def get_data_config() -> Dict[str, Any]:
    """
    Get data configuration settings.
    
    Returns:
        Dictionary containing data configuration values
    """
    return get_config()["data"]

def get_app_config() -> Dict[str, Any]:
    """
    Get application configuration settings.
    
    Returns:
        Dictionary containing application configuration values
    """
    return get_config()["app"]

def update_config(section: str, key: str, value: Any) -> None:
    """
    Update a configuration value in memory.
    
    Args:
        section: Configuration section ('map', 'chart', 'data', or 'app')
        key: Configuration key to update
        value: New value for the configuration key
    """
    config = get_config()
    
    if section not in config:
        raise ValueError(f"Invalid configuration section: {section}")
    
    config[section][key] = value

def save_config(config: Dict[str, Any], config_path: str) -> None:
    """
    Save configuration to a file.
    
    Args:
        config: Configuration dictionary to save
        config_path: Path to save the configuration file
    """
    file_ext = os.path.splitext(config_path)[1].lower()
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    
    try:
        if file_ext == '.json':
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
        elif file_ext in ['.yaml', '.yml']:
            with open(config_path, 'w') as f:
                yaml.dump(config, f, default_flow_style=False)
        else:
            raise ValueError(f"Unsupported configuration file format: {file_ext}")
    except Exception as e:
        print(f"Error saving configuration to {config_path}: {str(e)}")

def generate_default_config(config_path: str) -> None:
    """
    Generate and save a default configuration file.
    
    Args:
        config_path: Path to save the default configuration file
    """
    default_config = {
        "map": DEFAULT_MAP_CONFIG,
        "chart": DEFAULT_CHART_CONFIG,
        "data": DEFAULT_DATA_CONFIG,
        "app": DEFAULT_APP_CONFIG
    }
    
    save_config(default_config, config_path)

def get_setting(section: str, key: str, default: Any = None) -> Any:
    """
    Get a specific configuration setting.
    
    Args:
        section: Configuration section ('map', 'chart', 'data', or 'app')
        key: Configuration key to retrieve
        default: Default value if the setting is not found
        
    Returns:
        The configuration value or the default value if not found
    """
    config = get_config()
    
    if section not in config:
        return default
    
    return config[section].get(key, default)

def get_map_style(map_type: Optional[str] = None) -> str:
    """
    Get the mapbox style based on the map type.
    
    Args:
        map_type: Type of map ('light', 'dark', 'satellite', 'streets', 'outdoors')
        
    Returns:
        Mapbox style string
    """
    if map_type is None:
        map_type = get_setting("map", "map_type", "light")
    
    style_mapping = {
        "light": "carto-positron",
        "dark": "carto-darkmatter",
        "satellite": "mapbox://styles/mapbox/satellite-streets-v11",
        "streets": "mapbox://styles/mapbox/streets-v11",
        "outdoors": "mapbox://styles/mapbox/outdoors-v11"
    }
    
    return style_mapping.get(map_type, "carto-positron")

def get_color_scale(scale_name: Optional[str] = None, section: str = "chart") -> List[str]:
    """
    Get color scale based on the scale name.
    
    Args:
        scale_name: Name of the color scale
        section: Configuration section to use ('map' or 'chart')
        
    Returns:
        List of color strings or scale name
    """
    if scale_name is None:
        scale_name = get_setting(section, "color_scale", "Viridis")
    
    # Return the scale name as is since Plotly can handle named scales
    return scale_name

def get_data_path(relative_path: str = "") -> str:
    """
    Get the absolute path to a data file or directory.
    
    Args:
        relative_path: Relative path within the data directory
        
    Returns:
        Absolute path to the data file or directory
    """
    data_dir = get_setting("data", "data_dir", "data")
    return os.path.join(os.getcwd(), data_dir, relative_path)

def get_geo_path(relative_path: str = "") -> str:
    """
    Get the absolute path to a geographic data file or directory.
    
    Args:
        relative_path: Relative path within the geo directory
        
    Returns:
        Absolute path to the geographic data file or directory
    """
    geo_dir = get_setting("data", "geo_dir", "data/geo")
    return os.path.join(os.getcwd(), geo_dir, relative_path)

def get_cache_path(relative_path: str = "") -> str:
    """
    Get the absolute path to a cache file or directory.
    
    Args:
        relative_path: Relative path within the cache directory
        
    Returns:
        Absolute path to the cache file or directory
    """
    cache_dir = get_setting("data", "cache_dir", "data/cache")
    
    # Create cache directory if it doesn't exist
    os.makedirs(os.path.join(os.getcwd(), cache_dir), exist_ok=True)
    
    return os.path.join(os.getcwd(), cache_dir, relative_path) 