"""
Data processing utilities for the RPA Land Use Viewer.

This module provides functions for loading, filtering, and transforming land use data.
"""

import pandas as pd
import os
from typing import Dict, List, Optional, Tuple, Union
import geopandas as gpd

def load_data(data_path: str) -> pd.DataFrame:
    """
    Load land use data from the specified path.
    
    Args:
        data_path: Path to the data file (CSV, Excel, or other pandas-supported format)
        
    Returns:
        DataFrame containing the land use data
    """
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file not found: {data_path}")
    
    # Determine file type and load accordingly
    file_ext = os.path.splitext(data_path)[1].lower()
    
    if file_ext == '.csv':
        df = pd.read_csv(data_path)
    elif file_ext in ['.xlsx', '.xls']:
        df = pd.read_excel(data_path)
    elif file_ext == '.parquet':
        df = pd.read_parquet(data_path)
    elif file_ext == '.feather':
        df = pd.read_feather(data_path)
    elif file_ext in ['.json', '.geojson']:
        df = gpd.read_file(data_path)
    else:
        raise ValueError(f"Unsupported file format: {file_ext}")
    
    return df

def filter_data(
    df: pd.DataFrame,
    scenario: Optional[str] = None,
    year: Optional[Union[int, str]] = None,
    state: Optional[Union[str, List[str]]] = None,
    county: Optional[Union[str, List[str]]] = None,
    land_use_categories: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Filter the land use data based on specified criteria.
    
    Args:
        df: DataFrame containing the land use data
        scenario: Scenario name to filter by
        year: Year to filter by
        state: State(s) to filter by
        county: County(ies) to filter by
        land_use_categories: Land use categories to include
        
    Returns:
        Filtered DataFrame
    """
    filtered_df = df.copy()
    
    # Apply filters if provided
    if scenario is not None and 'scenario' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['scenario'] == scenario]
    
    if year is not None and 'year' in filtered_df.columns:
        # Convert year to integer if it's a string
        if isinstance(year, str):
            try:
                year = int(year)
            except ValueError:
                pass  # Keep as string if conversion fails
        filtered_df = filtered_df[filtered_df['year'] == year]
    
    if state is not None and 'state' in filtered_df.columns:
        if isinstance(state, str):
            state = [state]
        filtered_df = filtered_df[filtered_df['state'].isin(state)]
    
    if county is not None and 'county' in filtered_df.columns:
        if isinstance(county, str):
            county = [county]
        filtered_df = filtered_df[filtered_df['county'].isin(county)]
    
    if land_use_categories is not None and 'land_use_category' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['land_use_category'].isin(land_use_categories)]
    
    return filtered_df

def aggregate_data(
    df: pd.DataFrame,
    group_by: List[str],
    agg_columns: Dict[str, str] = None,
    include_pct: bool = False
) -> pd.DataFrame:
    """
    Aggregate data by specified grouping columns.
    
    Args:
        df: DataFrame to aggregate
        group_by: List of columns to group by
        agg_columns: Dictionary of column names and aggregation functions
        include_pct: Whether to include percentage columns for numeric fields
        
    Returns:
        Aggregated DataFrame
    """
    # Ensure all group_by columns exist
    for col in group_by:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame")
    
    # Default aggregation is sum for numeric columns
    if agg_columns is None:
        agg_columns = {}
        for col in df.select_dtypes(include=['number']).columns:
            if col not in group_by:
                agg_columns[col] = 'sum'
    
    # Perform aggregation
    aggregated = df.groupby(group_by).agg(agg_columns).reset_index()
    
    # Calculate percentages if requested
    if include_pct:
        numeric_cols = aggregated.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            if col not in group_by:
                total = aggregated[col].sum()
                if total > 0:  # Avoid division by zero
                    pct_col = f"{col}_pct"
                    aggregated[pct_col] = (aggregated[col] / total) * 100
    
    return aggregated

def calculate_change(
    df: pd.DataFrame,
    value_col: str,
    time_col: str = 'year',
    group_by: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Calculate changes in values over time.
    
    Args:
        df: DataFrame containing time series data
        value_col: Column containing the values to calculate changes for
        time_col: Column containing time information (typically 'year')
        group_by: Additional columns to group by when calculating changes
        
    Returns:
        DataFrame with additional columns for absolute and percentage changes
    """
    if group_by is None:
        group_by = []
    
    # Ensure required columns exist
    for col in [value_col, time_col] + group_by:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame")
    
    # Sort by time column
    result_df = df.sort_values(group_by + [time_col])
    
    # Calculate changes within each group
    if group_by:
        result_df[f"{value_col}_prev"] = result_df.groupby(group_by)[value_col].shift(1)
    else:
        result_df[f"{value_col}_prev"] = result_df[value_col].shift(1)
    
    # Calculate absolute and percentage changes
    result_df[f"{value_col}_change"] = result_df[value_col] - result_df[f"{value_col}_prev"]
    result_df[f"{value_col}_change_pct"] = (
        (result_df[value_col] / result_df[f"{value_col}_prev"] - 1) * 100
        if result_df[f"{value_col}_prev"].min() > 0  # Only if no zeros
        else float('nan')
    )
    
    # Replace NaN values in the change columns for the first entries
    result_df[f"{value_col}_change"] = result_df[f"{value_col}_change"].fillna(0)
    result_df[f"{value_col}_change_pct"] = result_df[f"{value_col}_change_pct"].fillna(0)
    
    return result_df

def get_unique_values(df: pd.DataFrame, column: str) -> List:
    """
    Get unique values from a DataFrame column.
    
    Args:
        df: DataFrame to extract values from
        column: Column name to get unique values from
        
    Returns:
        List of unique values sorted alphabetically/numerically
    """
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")
    
    values = sorted(df[column].unique())
    return values

def get_summary_stats(
    df: pd.DataFrame,
    numeric_cols: Optional[List[str]] = None
) -> Dict[str, Dict[str, float]]:
    """
    Calculate summary statistics for numeric columns.
    
    Args:
        df: DataFrame to analyze
        numeric_cols: List of numeric columns to include (if None, use all numeric columns)
        
    Returns:
        Dictionary of column names and their statistics
    """
    if numeric_cols is None:
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    else:
        # Ensure all specified columns exist and are numeric
        for col in numeric_cols:
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found in DataFrame")
            if not pd.api.types.is_numeric_dtype(df[col]):
                raise ValueError(f"Column '{col}' is not numeric")
    
    stats = {}
    for col in numeric_cols:
        col_stats = {
            'min': df[col].min(),
            'max': df[col].max(),
            'mean': df[col].mean(),
            'median': df[col].median(),
            'sum': df[col].sum(),
            'count': df[col].count()
        }
        stats[col] = col_stats
    
    return stats

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names by converting to lowercase and replacing spaces with underscores.
    
    Args:
        df: DataFrame to normalize
        
    Returns:
        DataFrame with normalized column names
    """
    df = df.copy()
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    return df 