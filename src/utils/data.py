"""
Data utilities for the RPA Land Use Viewer.

This module provides functions for loading, preprocessing, and manipulating
land use data for analysis and visualization.
"""

import pandas as pd
import geopandas as gpd
import numpy as np
import os
from typing import Dict, List, Optional, Tuple, Union, Any
import json
from .config import get_data_config

def load_geojson(filepath: str) -> Dict[str, Any]:
    """
    Load GeoJSON data from file.
    
    Args:
        filepath: Path to GeoJSON file
        
    Returns:
        Dictionary containing GeoJSON data
    """
    with open(filepath, 'r') as f:
        return json.load(f)

def load_geodataframe(filepath: str) -> gpd.GeoDataFrame:
    """
    Load geographic data into a GeoDataFrame.
    
    Args:
        filepath: Path to geographic data file (.shp, .geojson, etc.)
        
    Returns:
        GeoDataFrame containing the geographic data
    """
    return gpd.read_file(filepath)

def load_data(filepath: str, **kwargs) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Load data from various file formats into a DataFrame or GeoDataFrame.
    
    Args:
        filepath: Path to data file
        **kwargs: Additional keyword arguments for pandas/geopandas read functions
        
    Returns:
        DataFrame or GeoDataFrame containing the data
    """
    file_ext = os.path.splitext(filepath)[1].lower()
    
    if file_ext in ['.shp', '.geojson']:
        return load_geodataframe(filepath)
    elif file_ext == '.csv':
        return pd.read_csv(filepath, **kwargs)
    elif file_ext == '.xlsx' or file_ext == '.xls':
        return pd.read_excel(filepath, **kwargs)
    elif file_ext == '.json':
        return pd.read_json(filepath, **kwargs)
    elif file_ext == '.parquet':
        return pd.read_parquet(filepath, **kwargs)
    else:
        raise ValueError(f"Unsupported file format: {file_ext}")

def filter_dataframe(
    df: pd.DataFrame,
    filters: Dict[str, Any]
) -> pd.DataFrame:
    """
    Filter a DataFrame based on column conditions.
    
    Args:
        df: DataFrame to filter
        filters: Dictionary of {column: value} or {column: [list of values]} pairs
        
    Returns:
        Filtered DataFrame
    """
    filtered_df = df.copy()
    
    for col, val in filters.items():
        if col not in filtered_df.columns:
            continue
            
        if isinstance(val, list):
            filtered_df = filtered_df[filtered_df[col].isin(val)]
        else:
            filtered_df = filtered_df[filtered_df[col] == val]
            
    return filtered_df

def aggregate_data(
    df: pd.DataFrame,
    group_by: Union[str, List[str]],
    agg_column: str,
    agg_function: str = 'sum'
) -> pd.DataFrame:
    """
    Aggregate data by grouping and applying an aggregation function.
    
    Args:
        df: DataFrame to aggregate
        group_by: Column(s) to group by
        agg_column: Column to aggregate
        agg_function: Aggregation function to apply ('sum', 'mean', 'count', etc.)
        
    Returns:
        Aggregated DataFrame
    """
    # Ensure group_by is a list
    if isinstance(group_by, str):
        group_by = [group_by]
        
    # Create dictionary for aggregation
    agg_dict = {agg_column: agg_function}
    
    # Perform aggregation
    result = df.groupby(group_by).agg(agg_dict).reset_index()
    
    return result

def calculate_percentage(
    df: pd.DataFrame,
    value_column: str,
    group_column: Optional[str] = None,
    new_column_name: Optional[str] = None
) -> pd.DataFrame:
    """
    Calculate percentages within groups or of the total.
    
    Args:
        df: DataFrame containing the data
        value_column: Column containing values to calculate percentages of
        group_column: Column to group by (if None, percentages of total)
        new_column_name: Name for the new percentage column
        
    Returns:
        DataFrame with added percentage column
    """
    result = df.copy()
    
    if new_column_name is None:
        new_column_name = f"{value_column}_percent"
    
    if group_column is None:
        # Calculate percentage of total
        total = result[value_column].sum()
        result[new_column_name] = (result[value_column] / total) * 100
    else:
        # Calculate percentage within groups
        result[new_column_name] = (
            result[value_column] / result.groupby(group_column)[value_column].transform('sum')
        ) * 100
        
    return result

def normalize_column(
    df: pd.DataFrame,
    column: str,
    method: str = 'minmax',
    new_column_name: Optional[str] = None
) -> pd.DataFrame:
    """
    Normalize a column in the DataFrame.
    
    Args:
        df: DataFrame containing the data
        column: Column to normalize
        method: Normalization method ('minmax', 'zscore', 'robust')
        new_column_name: Name for the new normalized column
        
    Returns:
        DataFrame with added normalized column
    """
    result = df.copy()
    
    if new_column_name is None:
        new_column_name = f"{column}_norm"
    
    if method == 'minmax':
        # Min-max normalization (0-1 scale)
        min_val = result[column].min()
        max_val = result[column].max()
        result[new_column_name] = (result[column] - min_val) / (max_val - min_val)
    
    elif method == 'zscore':
        # Z-score normalization
        mean = result[column].mean()
        std = result[column].std()
        result[new_column_name] = (result[column] - mean) / std
    
    elif method == 'robust':
        # Robust scaling using median and interquartile range
        median = result[column].median()
        q1 = result[column].quantile(0.25)
        q3 = result[column].quantile(0.75)
        iqr = q3 - q1
        result[new_column_name] = (result[column] - median) / iqr
    
    else:
        raise ValueError(f"Unsupported normalization method: {method}")
        
    return result

def join_data(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    left_on: Union[str, List[str]],
    right_on: Optional[Union[str, List[str]]] = None,
    how: str = 'left',
    suffixes: Tuple[str, str] = ('_left', '_right')
) -> pd.DataFrame:
    """
    Join two DataFrames.
    
    Args:
        left_df: Left DataFrame
        right_df: Right DataFrame
        left_on: Column(s) from left DataFrame to join on
        right_on: Column(s) from right DataFrame to join on (if None, uses left_on)
        how: Type of join ('left', 'right', 'inner', 'outer')
        suffixes: Suffixes to apply to overlapping column names
        
    Returns:
        Joined DataFrame
    """
    if right_on is None:
        right_on = left_on
        
    return left_df.merge(
        right_df,
        left_on=left_on,
        right_on=right_on,
        how=how,
        suffixes=suffixes
    )

def pivot_data(
    df: pd.DataFrame,
    index: Union[str, List[str]],
    columns: str,
    values: str,
    aggfunc: str = 'sum',
    fill_value: Optional[Any] = None
) -> pd.DataFrame:
    """
    Create a pivot table from DataFrame.
    
    Args:
        df: DataFrame to pivot
        index: Column(s) to use as index
        columns: Column to use as columns
        values: Column to use as values
        aggfunc: Aggregation function to apply ('sum', 'mean', 'count', etc.)
        fill_value: Value to use for missing values
        
    Returns:
        Pivoted DataFrame
    """
    return pd.pivot_table(
        df,
        index=index,
        columns=columns,
        values=values,
        aggfunc=aggfunc,
        fill_value=fill_value
    )

def categorize_column(
    df: pd.DataFrame,
    column: str,
    bins: Optional[List[float]] = None,
    labels: Optional[List[str]] = None,
    method: str = 'custom',
    num_bins: int = 5,
    new_column_name: Optional[str] = None
) -> pd.DataFrame:
    """
    Categorize a numeric column into bins.
    
    Args:
        df: DataFrame containing the data
        column: Column to categorize
        bins: Custom bin edges (only for method='custom')
        labels: Labels for bins
        method: Binning method ('custom', 'equal_width', 'equal_frequency', 'jenks')
        num_bins: Number of bins (if bins not provided)
        new_column_name: Name for the new categorical column
        
    Returns:
        DataFrame with added categorical column
    """
    result = df.copy()
    
    if new_column_name is None:
        new_column_name = f"{column}_cat"
    
    if method == 'custom':
        if bins is None:
            raise ValueError("Bins must be provided for 'custom' method")
        result[new_column_name] = pd.cut(result[column], bins=bins, labels=labels, include_lowest=True)
    
    elif method == 'equal_width':
        result[new_column_name] = pd.cut(
            result[column],
            bins=num_bins,
            labels=labels,
            include_lowest=True
        )
    
    elif method == 'equal_frequency':
        result[new_column_name] = pd.qcut(
            result[column],
            q=num_bins,
            labels=labels,
            duplicates='drop'
        )
    
    # For jenks natural breaks, we'd need additional libraries like mapclassify
    # This is a placeholder implementation
    elif method == 'jenks':
        try:
            import mapclassify as mc
            classifier = mc.NaturalBreaks(result[column].values, k=num_bins)
            bins = list(classifier.bins)
            bins.insert(0, result[column].min())
            result[new_column_name] = pd.cut(
                result[column],
                bins=bins,
                labels=labels,
                include_lowest=True
            )
        except ImportError:
            raise ImportError("mapclassify is required for 'jenks' method. Install with: pip install mapclassify")
    
    else:
        raise ValueError(f"Unsupported binning method: {method}")
        
    return result

def get_summary_statistics(df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Generate summary statistics for numeric columns.
    
    Args:
        df: DataFrame to analyze
        columns: List of columns to get statistics for (if None, uses all numeric columns)
        
    Returns:
        DataFrame with summary statistics
    """
    if columns is None:
        # Use all numeric columns
        columns = df.select_dtypes(include=[np.number]).columns.tolist()
    else:
        # Filter to ensure only existing numeric columns are used
        valid_columns = []
        for col in columns:
            if col in df.columns and np.issubdtype(df[col].dtype, np.number):
                valid_columns.append(col)
        columns = valid_columns
    
    if not columns:
        return pd.DataFrame()
    
    # Calculate statistics
    stats = df[columns].describe().T
    
    # Add additional statistics
    stats['median'] = df[columns].median()
    stats['missing'] = df[columns].isna().sum()
    stats['missing_percent'] = (df[columns].isna().sum() / len(df)) * 100
    stats['unique'] = df[columns].nunique()
    
    return stats

def get_categorical_summary(df: pd.DataFrame, columns: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
    """
    Generate summary for categorical columns.
    
    Args:
        df: DataFrame to analyze
        columns: List of columns to get summaries for (if None, uses all object and categorical columns)
        
    Returns:
        Dictionary of DataFrames with categorical summaries
    """
    if columns is None:
        # Use all object and categorical columns
        columns = df.select_dtypes(include=['object', 'category']).columns.tolist()
    else:
        # Filter to ensure only existing categorical columns are used
        valid_columns = []
        for col in columns:
            if col in df.columns and (df[col].dtype == 'object' or df[col].dtype.name == 'category'):
                valid_columns.append(col)
        columns = valid_columns
    
    results = {}
    
    for col in columns:
        # Get value counts
        value_counts = df[col].value_counts().reset_index()
        value_counts.columns = [col, 'count']
        
        # Add percentage
        value_counts['percentage'] = (value_counts['count'] / len(df)) * 100
        
        # Store in results dictionary
        results[col] = value_counts
    
    return results

def detect_outliers(
    df: pd.DataFrame,
    column: str,
    method: str = 'iqr',
    threshold: float = 1.5
) -> pd.Series:
    """
    Detect outliers in a column using various methods.
    
    Args:
        df: DataFrame containing the data
        column: Column to analyze for outliers
        method: Detection method ('iqr', 'zscore', 'modified_zscore')
        threshold: Threshold for outlier detection
        
    Returns:
        Boolean Series indicating outlier rows
    """
    if method == 'iqr':
        # IQR method
        q1 = df[column].quantile(0.25)
        q3 = df[column].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - (threshold * iqr)
        upper_bound = q3 + (threshold * iqr)
        
        return (df[column] < lower_bound) | (df[column] > upper_bound)
    
    elif method == 'zscore':
        # Z-score method
        mean = df[column].mean()
        std = df[column].std()
        z_scores = (df[column] - mean) / std
        
        return abs(z_scores) > threshold
    
    elif method == 'modified_zscore':
        # Modified z-score using median
        median = df[column].median()
        mad = np.median(np.abs(df[column] - median))
        modified_z_scores = 0.6745 * (df[column] - median) / mad
        
        return abs(modified_z_scores) > threshold
    
    else:
        raise ValueError(f"Unsupported outlier detection method: {method}")

def calculate_correlation(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    method: str = 'pearson'
) -> pd.DataFrame:
    """
    Calculate correlation matrix between numeric columns.
    
    Args:
        df: DataFrame containing the data
        columns: List of columns to calculate correlations for (if None, uses all numeric columns)
        method: Correlation method ('pearson', 'spearman', 'kendall')
        
    Returns:
        Correlation matrix
    """
    if columns is None:
        # Use all numeric columns
        columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
    # Calculate correlation
    correlation_matrix = df[columns].corr(method=method)
    
    return correlation_matrix

def find_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find missing values in the DataFrame.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        DataFrame with missing value statistics
    """
    # Count missing values per column
    missing = pd.DataFrame(df.isna().sum(), columns=['count'])
    
    # Calculate percentage
    missing['percentage'] = (missing['count'] / len(df)) * 100
    
    # Reset index to make column name a column
    missing = missing.reset_index()
    missing.columns = ['column', 'count', 'percentage']
    
    # Sort by count of missing values, descending
    missing = missing.sort_values('count', ascending=False)
    
    return missing 