import pandas as pd
from typing import Optional, Union, List
from pathlib import Path

def load_landuse_data(filepath: str = "rpa_landuse_data.parquet") -> pd.DataFrame:
    """
    Load land use change data from a Parquet file.
    
    Args:
        filepath (str): Path to the Parquet file
        
    Returns:
        pd.DataFrame: DataFrame containing the land use change data
        
    Raises:
        FileNotFoundError: If the Parquet file doesn't exist
    """
    if not Path(filepath).exists():
        raise FileNotFoundError(f"Data file not found: {filepath}")
    
    return pd.read_parquet(filepath)

def filter_data(
    df: pd.DataFrame,
    scenario: Optional[Union[str, List[str]]] = None,
    year: Optional[Union[int, List[int]]] = None,
    fips: Optional[Union[str, List[str]]] = None,
    from_landuse: Optional[Union[str, List[str]]] = None,
    to_landuse: Optional[Union[str, List[str]]] = None
) -> pd.DataFrame:
    """
    Filter the land use change data by various criteria.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        scenario (str or list): Scenario(s) to filter by
        year (int or list): Year(s) to filter by
        fips (str or list): FIPS code(s) to filter by
        from_landuse (str or list): Source land use type(s)
        to_landuse (str or list): Destination land use type(s)
    
    Returns:
        pd.DataFrame: Filtered DataFrame
    """
    filtered_df = df.copy()
    
    # Helper function to apply filters
    def apply_filter(df, column, value):
        if value is not None:
            if isinstance(value, (str, int)):
                return df[df[column] == value]
            else:  # list-like
                return df[df[column].isin(value)]
        return df
    
    # Apply each filter
    filtered_df = apply_filter(filtered_df, 'Scenario', scenario)
    filtered_df = apply_filter(filtered_df, 'Year', year)
    filtered_df = apply_filter(filtered_df, 'FIPS', fips)
    filtered_df = apply_filter(filtered_df, 'From', from_landuse)
    filtered_df = apply_filter(filtered_df, 'To', to_landuse)
    
    return filtered_df

def get_unique_values(df: pd.DataFrame, column: str) -> list:
    """
    Get unique values from a specific column in the dataset.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        column (str): Column name
    
    Returns:
        list: Sorted list of unique values
    """
    return sorted(df[column].unique().tolist())

def summarize_transitions(
    df: pd.DataFrame,
    group_by: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Summarize land use transitions by grouping variables.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        group_by (list): Columns to group by, in addition to From and To
        
    Returns:
        pd.DataFrame: Summarized transitions
    """
    if group_by is None:
        group_by = []
    
    group_cols = group_by + ['From', 'To']
    return df.groupby(group_cols)['Acres'].sum().reset_index() 