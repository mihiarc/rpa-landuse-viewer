"""
Functions for querying the PandasAI semantic layers.
"""

import os
import pandasai as pai
from dotenv import load_dotenv


def setup_api_key():
    """Set up the PandasAI API key from environment variables."""
    load_dotenv(dotenv_path=".env")
    api_key = os.getenv("PANDASAI_API_KEY")
    
    if not api_key:
        raise ValueError(
            "No PANDASAI_API_KEY found in .env file. "
            "Please create a .env file with your PandasAI API key."
        )
    
    pai.api_key.set(api_key)


def load_datasets(org_path="rpa-landuse"):
    """
    Load PandasAI datasets from the organization.
    
    Args:
        org_path (str): Organization path prefix for PandasAI datasets
        
    Returns:
        dict: Dictionary with the loaded semantic layer objects
    """
    setup_api_key()
    
    # Load the datasets
    try:
        transitions_dataset = pai.load(f"{org_path}/land-use-transitions")
        county_dataset = pai.load(f"{org_path}/county-transitions")
        urbanization_dataset = pai.load(f"{org_path}/urbanization-trends")
        
        return {
            "transitions": transitions_dataset,
            "county": county_dataset,
            "urbanization": urbanization_dataset
        }
    except Exception as e:
        raise RuntimeError(f"Failed to load datasets: {e}")


def query_transitions(query, org_path="rpa-landuse"):
    """
    Query the land use transitions dataset with natural language.
    
    Args:
        query (str): Natural language query
        org_path (str): Organization path prefix for PandasAI datasets
        
    Returns:
        object: Response from PandasAI
    """
    setup_api_key()
    
    # Load the dataset
    dataset = pai.load(f"{org_path}/land-use-transitions")
    
    # Query the dataset
    return dataset.chat(query)


def query_county_transitions(query, org_path="rpa-landuse"):
    """
    Query the county-level transitions dataset with natural language.
    
    Args:
        query (str): Natural language query
        org_path (str): Organization path prefix for PandasAI datasets
        
    Returns:
        object: Response from PandasAI
    """
    setup_api_key()
    
    # Load the dataset
    dataset = pai.load(f"{org_path}/county-transitions")
    
    # Query the dataset
    return dataset.chat(query)


def query_urbanization_trends(query, org_path="rpa-landuse"):
    """
    Query the urbanization trends dataset with natural language.
    
    Args:
        query (str): Natural language query
        org_path (str): Organization path prefix for PandasAI datasets
        
    Returns:
        object: Response from PandasAI
    """
    setup_api_key()
    
    # Load the dataset
    dataset = pai.load(f"{org_path}/urbanization-trends")
    
    # Query the dataset
    return dataset.chat(query)


def multi_dataset_query(query, datasets=None, org_path="rpa-landuse"):
    """
    Query multiple datasets with natural language.
    
    Args:
        query (str): Natural language query
        datasets (list, optional): List of dataset names to include
                                  (transitions, county, urbanization)
        org_path (str): Organization path prefix for PandasAI datasets
        
    Returns:
        object: Response from PandasAI
    """
    setup_api_key()
    
    # Default to all datasets if none specified
    if not datasets:
        datasets = ["transitions", "county", "urbanization"]
    
    # Validate dataset names
    valid_names = ["transitions", "county", "urbanization"]
    for name in datasets:
        if name not in valid_names:
            raise ValueError(f"Invalid dataset name: {name}. "
                           f"Valid names are: {', '.join(valid_names)}")
    
    # Get dataset paths
    paths = {
        "transitions": f"{org_path}/land-use-transitions",
        "county": f"{org_path}/county-transitions",
        "urbanization": f"{org_path}/urbanization-trends"
    }
    
    # Load the selected datasets
    loaded_datasets = [pai.load(paths[name]) for name in datasets]
    
    # Query the datasets
    return pai.chat(query, *loaded_datasets) 