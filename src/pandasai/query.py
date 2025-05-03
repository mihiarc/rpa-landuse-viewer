"""
Functions for querying the PandasAI semantic layers.
"""

import os
import pandas as pd
from pandasai import SmartDataframe
from pandasai.llm import OpenAI
from dotenv import load_dotenv


def get_api_key():
    """Get the API key from environment variables."""
    load_dotenv(dotenv_path=".env")
    
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "No API key found in .env file. "
            "Please create a .env file with your OPENAI_API_KEY."
        )
    
    return api_key


def get_llm():
    """Get the LLM with the API key."""
    api_key = get_api_key()
    
    # Use OpenAI with the provided API key
    # Note: If using non-OpenAI keys (like Anthropic), you might need to modify
    # your .env file to use a standard OpenAI format key instead
    return OpenAI(api_token=api_key)


def load_datasets(parquet_dir="land_use_parquet"):
    """
    Load datasets from parquet files and create SmartDataframes.
    
    Args:
        parquet_dir (str): Directory containing parquet files
        
    Returns:
        dict: Dictionary with SmartDataframe objects
    """
    llm = get_llm()
    
    # Paths for the parquet files
    transitions_parquet = f"{parquet_dir}/transitions_summary.parquet"
    county_parquet = f"{parquet_dir}/county_transitions.parquet"
    urbanization_parquet = f"{parquet_dir}/urbanization_trends.parquet"
    
    try:
        # Load the dataframes
        transitions_df = pd.read_parquet(transitions_parquet)
        county_df = pd.read_parquet(county_parquet)
        urbanization_df = pd.read_parquet(urbanization_parquet)
        
        # Create SmartDataframes
        transitions_smart_df = SmartDataframe(
            transitions_df,
            config={"llm": llm, "name": "Land Use Transitions"}
        )
        
        county_smart_df = SmartDataframe(
            county_df,
            config={"llm": llm, "name": "County Land Use Transitions"}
        )
        
        urbanization_smart_df = SmartDataframe(
            urbanization_df,
            config={"llm": llm, "name": "Urbanization Trends"}
        )
        
        return {
            "transitions": transitions_smart_df,
            "county": county_smart_df,
            "urbanization": urbanization_smart_df
        }
    except Exception as e:
        raise RuntimeError(f"Failed to load datasets: {e}")


def query_transitions(query, parquet_dir="land_use_parquet"):
    """
    Query the land use transitions dataset with natural language.
    
    Args:
        query (str): Natural language query
        parquet_dir (str): Directory containing parquet files
        
    Returns:
        object: Response from PandasAI
    """
    llm = get_llm()
    
    # Load the dataset
    transitions_df = pd.read_parquet(f"{parquet_dir}/transitions_summary.parquet")
    smart_df = SmartDataframe(
        transitions_df,
        config={"llm": llm, "name": "Land Use Transitions"}
    )
    
    # Query the dataset
    return smart_df.chat(query)


def query_county_transitions(query, parquet_dir="land_use_parquet"):
    """
    Query the county-level transitions dataset with natural language.
    
    Args:
        query (str): Natural language query
        parquet_dir (str): Directory containing parquet files
        
    Returns:
        object: Response from PandasAI
    """
    llm = get_llm()
    
    # Load the dataset
    county_df = pd.read_parquet(f"{parquet_dir}/county_transitions.parquet")
    smart_df = SmartDataframe(
        county_df,
        config={"llm": llm, "name": "County Land Use Transitions"}
    )
    
    # Query the dataset
    return smart_df.chat(query)


def query_urbanization_trends(query, parquet_dir="land_use_parquet"):
    """
    Query the urbanization trends dataset with natural language.
    
    Args:
        query (str): Natural language query
        parquet_dir (str): Directory containing parquet files
        
    Returns:
        object: Response from PandasAI
    """
    llm = get_llm()
    
    # Load the dataset
    urbanization_df = pd.read_parquet(f"{parquet_dir}/urbanization_trends.parquet")
    smart_df = SmartDataframe(
        urbanization_df,
        config={"llm": llm, "name": "Urbanization Trends"}
    )
    
    # Query the dataset
    return smart_df.chat(query)


def multi_dataset_query(query, datasets=None, parquet_dir="land_use_parquet"):
    """
    Query multiple datasets with natural language.
    
    Args:
        query (str): Natural language query
        datasets (list, optional): List of dataset names to include
                                  (transitions, county, urbanization)
        parquet_dir (str): Directory containing parquet files
        
    Returns:
        object: Response from PandasAI
    """
    # Default to all datasets if none specified
    if not datasets:
        datasets = ["transitions", "county", "urbanization"]
    
    # Validate dataset names
    valid_names = ["transitions", "county", "urbanization"]
    for name in datasets:
        if name not in valid_names:
            raise ValueError(f"Invalid dataset name: {name}. "
                           f"Valid names are: {', '.join(valid_names)}")
    
    # Load all datasets
    all_datasets = load_datasets(parquet_dir)
    
    # Filter to selected datasets
    selected_datasets = [all_datasets[name] for name in datasets]
    
    # For now, just use the first dataset since pandasai 2.4.2 doesn't support 
    # multi-dataset querying in the same way
    primary_dataset = selected_datasets[0]
    
    return primary_dataset.chat(f"Query across multiple datasets: {query}") 