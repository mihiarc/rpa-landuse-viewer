"""
Data validator for RPA land use data.
Performs basic data validation checks.
"""

import pandas as pd
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def validate_parquet_data(file_path):
    """
    Perform validation checks on the Parquet data.
    
    Args:
        file_path (str): Path to the Parquet file
        
    Returns:
        dict: Validation results with any issues found
    """
    if not Path(file_path).exists():
        logger.error(f"File not found: {file_path}")
        return {"valid": False, "error": "File not found"}
    
    try:
        logger.info(f"Validating data in {file_path}...")
        df = pd.read_parquet(file_path)
        
        results = {
            "valid": True,
            "issues": [],
            "record_count": len(df),
            "validation_details": {}
        }
        
        # Check 1: Required columns exist
        required_columns = ['Scenario', 'Year', 'YearRange', 'FIPS', 'From', 'To', 'Acres']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            results["valid"] = False
            results["issues"].append(f"Missing columns: {missing_columns}")
        
        # Check 2: No null values in key fields
        for col in required_columns:
            if col in df.columns and df[col].isnull().any():
                null_count = df[col].isnull().sum()
                results["valid"] = False
                results["issues"].append(f"Column '{col}' has {null_count} null values")
        
        # Check 3: Acres values are positive
        if 'Acres' in df.columns and (df['Acres'] <= 0).any():
            negative_count = (df['Acres'] <= 0).sum()
            results["valid"] = False
            results["issues"].append(f"Found {negative_count} records with non-positive acres")
        
        # Check 4: Valid land use types
        valid_land_uses = ['Crop', 'Pasture', 'Range', 'Forest', 'Urban']
        if 'From' in df.columns:
            invalid_from = df[~df['From'].isin(valid_land_uses)]['From'].unique()
            if len(invalid_from) > 0:
                results["valid"] = False
                results["issues"].append(f"Invalid 'From' land use types: {invalid_from}")
        
        if 'To' in df.columns:
            invalid_to = df[~df['To'].isin(valid_land_uses)]['To'].unique()
            if len(invalid_to) > 0:
                results["valid"] = False
                results["issues"].append(f"Invalid 'To' land use types: {invalid_to}")
        
        # Check 5: Valid years
        valid_years = list(range(2020, 2080, 10))
        if 'Year' in df.columns:
            invalid_years = df[~df['Year'].isin(valid_years)]['Year'].unique()
            if len(invalid_years) > 0:
                results["valid"] = False
                results["issues"].append(f"Invalid years: {invalid_years}")
        
        # Summary statistics
        results["validation_details"] = {
            "scenarios": df['Scenario'].nunique() if 'Scenario' in df.columns else 0,
            "counties": df['FIPS'].nunique() if 'FIPS' in df.columns else 0,
            "years": sorted(df['Year'].unique()) if 'Year' in df.columns else [],
            "total_acres": df['Acres'].sum() if 'Acres' in df.columns else 0
        }
        
        if results["valid"]:
            logger.info("Validation passed: No issues found")
        else:
            logger.warning(f"Validation failed with {len(results['issues'])} issues")
            for issue in results["issues"]:
                logger.warning(f"- {issue}")
        
        return results
    
    except Exception as e:
        logger.error(f"Error validating data: {str(e)}")
        return {"valid": False, "error": str(e)}

if __name__ == "__main__":
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description='Validate RPA land use Parquet data.')
    parser.add_argument('--input', default='data/processed/rpa_landuse_data.parquet',
                      help='Input Parquet file path')
    parser.add_argument('--output', default='data/validation_results.json',
                      help='Output validation results file (optional)')
    args = parser.parse_args()
    
    results = validate_parquet_data(args.input)
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Validation results saved to {args.output}")
    
    print("\nValidation Summary:")
    print(f"Valid: {results['valid']}")
    if not results['valid']:
        print("\nIssues found:")
        for issue in results.get('issues', []):
            print(f"- {issue}")
    
    if 'validation_details' in results:
        details = results['validation_details']
        print("\nData Summary:")
        print(f"Records: {results.get('record_count', 0):,}")
        print(f"Scenarios: {details.get('scenarios', 0)}")
        print(f"Counties: {details.get('counties', 0)}")
        print(f"Years: {details.get('years', [])}")
        print(f"Total acres: {details.get('total_acres', 0):,.2f}") 