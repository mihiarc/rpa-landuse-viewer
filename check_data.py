from data_loader import load_landuse_data
import pandas as pd

# Load the data
print("Loading data...")
df = load_landuse_data()

# Check for missing values
print("\nMissing values:")
print(df.isnull().sum())

# Check value ranges
print("\nValue ranges:")
print(df.describe())

# Check unique values in categorical columns
for col in ['From', 'To', 'Scenario']:
    print(f"\nUnique {col} values:")
    print(df[col].unique())

# Check for zero or negative acres
print("\nZero or negative acres:")
print((df['Acres'] <= 0).sum(), "rows") 