import pandasai as pai
import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set API key
api_key = os.getenv("PANDASAI_API_KEY")
if api_key:
    pai.api_key.set(api_key)
else:
    print("Warning: PANDASAI_API_KEY not found in .env file")
    # You can set it directly if needed
    # pai.api_key.set("your-api-key-here")

# Source parquet file path
parquet_path = "data/exports/state_transitions/state_transitions_scenario_21_ensemble_overall.parquet"
print(f"Reading parquet file: {parquet_path}")

# Convert parquet to CSV
csv_path = "data/exports/state_transitions/state_transitions_scenario_21_ensemble_overall.csv"
print(f"Converting to CSV: {csv_path}")

# Read parquet with pandas
df = pd.read_parquet(parquet_path)
print(f"Parquet file shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")

# Save as CSV
df.to_csv(csv_path, index=False)
print(f"CSV file saved to: {csv_path}")

# Now load the CSV with pandasai
file = pai.read_csv(csv_path)
print(f"Loaded CSV file with PandasAI")

# Save your dataset configuration
pai_df = pai.create(
  path="pai-personal-97c70/rpa-landuse-state-transitions",
  df=file,
  description="RPA Land Use state transitions data for ensemble overall scenario"
)

# Push your dataset to PandasAI
print("Uploading dataset to PandasAI...")
pai_df.push()
print("Upload complete!") 