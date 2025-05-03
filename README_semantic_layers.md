# RPA Land Use Viewer - Semantic Layers Implementation

This documentation explains how to use the semantic layers implementation for the RPA Land Use Viewer project. The semantic layers allow you to query the land use transition data using natural language through PandasAI.

## Prerequisites

- Python 3.8 or higher
- UV package manager
- PandasAI API key (Sign up at [pandasai.com](https://pandasai.com/))

## Setup

1. Create a virtual environment and install dependencies:
   ```bash
   uv venv .venv
   source .venv/bin/activate
   uv pip install duckdb pandas pyarrow pandasai python-dotenv
   ```

2. Copy the template env file and add your API key:
   ```bash
   cp pandasai.env.template .env
   ```
   Edit the `.env` file to add your PandasAI API key.

3. Run the script to extract data and create semantic layers:
   ```bash
   python create_semantic_layers.py
   ```
   This script will:
   - Extract relevant data from the DuckDB database
   - Save it as Parquet files in a `land_use_parquet` directory
   - Create semantic layers in PandasAI

4. Run the example query script to see how to use the semantic layers:
   ```bash
   python query_semantic_layers.py
   ```

## Available Semantic Layers

1. **Land Use Transitions Dataset** (`rpa-landuse/land-use-transitions`)
   - Aggregated land use transition data across scenarios and time periods
   - Includes scenario details, time periods, and transition areas

2. **County Land Use Transitions Dataset** (`rpa-landuse/county-transitions`)
   - County-level land use transition data
   - Includes county details, scenario information, and transition areas

3. **Urbanization Trends Dataset** (`rpa-landuse/urbanization-trends`)
   - Time series data of conversion to urban land use by scenario
   - Focuses on forest, cropland, and pasture conversion to urban areas

## Example Queries

Here are some example natural language queries you can use:

- "Which scenario shows the most conversion from Forest to Urban land?"
- "Which county shows the highest agricultural land conversion to urban areas?"
- "Create a bar chart showing the top 5 scenarios with highest forest to urban conversion"
- "Compare Forest to Urban conversion across different time periods"

## Custom Queries

You can use the PandasAI chat interface to create your own natural language queries. Here's a simple example:

```python
import pandasai as pai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load dataset
transitions_dataset = pai.load("rpa-landuse/land-use-transitions")

# Ask your question
response = transitions_dataset.chat("Your natural language query here")
print(response)
```

## Troubleshooting

- If you encounter errors with the PandasAI API key, make sure it's correctly set in your `.env` file
- If the datasets fail to load, ensure you've run `create_semantic_layers.py` first
- For more help with PandasAI, visit their [documentation](https://docs.pandasai.com/) 