## Implementation Plan for RPA Land Use Data Dashboard

### 1. Replacing Current Analysis with PandasAI

PandasAI is a powerful library that connects pandas dataframes with AI models to allow natural language querying of your data. Here's how to implement it:

1. **Installation**:
   ```bash
   pip install "pandasai>=3.0.0b2" streamlit
   ```

2. **Basic integration with your SQLite database**:
   Create a new module `src/analysis/pandas_ai_analyzer.py`:

   ```python
   import pandasai as pai
   import pandas as pd
   import sqlite3
   from dotenv import load_dotenv
   import os

   # Load environment variables
   load_dotenv()

   # Configure PandasAI
   api_key = os.getenv("PANDASAI_API_KEY")
   pai.api_key.set(api_key)  # Or configure with your choice of LLM

   class LandUseAnalyzer:
       def __init__(self, db_path="data/database/rpa_landuse.db"):
           self.db_path = db_path
           self.conn = sqlite3.connect(db_path)
       
       def load_data(self, query):
           """Load data from SQLite database using SQL query"""
           df = pd.read_sql_query(query, self.conn)
           return pai.DataFrame(df)
       
       def get_scenarios(self):
           """Get list of available scenarios"""
           query = "SELECT scenario_name FROM scenarios"
           df = pd.read_sql_query(query, self.conn)
           return df['scenario_name'].tolist()
       
       def get_time_periods(self):
           """Get list of available time periods"""
           query = "SELECT start_year, end_year FROM time_steps"
           df = pd.read_sql_query(query, self.conn)
           return [(row['start_year'], row['end_year']) for _, row in df.iterrows()]
       
       def analyze_transitions(self, scenario_name, start_year, end_year):
           """Load land use transition data for specific scenario and time period"""
           query = f"""
           SELECT 
               c.fips_code, c.county_name, t.from_land_use, t.to_land_use, t.acres,
               ts.start_year, ts.end_year, s.scenario_name, s.gcm, s.rcp, s.ssp
           FROM 
               land_use_transitions t
           JOIN 
               scenarios s ON t.scenario_id = s.scenario_id
           JOIN 
               time_steps ts ON t.time_step_id = ts.time_step_id
           JOIN 
               counties c ON t.fips_code = c.fips_code
           WHERE 
               s.scenario_name = '{scenario_name}'
               AND ts.start_year >= {start_year}
               AND ts.end_year <= {end_year}
           """
           df = self.load_data(query)
           return df
       
       def close(self):
           """Close database connection"""
           if self.conn:
               self.conn.close()
   ```

### 2. Creating a Streamlit Dashboard

Create `app.py` in the project root:

```python
import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
from streamlit_folium import folium_static
import os
import sqlite3
from src.analysis.pandas_ai_analyzer import LandUseAnalyzer
import json

# Set page configuration
st.set_page_config(
    page_title="RPA Land Use Change Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize the analyzer
analyzer = LandUseAnalyzer()

# Load county geojson data
@st.cache_data
def load_county_geo():
    counties = gpd.read_file("data/counties.geojson")
    return counties

# Title and description
st.title("USDA RPA Land Use Change Dashboard")
st.markdown("""
This dashboard allows you to explore land use change projections across the United States
from 2020 to 2070 under various climate and socioeconomic scenarios.
""")

# Sidebar controls
st.sidebar.header("Data Selection")

# Get available scenarios and time periods
scenarios = analyzer.get_scenarios()
time_periods = analyzer.get_time_periods()

# Create sidebar widgets
selected_scenario = st.sidebar.selectbox(
    "Select Scenario", 
    scenarios,
    help="Climate model and socioeconomic pathway combination"
)

# Create start and end year selections
start_years = sorted(set([t[0] for t in time_periods]))
end_years = sorted(set([t[1] for t in time_periods]))

selected_start_year = st.sidebar.selectbox("Start Year", start_years)
selected_end_year = st.sidebar.selectbox("End Year", end_years, index=len(end_years)-1)

# Land use type selection
land_use_types = ["Forest", "Cropland", "Pasture", "Range", "Urban"]
selected_land_use = st.sidebar.selectbox("Land Use Type", land_use_types)

# Main content area
tab1, tab2 = st.tabs(["Map View", "PandasAI Analysis"])

with tab1:
    st.header("Land Use Change Map")
    
    # Load county geometry
    counties_geo = load_county_geo()
    
    # Load transition data based on selection
    transitions_df = analyzer.analyze_transitions(
        selected_scenario, 
        selected_start_year, 
        selected_end_year
    )
    
    # Convert to pandas DataFrame for display
    transitions_pd = transitions_df.to_pandas()
    
    # Display sample data
    with st.expander("View Sample Data"):
        st.dataframe(transitions_pd.head(10))
    
    # Filter for selected land use type
    if selected_land_use:
        filtered_df = transitions_pd[transitions_pd['from_land_use'] == selected_land_use]
        
        # Calculate net change per county
        county_changes = filtered_df.groupby('fips_code').agg(
            net_change=('acres', lambda x: -x.sum()),  # Negative because we're looking at "from" land use
            county_name=('county_name', 'first')
        ).reset_index()
        
        # Merge with county geometries
        merged_data = counties_geo.merge(county_changes, left_on='FIPS', right_on='fips_code', how='left')
        merged_data['net_change'].fillna(0, inplace=True)
        
        # Create choropleth map
        m = folium.Map(location=[39.8283, -98.5795], zoom_start=4)
        
        # Add choropleth layer
        folium.Choropleth(
            geo_data=json.loads(merged_data.to_json()),
            name="choropleth",
            data=merged_data,
            columns=["fips_code", "net_change"],
            key_on="feature.properties.fips_code",
            fill_color="YlOrRd",
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name=f"Net Change in {selected_land_use} Land (acres)",
            highlight=True
        ).add_to(m)
        
        # Display the map
        folium_static(m)
        
        # Display statistics
        st.subheader(f"Statistics for {selected_land_use} Land Use")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            total_loss = filtered_df['acres'].sum()
            st.metric("Total Land Converted (acres)", f"{total_loss:,.0f}")
        
        with col2:
            top_destination = filtered_df.groupby('to_land_use')['acres'].sum().sort_values(ascending=False).index[0]
            st.metric("Primary Conversion Destination", top_destination)
        
        with col3:
            affected_counties = filtered_df['fips_code'].nunique()
            st.metric("Counties Affected", affected_counties)

with tab2:
    st.header("Natural Language Data Analysis")
    st.info("Ask questions about the land use data using natural language.")
    
    # Create input for natural language queries
    query = st.text_area("Enter your question about the land use data:", 
                        "What is the total forest land converted to urban use in California counties?")
    
    if st.button("Analyze"):
        with st.spinner("Analyzing data..."):
            # Get data for the selected scenario and time period
            df = analyzer.analyze_transitions(
                selected_scenario, 
                selected_start_year, 
                selected_end_year
            )
            
            # Process query with PandasAI
            try:
                result = df.chat(query)
                st.write(result)
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                st.info("Try rephrasing your question or check if the data contains the information you're looking for.")

# Footer
st.sidebar.markdown("---")
st.sidebar.caption("Data source: USDA Forest Service RPA Assessment")
```

### 3. Environment Setup

Create a `.env.example` file with:

```
# Environment Variables
PANDASAI_API_KEY=your_key_here  # Get from https://app.pandabi.ai
```

Update `requirements.txt`:

```
pandas>=1.3.0
plotly>=5.1.0
folium>=0.12.1
streamlit>=1.22.0
streamlit-folium>=0.15.0
pytest>=6.2.5
python-dotenv>=0.19.0
geopandas>=0.9.0
branca>=0.4.2
pandasai>=3.0.0b2
```

### 4. Key Features to Include in the Implementation

1. **Interactive Map Visualization**:
   - Choropleth maps showing county-level land use changes
   - Filterable by scenario, time period, and land use type
   - Color-coded to show intensity of changes

2. **PandasAI Natural Language Interface**:
   - Ask questions about the data in plain English
   - Analyze trends, patterns, and statistical information
   - Compare different scenarios and time periods

3. **Data Exploration Tools**:
   - Filtering capabilities for scenarios, time periods, and regions
   - Summary statistics and key metrics
   - Downloadable reports and data exports

4. **Dashboard Sections**:
   - Overview of the RPA land use projection project
   - Interactive map visualization
   - Natural language query interface
   - Scenario comparison tools
   - Key findings and insights

### 5. Running the Application

To run the dashboard:

```bash
streamlit run app.py
```

This implementation combines the strengths of PandasAI for flexible data analysis with Streamlit's interactive dashboard capabilities. The users will be able to explore complex land use change data through intuitive maps and natural language queries, making the projections more accessible and useful for decision-making.
