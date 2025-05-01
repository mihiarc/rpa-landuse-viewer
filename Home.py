"""
RPA Land Use Viewer - Main Application
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import os

# Import utility modules
from src.utils.data_processing import (
    filter_data,
    aggregate_data
)
from src.utils.data_loader import (
    get_scenarios as get_available_scenarios,
    get_years as get_available_years,
    get_states,
    get_counties_by_state as get_counties_for_state,
    get_national_summary as load_scenario_data
)
from src.utils.data import (
    calculate_percentage,
    get_summary_statistics as calculate_metrics
)
from src.utils.visualization import (
    create_choropleth_map as generate_choropleth,
    create_bar_chart as generate_bar_chart,
    create_pie_chart as generate_pie_chart,
    create_line_chart as generate_line_chart
)

# Set page configuration
st.set_page_config(
    page_title="RPA Land Use Viewer",
    page_icon="🌆",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load custom CSS
def load_css():
    css_file = Path("src/styles/style.css")
    if css_file.exists():
        with open(css_file) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

load_css()

# Application title and description
st.title("RPA Land Use Viewer")
st.markdown("""
This application allows you to explore land use scenarios for the Regional Plan Association (RPA) region.
Select different scenarios, years, and geographic areas to visualize the data.
""")

# Sidebar for inputs
st.sidebar.header("Data Selection")

# Scenario selection
scenario = st.sidebar.selectbox(
    "Select Scenario",
    get_available_scenarios()
)

# Year selection
year = st.sidebar.selectbox(
    "Select Year",
    get_available_years()
)

# Geographic filters
st.sidebar.header("Geographic Filters")

# State selection
state = st.sidebar.selectbox(
    "Select State",
    ["All States"] + get_states()
)

# County selection (dependent on state)
counties = ["All Counties"]
if state != "All States":
    counties += get_counties_for_state(state)

county = st.sidebar.selectbox(
    "Select County",
    counties
)

# Visualization options
st.sidebar.header("Visualization Options")
vis_type = st.sidebar.selectbox(
    "Visualization Type",
    ["Map", "Bar Chart", "Pie Chart", "Line Chart"]
)

# Data loading with a spinner
with st.spinner("Loading data..."):
    df = load_scenario_data(scenario, year)
    
    # Check if data was loaded successfully
    if df.empty:
        st.error("No data available for the selected scenario and year.")
        st.stop()
    
    # Apply geographic filters
    if state != "All States":
        state_filter = state
    else:
        state_filter = None
        
    if county != "All Counties":
        county_filter = county
    else:
        county_filter = None
    
    filtered_df = filter_data(df, state_filter, county_filter)
    
    # Check if filtered data is empty
    if filtered_df.empty:
        st.warning("No data available for the selected geographic filters.")
        st.stop()

# Display metrics
st.header("Key Metrics")
metrics = calculate_metrics(filtered_df)

# Create a row of metrics
metric_cols = st.columns(len(metrics))
for i, (metric_name, metric_value) in enumerate(metrics.items()):
    # Format the metric name to be more readable
    formatted_name = metric_name.replace('_', ' ').title()
    # Format the value based on type
    if isinstance(metric_value, (int, float)):
        if metric_name.startswith('percent'):
            formatted_value = f"{metric_value:.1f}%"
        elif metric_value >= 1_000_000:
            formatted_value = f"{metric_value/1_000_000:.2f}M"
        elif metric_value >= 1_000:
            formatted_value = f"{metric_value/1_000:.1f}K"
        else:
            formatted_value = f"{metric_value:,.0f}"
    else:
        formatted_value = str(metric_value)
    
    metric_cols[i].metric(formatted_name, formatted_value)

# Display visualizations
st.header("Visualization")

if vis_type == "Map":
    st.subheader("Geographic Distribution")
    
    # Check if necessary columns for choropleth exist
    if 'state' in filtered_df.columns and any(col for col in filtered_df.columns if col.endswith('_acres')):
        # Find the first column that ends with '_acres' for the choropleth
        value_col = next((col for col in filtered_df.columns if col.endswith('_acres')), None)
        
        if value_col:
            fig = generate_choropleth(
                filtered_df,
                geo_col='state',
                value_col=value_col,
                title=f"{value_col.replace('_', ' ').title()} by State ({year})",
                scope="usa"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No suitable data columns found for map visualization.")
    else:
        st.warning("Required geographic data columns not found in the dataset.")

elif vis_type == "Bar Chart":
    st.subheader("Comparative Analysis")
    
    # Determine appropriate columns for bar chart
    if 'county' in filtered_df.columns and any(col for col in filtered_df.columns if col.endswith('_acres')):
        # Find the first column that ends with '_acres' for the bar chart
        value_col = next((col for col in filtered_df.columns if col.endswith('_acres')), None)
        
        if value_col:
            # Aggregate data by county if there are too many rows
            if len(filtered_df) > 20:
                chart_df = aggregate_data(filtered_df, 'county')
            else:
                chart_df = filtered_df
            
            fig = generate_bar_chart(
                chart_df,
                x_col='county',
                y_col=value_col,
                title=f"{value_col.replace('_', ' ').title()} by County ({year})",
                x_title="County",
                y_title=value_col.replace('_', ' ').title()
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No suitable data columns found for bar chart visualization.")
    else:
        st.warning("Required data columns not found for bar chart visualization.")

elif vis_type == "Pie Chart":
    st.subheader("Proportional Analysis")
    
    # Determine appropriate columns for pie chart
    land_use_cols = [col for col in filtered_df.columns if col.endswith('_acres') and col != 'total_acres']
    
    if land_use_cols:
        # Create a new dataframe with land use types and their values
        pie_data = pd.DataFrame({
            'land_use_type': [col.replace('_acres', '').replace('_', ' ').title() for col in land_use_cols],
            'acres': [filtered_df[col].sum() for col in land_use_cols]
        })
        
        fig = generate_pie_chart(
            pie_data,
            values_col='acres',
            names_col='land_use_type',
            title=f"Land Use Distribution ({year})"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No suitable land use data columns found for pie chart visualization.")

elif vis_type == "Line Chart":
    st.subheader("Temporal Analysis")
    st.info("Line chart visualizations require data from multiple years. Please select specific geographic areas and metrics to compare across years.")
    
    # This section would ideally load data for all years and create a line chart
    # Since we're loading one year at a time currently, this is a placeholder
    st.warning("Multi-year data analysis is not yet implemented.")

# Additional information section
st.header("About the Data")
st.markdown("""
This viewer uses land use data for the RPA region, projecting different scenarios for future development.
The data includes metrics like developed acres, population, employment, and more across different geographic levels.

For more information, please visit the [Regional Plan Association website](https://rpa.org/).
""")

# Footer
st.markdown("---")
st.markdown("© 2023 Regional Plan Association | Data Visualization Tool") 