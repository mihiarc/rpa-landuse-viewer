"""
RPA Land Use Viewer - Main Application
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import os

# Import shared utility modules
from src.utils.shared import (
    get_scenarios,
    get_years,
    get_states,
    get_counties_by_state,
    get_national_summary,
    load_css,
    set_page_config,
    display_metrics
)
from src.utils.data_processing import (
    filter_data,
    aggregate_data
)
from src.utils.data import (
    calculate_percentage,
    get_summary_statistics as calculate_metrics
)
from src.utils.visualizations import (
    create_choropleth_map,
    create_bar_chart,
    create_pie_chart,
    create_line_chart
)

# Set page configuration
set_page_config(
    title="RPA Land Use Viewer",
    icon="🌆",
    layout="wide"
)

# Load custom CSS
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
scenarios = get_scenarios()
scenario_names = [f"{s['name']} ({s['gcm']}_{s['rcp']}_{s['ssp']})" for s in scenarios]
selected_scenario_idx = st.sidebar.selectbox(
    "Select Scenario",
    range(len(scenarios)),
    format_func=lambda i: scenario_names[i]
)
selected_scenario = scenarios[selected_scenario_idx]

# Year selection
year = st.sidebar.selectbox(
    "Select Year",
    get_years()
)

# Geographic filters
st.sidebar.header("Geographic Filters")

# State selection
states_data = get_states()
state_options = ["All States"] + [state['name'] for state in states_data]
selected_state_name = st.sidebar.selectbox(
    "Select State",
    state_options
)

# County selection (dependent on state)
counties = ["All Counties"]
selected_state_fips = None

if selected_state_name != "All States":
    # Find the selected state in our data to get its FIPS code
    for state in states_data:
        if state['name'] == selected_state_name:
            selected_state_fips = state['fips']
            break
    
    if selected_state_fips:
        counties_data = get_counties_by_state(selected_state_fips)
        counties += [county['name'] for county in counties_data]

selected_county = st.sidebar.selectbox(
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
    # Pass only the scenario ID to the load_scenario_data function
    df = get_national_summary(selected_scenario['id'], year)
    
    # Check if data was loaded successfully
    if df.empty:
        st.error("No data available for the selected scenario and year.")
        st.stop()
    
    # Apply geographic filters
    state_filter = selected_state_fips if selected_state_name != "All States" else None
    county_filter = selected_county if selected_county != "All Counties" else None
    
    filtered_df = filter_data(df, state_filter, county_filter)
    
    # Check if filtered data is empty
    if filtered_df.empty:
        st.warning("No data available for the selected geographic filters.")
        st.stop()

# Display metrics
st.header("Key Metrics")
metrics = calculate_metrics(filtered_df)
display_metrics(metrics)

# Display visualizations
st.header("Visualization")

if vis_type == "Map":
    st.subheader("Geographic Distribution")
    
    # Check if necessary columns for choropleth exist
    if 'state' in filtered_df.columns and any(col for col in filtered_df.columns if col.endswith('_acres')):
        # Find the first column that ends with '_acres' for the choropleth
        value_col = next((col for col in filtered_df.columns if col.endswith('_acres')), None)
        
        if value_col:
            fig = create_choropleth_map(
                filtered_df,
                geo_col='state',
                value_col=value_col,
                title=f"{value_col.replace('_', ' ').title()} by State ({year})"
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
            
            fig = create_bar_chart(
                chart_df,
                x_col='county',
                y_col=value_col,
                title=f"{value_col.replace('_', ' ').title()} by County ({year})"
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
        
        fig = create_pie_chart(
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

# Display footer
st.markdown("""
<div class="footer">
    <p>© 2024 RPA Land Use Viewer | Developed by the RPA Land Use Team</p>
</div>
""", unsafe_allow_html=True) 