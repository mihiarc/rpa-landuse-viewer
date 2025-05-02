"""
RPA Land Use Viewer - Main Application
"""

import streamlit as st
import pandas as pd
import duckdb
import os
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up page configuration
st.set_page_config(
    page_title="RPA Land Use Viewer",
    page_icon="🌆",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load custom CSS if available
css_file = Path("src/styles/style.css")
if css_file.exists():
    with open(css_file) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Application title and description
st.title("RPA Land Use Viewer")
st.markdown("""
This application allows you to explore land use scenarios for the Regional Plan Association (RPA) region.
Select different scenarios, years, and geographic areas to visualize the data.
""")

# Get database path from environment
DB_PATH = os.getenv('DB_PATH', 'data/database/rpa_landuse.db')

# Database connection functions
@st.cache_resource
def get_connection():
    """Create and cache a database connection"""
    return duckdb.connect(DB_PATH)

@st.cache_data(ttl=3600)
def get_scenarios():
    """Get scenarios from database"""
    conn = get_connection()
    results = conn.execute(
        "SELECT scenario_id, scenario_name, gcm, rcp, ssp FROM scenarios ORDER BY scenario_name"
    ).fetchall()
    
    return [{'id': row[0], 'name': row[1], 'gcm': row[2], 'rcp': row[3], 'ssp': row[4]} for row in results]

@st.cache_data(ttl=3600)
def get_years():
    """Get years from database"""
    conn = get_connection()
    results = conn.execute(
        "SELECT DISTINCT year FROM land_use_summary ORDER BY year"
    ).fetchall()
    
    return [row[0] for row in results]

@st.cache_data(ttl=3600)
def get_states():
    """Get states from database"""
    conn = get_connection()
    results = conn.execute(
        "SELECT state_fips, state_name FROM states ORDER BY state_name"
    ).fetchall()
    
    return [{'fips': row[0], 'name': row[1]} for row in results]

@st.cache_data(ttl=3600)
def get_counties_by_state(state_fips):
    """Get counties for a specific state"""
    conn = get_connection()
    results = conn.execute("""
        SELECT fips_code, county_name 
        FROM counties 
        WHERE SUBSTRING(fips_code, 1, 2) = ?
        ORDER BY county_name
    """, [state_fips]).fetchall()
    
    return [{'fips': row[0], 'name': row[1]} for row in results]

@st.cache_data(ttl=1800)
def get_land_use_data(scenario_id, year, state=None, county=None):
    """Get land use data filtered by scenario, year, and geography"""
    conn = get_connection()
    
    # Base query
    query = """
    SELECT *
    FROM land_use_summary
    WHERE scenario_id = ? AND year = ?
    """
    params = [scenario_id, year]
    
    # Add filters for state and county if provided
    if state:
        query += " AND state = ?"
        params.append(state)
    
    if county:
        query += " AND county = ?"
        params.append(county)
    
    # Execute query and convert to pandas DataFrame
    result = conn.execute(query, params).fetch_df()
    return result

def calculate_metrics(df):
    """Calculate summary metrics from the DataFrame"""
    metrics = {}
    
    # Total land area
    if 'total_acres' in df.columns:
        metrics['total_acres'] = df['total_acres'].sum()
    
    # Calculate acreage by land use type
    land_use_types = ['cropland', 'pasture', 'range', 'forest', 'urban']
    for land_type in land_use_types:
        col_name = f"{land_type}_acres"
        if col_name in df.columns:
            metrics[col_name] = df[col_name].sum()
            
            # Calculate percentage 
            if metrics['total_acres'] > 0:
                metrics[f"{land_type}_percent"] = (metrics[col_name] / metrics['total_acres']) * 100
    
    return metrics

def format_metric(value, is_percent=False):
    """Format metric values for display"""
    if is_percent:
        return f"{value:.1f}%"
    elif value >= 1_000_000:
        return f"{value/1_000_000:.2f}M"
    elif value >= 1_000:
        return f"{value/1_000:.1f}K"
    else:
        return f"{value:,.0f}"

def create_choropleth_map(df, geo_col, value_col, title):
    """Create a choropleth map for geographic data"""
    fig = px.choropleth(
        df,
        locations=geo_col,
        locationmode='USA-states' if geo_col == 'state' else 'geojson-id',
        color=value_col,
        color_continuous_scale="Viridis",
        scope="usa",
        title=title,
        labels={value_col: value_col.replace('_', ' ').title()}
    )
    
    # Improve layout
    fig.update_layout(
        margin={"r": 0, "t": 40, "l": 0, "b": 0},
        coloraxis_colorbar=dict(
            title=value_col.replace('_', ' ').title(),
            thicknessmode="pixels", 
            thickness=20,
            lenmode="pixels", 
            len=300
        )
    )
    
    return fig

def create_bar_chart(df, x_col, y_col, title):
    """Create a bar chart for comparing values"""
    # Format axis titles
    x_title = x_col.replace('_', ' ').title()
    y_title = y_col.replace('_', ' ').title()
    
    # Limit data if too many rows
    if len(df) > 30:
        df = df.sort_values(by=y_col, ascending=False).head(30)
    
    # Create the bar chart
    fig = px.bar(
        df,
        x=x_col,
        y=y_col,
        title=title,
        labels={
            x_col: x_title,
            y_col: y_title
        }
    )
    
    # Improve layout
    fig.update_layout(
        margin={"r": 20, "t": 40, "l": 20, "b": 20},
        xaxis_title=x_title,
        yaxis_title=y_title
    )
    
    return fig

def create_pie_chart(df, values_col, names_col, title):
    """Create a pie chart for showing proportions"""
    fig = px.pie(
        df,
        values=values_col,
        names=names_col,
        title=title,
        hole=0.4,
        labels={
            values_col: values_col.replace('_', ' ').title(),
            names_col: names_col.replace('_', ' ').title()
        }
    )
    
    # Improve layout
    fig.update_layout(
        margin={"r": 20, "t": 40, "l": 20, "b": 20},
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        )
    )
    
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        insidetextfont=dict(size=12)
    )
    
    return fig

# Sidebar for inputs
st.sidebar.header("Data Selection")

# Check if database exists
if not Path(DB_PATH).exists():
    st.error(f"Database file not found: {DB_PATH}")
    st.stop()

# Check if land_use_summary table exists
conn = get_connection()
try:
    tables = [row[0] for row in conn.execute('SHOW TABLES').fetchall()]
    if 'land_use_summary' not in tables:
        st.error("""
        The land_use_summary table does not exist in the database. 
        
        Please run the database initialization scripts to create and populate this table.
        
        Check the documentation or README file for instructions on setting up the database.
        """)
        st.stop()
except Exception as e:
    st.error(f"Error accessing database: {e}")
    st.stop()

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
years = get_years()
if not years:
    st.error("No years available in the database.")
    st.stop()

year = st.sidebar.selectbox(
    "Select Year",
    years
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

# Load and filter data
with st.spinner("Loading data..."):
    # Get county FIPS code if a county was selected
    county_fips = None
    if selected_county != "All Counties" and selected_state_fips:
        counties_data = get_counties_by_state(selected_state_fips)
        for county in counties_data:
            if county['name'] == selected_county:
                county_fips = county['fips']
                break
    
    # Get data from database
    df = get_land_use_data(
        selected_scenario['id'], 
        year, 
        state=selected_state_fips if selected_state_name != "All States" else None,
        county=county_fips if selected_county != "All Counties" else None
    )
    
    # Check if data is empty
    if df.empty:
        st.warning("No data available for the selected filters.")
        st.stop()

# Display metrics
st.header("Key Metrics")
metrics = calculate_metrics(df)

# Display metrics in columns
metric_cols = st.columns(len(metrics))
for i, (metric_name, value) in enumerate(metrics.items()):
    is_percent = 'percent' in metric_name
    formatted_name = metric_name.replace('_', ' ').title()
    formatted_value = format_metric(value, is_percent=is_percent)
    metric_cols[i].metric(formatted_name, formatted_value)

# Display visualizations
st.header("Visualization")

if vis_type == "Map":
    st.subheader("Geographic Distribution")
    
    if 'state' in df.columns and any(col for col in df.columns if col.endswith('_acres')):
        # Find the first column that ends with '_acres' for the choropleth
        value_col = next((col for col in df.columns if col.endswith('_acres')), None)
        
        if value_col:
            fig = create_choropleth_map(
                df,
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
    
    if 'county' in df.columns and any(col for col in df.columns if col.endswith('_acres')):
        # Find the first column that ends with '_acres' for the bar chart
        value_col = next((col for col in df.columns if col.endswith('_acres')), None)
        
        if value_col:
            # Aggregate data by county if needed
            if 'county_name' in df.columns:
                chart_df = df.groupby('county_name')[value_col].sum().reset_index()
                chart_df.columns = ['county', value_col]
            else:
                chart_df = df
            
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
    
    # Get land use columns (columns ending with _acres except total_acres)
    land_use_cols = [col for col in df.columns if col.endswith('_acres') and col != 'total_acres']
    
    if land_use_cols:
        # Create a new dataframe with land use types and their values
        pie_data = pd.DataFrame({
            'land_use_type': [col.replace('_acres', '').replace('_', ' ').title() for col in land_use_cols],
            'acres': [df[col].sum() for col in land_use_cols]
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