import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os

# Add parent directory to path to access shared modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src.db.database import DatabaseConnection
from src.db.queries import LandUseQueries

# Set page configuration
st.set_page_config(
    page_title="Land Use Projections Dashboard | RPA Viewer",
    page_icon="🏞️",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main {
        background-color: #f8f9fa;
    }
    .title-text {
        color: #1e3a8a;
        font-size: 3rem;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .subtitle-text {
        color: #4b5563;
        font-size: 1.5rem;
        margin-bottom: 2rem;
    }
    .card {
        padding: 20px;
        border-radius: 5px;
        background-color: white;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 20px;
    }
    .info-card {
        background-color: #e7f5ff;
        border-left: 4px solid #1c7ed6;
        padding: 15px;
        margin-bottom: 20px;
        border-radius: 5px;
    }
    .metric-card {
        background-color: white;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        padding: 15px;
        text-align: center;
    }
    .metric-value {
        font-size: 2.2rem;
        font-weight: bold;
        color: #1e3a8a;
    }
    .metric-label {
        font-size: 1rem;
        color: #4b5563;
    }
    .nav-card {
        background-color: white;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        padding: 20px;
        margin-bottom: 15px;
        cursor: pointer;
        transition: transform 0.2s, box-shadow 0.2s;
    }
    .nav-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 6px 12px rgba(0, 0, 0, 0.1);
    }
    .nav-card-title {
        font-size: 1.25rem;
        font-weight: bold;
        color: #1e3a8a;
        margin-bottom: 10px;
    }
    .nav-card-desc {
        color: #4b5563;
        font-size: 0.9rem;
    }
    .footer {
        text-align: center;
        padding: 20px 0;
        color: #6b7280;
        font-size: 0.9rem;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def get_scenarios():
    """Get available scenarios from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT scenario_id, scenario_name, gcm, rcp, ssp FROM scenarios ORDER BY scenario_name")
    scenarios = [{'id': row[0], 'name': row[1], 'gcm': row[2], 'rcp': row[3], 'ssp': row[4]} for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return scenarios

@st.cache_data
def get_years():
    """Get a list of all available years from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT start_year FROM time_steps UNION SELECT DISTINCT end_year FROM time_steps ORDER BY 1")
    years = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return years

@st.cache_data
def get_land_use_types():
    """Get a list of all land use types from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT from_land_use AS land_use_type 
        FROM land_use_transitions
        UNION
        SELECT DISTINCT to_land_use AS land_use_type
        FROM land_use_transitions
        ORDER BY land_use_type
    """)
    land_use_types = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return land_use_types

@st.cache_data
def get_states():
    """Get a list of all states from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT state_fips, state_name, state_abbr FROM states ORDER BY state_name")
    states = [{'fips': row[0], 'name': row[1], 'abbr': row[2]} for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return states

@st.cache_data
def get_counties_by_state(state_fips=None):
    """Get a list of counties, optionally filtered by state."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    if state_fips:
        cursor.execute("""
            SELECT county_fips, county_name, state_fips, state_name, state_abbr
            FROM counties_by_state
            WHERE state_fips = ?
            ORDER BY county_name
        """, (state_fips,))
    else:
        cursor.execute("""
            SELECT county_fips, county_name, state_fips, state_name, state_abbr
            FROM counties_by_state
            ORDER BY state_name, county_name
        """)
    
    counties = [{'fips': row[0], 'name': row[1], 'state_fips': row[2], 
                'state_name': row[3], 'state_abbr': row[4]} for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return counties

@st.cache_data
def get_national_summary(scenario_id=None, year=None, aggregation_level="national"):
    """
    Get a summary of land use data at different aggregation levels.
    
    Args:
        scenario_id: Optional scenario ID to filter results
        year: Optional year to filter results
        aggregation_level: The level of aggregation - "national", "state", or "county"
    
    Returns:
        DataFrame containing the aggregated land use data
    """
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    # First, check if the scenario has data
    if scenario_id:
        cursor.execute(
            "SELECT COUNT(*) FROM land_use_transitions WHERE scenario_id = ?", 
            (scenario_id,)
        )
        count = cursor.fetchone()[0]
        if count == 0:
            # If no data for this scenario, find a scenario that has data
            cursor.execute(
                """
                SELECT scenario_id, COUNT(*) as count 
                FROM land_use_transitions 
                GROUP BY scenario_id 
                ORDER BY count DESC 
                LIMIT 1
                """
            )
            result = cursor.fetchone()
            if result:
                scenario_id = result[0]
                st.warning(f"Selected scenario has no data. Using scenario ID {scenario_id} instead.")
    
    # Find the time_step(s) that include this year (either as start or end year)
    year_filter = ""
    params = []
    if year:
        year_filter = "AND (ts.start_year = ? OR ts.end_year = ?)"
        params.extend([year, year])
    
    # This query aggregates data to get current land use state by calculating
    # net changes from transitions and assuming transitions happen at year boundary
    if aggregation_level == "national":
        query = """
        WITH transitions_summary AS (
            SELECT 
                lut.scenario_id,
                s.scenario_name,
                CASE 
                    WHEN ts.end_year = ? THEN ts.end_year
                    ELSE ts.start_year
                END as year,
                lut.from_land_use as land_use_type,
                -SUM(lut.acres) as net_change
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            WHERE 1=1
            {scenario_filter}
            {year_filter}
            GROUP BY lut.scenario_id, s.scenario_name, year, lut.from_land_use
            
            UNION ALL
            
            SELECT 
                lut.scenario_id,
                s.scenario_name,
                CASE 
                    WHEN ts.end_year = ? THEN ts.end_year
                    ELSE ts.start_year
                END as year,
                lut.to_land_use as land_use_type,
                SUM(lut.acres) as net_change
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            WHERE 1=1
            {scenario_filter}
            {year_filter}
            GROUP BY lut.scenario_id, s.scenario_name, year, lut.to_land_use
        )
        
        SELECT 
            scenario_id,
            scenario_name,
            year,
            land_use_type,
            SUM(net_change) as total_acres,
            NULL as state_fips,
            NULL as state_name,
            NULL as state_abbr
        FROM transitions_summary
        GROUP BY scenario_id, scenario_name, year, land_use_type
        ORDER BY scenario_name, year, land_use_type
        """
    elif aggregation_level == "state":
        query = """
        WITH state_transitions_summary AS (
            SELECT 
                lut.scenario_id,
                s.scenario_name,
                CASE 
                    WHEN ts.end_year = ? THEN ts.end_year
                    ELSE ts.start_year
                END as year,
                SUBSTR(lut.fips_code, 1, 2) as state_fips,
                st.state_name,
                st.state_abbr,
                lut.from_land_use as land_use_type,
                -SUM(lut.acres) as net_change
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            JOIN states st ON SUBSTR(lut.fips_code, 1, 2) = st.state_fips
            WHERE 1=1
            {scenario_filter}
            {year_filter}
            GROUP BY lut.scenario_id, s.scenario_name, year, state_fips, st.state_name, st.state_abbr, lut.from_land_use
            
            UNION ALL
            
            SELECT 
                lut.scenario_id,
                s.scenario_name,
                CASE 
                    WHEN ts.end_year = ? THEN ts.end_year
                    ELSE ts.start_year
                END as year,
                SUBSTR(lut.fips_code, 1, 2) as state_fips,
                st.state_name,
                st.state_abbr,
                lut.to_land_use as land_use_type,
                SUM(lut.acres) as net_change
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            JOIN states st ON SUBSTR(lut.fips_code, 1, 2) = st.state_fips
            WHERE 1=1
            {scenario_filter}
            {year_filter}
            GROUP BY lut.scenario_id, s.scenario_name, year, state_fips, st.state_name, st.state_abbr, lut.to_land_use
        )
        
        SELECT 
            scenario_id,
            scenario_name,
            year,
            land_use_type,
            SUM(net_change) as total_acres,
            state_fips,
            state_name,
            state_abbr
        FROM state_transitions_summary
        GROUP BY scenario_id, scenario_name, year, state_fips, state_name, state_abbr, land_use_type
        ORDER BY scenario_name, year, state_name, land_use_type
        """
    else:  # county level
        query = """
        WITH county_transitions_summary AS (
            SELECT 
                lut.scenario_id,
                s.scenario_name,
                CASE 
                    WHEN ts.end_year = ? THEN ts.end_year
                    ELSE ts.start_year
                END as year,
                lut.fips_code as county_fips,
                c.county_name,
                SUBSTR(lut.fips_code, 1, 2) as state_fips,
                st.state_name,
                st.state_abbr,
                lut.from_land_use as land_use_type,
                -SUM(lut.acres) as net_change
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            JOIN counties c ON lut.fips_code = c.fips_code
            JOIN states st ON SUBSTR(lut.fips_code, 1, 2) = st.state_fips
            WHERE 1=1
            {scenario_filter}
            {year_filter}
            GROUP BY lut.scenario_id, s.scenario_name, year, county_fips, c.county_name, state_fips, st.state_name, st.state_abbr, lut.from_land_use
            
            UNION ALL
            
            SELECT 
                lut.scenario_id,
                s.scenario_name,
                CASE 
                    WHEN ts.end_year = ? THEN ts.end_year
                    ELSE ts.start_year
                END as year,
                lut.fips_code as county_fips,
                c.county_name,
                SUBSTR(lut.fips_code, 1, 2) as state_fips,
                st.state_name,
                st.state_abbr,
                lut.to_land_use as land_use_type,
                SUM(lut.acres) as net_change
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            JOIN counties c ON lut.fips_code = c.fips_code
            JOIN states st ON SUBSTR(lut.fips_code, 1, 2) = st.state_fips
            WHERE 1=1
            {scenario_filter}
            {year_filter}
            GROUP BY lut.scenario_id, s.scenario_name, year, county_fips, c.county_name, state_fips, st.state_name, st.state_abbr, lut.to_land_use
        )
        
        SELECT 
            scenario_id,
            scenario_name,
            year,
            land_use_type,
            SUM(net_change) as total_acres,
            county_fips,
            county_name,
            state_fips,
            state_name,
            state_abbr
        FROM county_transitions_summary
        GROUP BY scenario_id, scenario_name, year, county_fips, county_name, state_fips, state_name, state_abbr, land_use_type
        ORDER BY scenario_name, year, state_name, county_name, land_use_type
        """
    
    # Add scenario filter if specified
    scenario_filter = ""
    if scenario_id:
        scenario_filter = "AND lut.scenario_id = ?"
        params.append(scenario_id)
    
    # Format the query with the appropriate filters
    query = query.format(
        scenario_filter=scenario_filter,
        year_filter=year_filter
    )
    
    # Add the year parameters for both subqueries
    # We use the first available year as a base year if none specified
    selected_year = year
    if not selected_year:
        cursor.execute("SELECT MIN(start_year) FROM time_steps")
        selected_year = cursor.fetchone()[0]
    
    # Add year parameters (needed twice for the two subqueries)
    full_params = [selected_year] + params + [selected_year] + params
    
    cursor.execute(query, full_params)
    data = cursor.fetchall()
    
    # Get column names
    columns = [desc[0] for desc in cursor.description]
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    
    # Handle empty results
    if df.empty:
        st.warning(f"No data found for the selected filters. Try different criteria.")
        # Create empty dataframe with expected columns
        df = pd.DataFrame(columns=columns)
    
    DatabaseConnection.close_connection(conn)
    return df

@st.cache_data
def get_database_stats():
    """Get statistics about the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    stats = {}
    
    # Count scenarios
    cursor.execute("SELECT COUNT(*) FROM scenarios")
    stats['scenario_count'] = cursor.fetchone()[0]
    
    # Count regions (counties)
    cursor.execute("SELECT COUNT(*) FROM counties")
    stats['region_count'] = cursor.fetchone()[0]
    
    # For region types, we only have counties in this database
    stats['region_type_count'] = 1
    
    # Count unique land use types (combining both from and to land uses)
    cursor.execute("""
        SELECT COUNT(DISTINCT land_use_type) 
        FROM (
            SELECT from_land_use AS land_use_type FROM land_use_transitions
            UNION
            SELECT to_land_use AS land_use_type FROM land_use_transitions
        )
    """)
    stats['land_use_type_count'] = cursor.fetchone()[0]
    
    # Count unique time steps
    cursor.execute("SELECT COUNT(*) FROM time_steps")
    stats['year_count'] = cursor.fetchone()[0]
    
    # Get total data points
    cursor.execute("SELECT COUNT(*) FROM land_use_transitions")
    stats['data_point_count'] = cursor.fetchone()[0]
    
    # Get min and max years
    cursor.execute("SELECT MIN(start_year), MAX(end_year) FROM time_steps")
    min_year, max_year = cursor.fetchone()
    stats['year_range'] = (min_year, max_year)
    
    DatabaseConnection.close_connection(conn)
    return stats

def create_timeline_chart(df, scenario_id=None):
    """Create a timeline chart of land use evolution."""
    if df.empty:
        return None
    
    # If scenario_id is provided, filter data
    if scenario_id:
        df = df[df['scenario_id'] == scenario_id]
    
    # Pivot data for timeline view
    pivot_df = df.pivot_table(
        index='year', 
        columns='land_use_type', 
        values='total_acres',
        aggfunc='sum'
    ).reset_index()
    
    # Melt the pivoted dataframe for plotly
    melted_df = pd.melt(
        pivot_df, 
        id_vars=['year'], 
        var_name='land_use_type', 
        value_name='acres'
    )
    
    # Create line chart
    fig = px.line(
        melted_df,
        x='year',
        y='acres',
        color='land_use_type',
        title='Land Use Evolution Over Time',
        labels={'year': 'Year', 'acres': 'Area (acres)', 'land_use_type': 'Land Use Type'},
        line_shape='linear',
        markers=True
    )
    
    # Update layout
    fig.update_layout(
        height=500,
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        margin=dict(l=50, r=50, t=80, b=50),
        hovermode='x unified'
    )
    
    return fig

def create_distribution_chart(df, year):
    """Create a pie chart of land use distribution for a specific year."""
    if df.empty:
        return None
    
    # Filter for the selected year
    year_df = df[df['year'] == year]
    
    if year_df.empty:
        return None
    
    # Group by land use type and sum the acres
    grouped_df = year_df.groupby('land_use_type')['total_acres'].sum().reset_index()
    
    # Calculate percentages
    total_acres = grouped_df['total_acres'].sum()
    grouped_df['percentage'] = (grouped_df['total_acres'] / total_acres) * 100
    
    # Create pie chart
    fig = px.pie(
        grouped_df,
        values='percentage',
        names='land_use_type',
        title=f'Land Use Distribution in {year}',
        hover_data=['total_acres'],
        labels={'percentage': 'Percentage (%)', 'total_acres': 'Area (acres)'},
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    
    # Update layout
    fig.update_layout(
        height=400,
        margin=dict(l=50, r=50, t=80, b=50),
        legend=dict(orientation='h', yanchor='bottom', y=-0.2, xanchor='center', x=0.5)
    )
    
    # Update hover template
    fig.update_traces(
        hovertemplate='<b>%{label}</b><br>Percentage: %{value:.1f}%<br>Area: %{customdata[0]:,.0f} acres'
    )
    
    return fig

def create_scenario_comparison(df, year):
    """Create a comparison of land use across different scenarios for a specific year."""
    if df.empty:
        return None
    
    # Filter for the selected year
    year_df = df[df['year'] == year]
    
    if year_df.empty:
        return None
    
    # Create a grouped bar chart
    fig = px.bar(
        year_df,
        x='land_use_type',
        y='total_acres',
        color='scenario_name',
        barmode='group',
        title=f'Scenario Comparison for {year}',
        labels={'land_use_type': 'Land Use Type', 'total_acres': 'Area (acres)', 'scenario_name': 'Scenario'},
        height=500
    )
    
    # Update layout
    fig.update_layout(
        margin=dict(l=50, r=50, t=80, b=100),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        xaxis=dict(tickangle=45)
    )
    
    return fig

def main():
    # Page header
    st.markdown('<h1 class="title-text">National Land Use Projections Dashboard</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle-text">Explore and analyze future land use scenarios across the United States</p>', unsafe_allow_html=True)
    
    # Database statistics
    stats = get_database_stats()
    
    # Information card
    st.markdown('<div class="info-card">', unsafe_allow_html=True)
    st.markdown("""
    This dashboard provides interactive visualizations of projected land use changes across the United States. 
    The projections are based on climate scenarios, socioeconomic pathways, and other factors that may influence
    future land use patterns. Use the sidebar to filter the data and explore different analysis pages.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Key metrics
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### Database Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{stats["scenario_count"]}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Scenarios</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{stats["region_count"]}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Counties</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{stats["land_use_type_count"]}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Land Use Types</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        year_range = stats["year_range"]
        st.markdown(f'<div class="metric-value">{year_range[0]}-{year_range[1]}</div>', unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Year Range</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Sidebar filters
    st.sidebar.header("Data Filters")
    
    # Get data for filters
    scenarios = get_scenarios()
    years = get_years()
    states = get_states()
    
    # Get default scenario with data
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT scenario_id, COUNT(*) as count 
        FROM land_use_transitions 
        GROUP BY scenario_id 
        ORDER BY count DESC 
        LIMIT 1
        """
    )
    result = cursor.fetchone()
    default_scenario_id = result[0] if result else None
    DatabaseConnection.close_connection(conn)
    
    # Find the index of the default scenario
    default_scenario_index = 0
    if default_scenario_id:
        for i, s in enumerate(scenarios):
            if s['id'] == default_scenario_id:
                default_scenario_index = i
                break
    
    # Scenario selection
    scenario_options = [f"{s['name']} ({s['gcm']}, {s['rcp']}-{s['ssp']})" for s in scenarios]
    selected_scenario = st.sidebar.selectbox(
        "Select Scenario", 
        scenario_options, 
        index=default_scenario_index
    )
    selected_scenario_id = scenarios[scenario_options.index(selected_scenario)]['id']
    
    # Year selection
    selected_year = st.sidebar.selectbox("Select Year", years, index=0)
    
    # Region selection
    aggregation_level = st.sidebar.radio("Geographic Level", ["National", "State", "County"], index=0)
    
    # Conditional state filter based on aggregation level
    selected_state_fips = None
    if aggregation_level in ["State", "County"]:
        state_options = [f"{s['name']} ({s['abbr']})" for s in states]
        if aggregation_level == "State":
            state_label = "Filter by State"
            st.sidebar.markdown("**Note:** Selecting a state will show data only for that state.")
            
            # Add "All States" option for State level
            state_options = ["All States"] + state_options
            selected_state = st.sidebar.selectbox(state_label, state_options)
            if selected_state != "All States":
                selected_state_fips = states[state_options.index(selected_state)-1]['fips']
        else:
            state_label = "Select State for County Data"
            selected_state = st.sidebar.selectbox(state_label, state_options)
            selected_state_index = state_options.index(selected_state)
            selected_state_fips = states[selected_state_index]['fips']
    
    # County selection (only shown for county-level analysis)
    selected_county_fips = None
    if aggregation_level == "County" and selected_state_fips:
        counties = get_counties_by_state(selected_state_fips)
        if counties:
            county_options = [f"{c['name']} ({c['state_abbr']})" for c in counties]
            selected_county = st.sidebar.selectbox("Select County", county_options)
            selected_county_index = county_options.index(selected_county)
            selected_county_fips = counties[selected_county_index]['fips']
    
    # Get data based on selection
    aggregation_level_param = aggregation_level.lower()
    
    with st.spinner("Loading data..."):
        summary_data = get_national_summary(selected_scenario_id, selected_year, aggregation_level_param)
        
        # Filter by state or county if needed
        if selected_state_fips and aggregation_level == "State":
            filtered_data = summary_data[summary_data['state_fips'] == selected_state_fips]
            if not filtered_data.empty:
                summary_data = filtered_data
            else:
                st.warning(f"No data available for the selected state. Showing all states instead.")
        elif selected_county_fips:
            filtered_data = summary_data[summary_data['county_fips'] == selected_county_fips]
            if not filtered_data.empty:
                summary_data = filtered_data
            else:
                st.warning(f"No data available for the selected county. Showing all counties in state instead.")
                if selected_state_fips:
                    summary_data = summary_data[summary_data['state_fips'] == selected_state_fips]
    
    # Main content
    st.markdown('<div class="card">', unsafe_allow_html=True)
    
    # Dynamic title based on aggregation level
    title_prefix = {
        "national": "National", 
        "state": "State-Level", 
        "county": "County-Level"
    }[aggregation_level_param]
    
    # Additional title modifier for selected state/county
    title_modifier = ""
    if selected_state_fips and aggregation_level in ["State", "County"]:
        state_info = next((s for s in states if s['fips'] == selected_state_fips), None)
        if state_info:
            title_modifier = f" - {state_info['name']}"
            if selected_county_fips and aggregation_level == "County":
                county_info = next((c for c in counties if c['fips'] == selected_county_fips), None)
                if county_info:
                    title_modifier += f", {county_info['name']}"
    
    # Timeline chart
    st.subheader(f"{title_prefix}{title_modifier} Land Use Trends")
    
    if not summary_data.empty:
        timeline_chart = create_timeline_chart(summary_data, selected_scenario_id)
        if timeline_chart:
            st.plotly_chart(timeline_chart, use_container_width=True)
        else:
            st.info("No data available for the timeline chart.")
    else:
        st.info("No data available for the selected filters.")
    
    # Distribution and comparison charts
    if not summary_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader(f"Land Use Distribution ({selected_year})")
            dist_chart = create_distribution_chart(summary_data, selected_year)
            if dist_chart:
                st.plotly_chart(dist_chart, use_container_width=True)
            else:
                st.info(f"No distribution data available for {selected_year}.")
        
        with col2:
            st.subheader("Scenario Comparison")
            comp_chart = create_scenario_comparison(summary_data, selected_year)
            if comp_chart:
                st.plotly_chart(comp_chart, use_container_width=True)
            else:
                st.info(f"No comparison data available for {selected_year}.")
    
    # State-level breakdown (only shown for national view)
    if aggregation_level == "National":
        st.subheader("State-Level Breakdown")
        
        # Get state-level data
        with st.spinner("Loading state-level data..."):
            state_data = get_national_summary(selected_scenario_id, selected_year, "state")
            
            if not state_data.empty:
                # Handle only numeric columns to avoid the pivot_table warning
                numeric_state_data = state_data[['state_name', 'state_abbr', 'land_use_type', 'total_acres']].copy()
                
                # Create a stacked bar chart for states
                fig = go.Figure()
                
                # Group by state and land use type first to avoid pivot_table warning
                grouped_data = numeric_state_data.groupby(['state_name', 'state_abbr', 'land_use_type'])['total_acres'].sum().reset_index()
                
                # Get unique land use types
                land_use_types = grouped_data['land_use_type'].unique()
                state_names = grouped_data['state_name'].unique()
                
                # Create the chart
                for land_use in land_use_types:
                    land_use_data = grouped_data[grouped_data['land_use_type'] == land_use]
                    fig.add_trace(go.Bar(
                        x=land_use_data['state_name'],
                        y=land_use_data['total_acres'],
                        name=land_use,
                        text=land_use_data['total_acres'].apply(lambda x: f'{x:,.0f} acres' if not pd.isna(x) else '')
                    ))
                
                fig.update_layout(
                    title=f'State-Level Land Use Distribution ({selected_year})',
                    barmode='stack',
                    height=600,
                    xaxis=dict(
                        title='State',
                        tickangle=45
                    ),
                    yaxis=dict(title='Acres'),
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    )
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No state-level data available for the selected filters.")
    
    # Show state details when a specific state is selected
    elif aggregation_level == "State" and selected_state_fips:
        selected_state_info = next((s for s in states if s['fips'] == selected_state_fips), None)
        if selected_state_info:
            st.subheader(f"{selected_state_info['name']} ({selected_state_info['abbr']}) Details")
            
            # Get county-level data for the selected state
            county_data = get_counties_by_state(selected_state_fips)
            
            # Display county count
            st.info(f"{selected_state_info['name']} contains {len(county_data)} counties")
            
            # Group land use data by county
            state_land_use = summary_data.copy()
            
            # Get county-level data for this state
            with st.spinner("Loading county-level data..."):
                county_level_data = get_national_summary(
                    selected_scenario_id, 
                    selected_year, 
                    "county"
                )
                county_level_data = county_level_data[
                    county_level_data['state_fips'] == selected_state_fips
                ]
                
                if not county_level_data.empty:
                    # Create a grouped bar chart for counties
                    fig = px.bar(
                        county_level_data,
                        x='county_name',
                        y='total_acres',
                        color='land_use_type',
                        title=f'Land Use by County in {selected_state_info["name"]} ({selected_year})',
                        labels={'county_name': 'County', 'total_acres': 'Acres', 'land_use_type': 'Land Use Type'},
                        height=500
                    )
                    
                    fig.update_layout(
                        xaxis=dict(tickangle=45),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"No county-level data available for {selected_state_info['name']} in {selected_year}")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Navigation cards to other pages
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### Explore Analysis Pages")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(
            """
            <div class="nav-card" onclick="window.location.href='/Transition_Analysis'">
                <div class="nav-card-title">🔄 Transition Analysis</div>
                <div class="nav-card-desc">
                    Analyze land use transitions between different time periods, 
                    visualize flows between land use types, and identify key patterns of change.
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    with col2:
        st.markdown(
            """
            <div class="nav-card" onclick="window.location.href='/Spatial_Analysis'">
                <div class="nav-card-title">🗺️ Spatial Analysis</div>
                <div class="nav-card-desc">
                    Explore the geographic distribution of land use changes across regions,
                    with interactive maps and regional comparisons.
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Footer
    st.markdown('<div class="footer">', unsafe_allow_html=True)
    st.markdown("Data source: National Land Use Projection Database | Developed for the U.S. Forest Service's Resource Planning Act Assessment")
    st.markdown("© 2025 | All Rights Reserved")
    st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main() 