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
    cursor.execute("SELECT DISTINCT year FROM land_use_projections ORDER BY year")
    years = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return years

@st.cache_data
def get_land_use_types():
    """Get a list of all land use types from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT land_use_type 
        FROM land_use_projections
        ORDER BY land_use_type
    """)
    land_use_types = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return land_use_types

@st.cache_data
def get_national_summary(scenario_id=None, year=None):
    """Get a summary of national land use data."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT lp.land_use_type, SUM(lp.acres) as total_acres, 
           lp.year, s.scenario_name, s.scenario_id
    FROM land_use_projections lp
    JOIN scenarios s ON lp.scenario_id = s.scenario_id
    """
    
    params = []
    if scenario_id:
        query += " WHERE lp.scenario_id = ?"
        params.append(scenario_id)
        if year:
            query += " AND lp.year = ?"
            params.append(year)
    elif year:
        query += " WHERE lp.year = ?"
        params.append(year)
    
    query += " GROUP BY lp.land_use_type, lp.year, s.scenario_id ORDER BY lp.year, lp.land_use_type"
    
    cursor.execute(query, params)
    data = cursor.fetchall()
    
    # Get column names
    columns = [desc[0] for desc in cursor.description]
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    
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
    
    # Count regions
    cursor.execute("SELECT COUNT(*) FROM regions")
    stats['region_count'] = cursor.fetchone()[0]
    
    # Count region types
    cursor.execute("SELECT COUNT(DISTINCT region_type) FROM regions")
    stats['region_type_count'] = cursor.fetchone()[0]
    
    # Count land use types
    cursor.execute("SELECT COUNT(DISTINCT land_use_type) FROM land_use_projections")
    stats['land_use_type_count'] = cursor.fetchone()[0]
    
    # Count years
    cursor.execute("SELECT COUNT(DISTINCT year) FROM land_use_projections")
    stats['year_count'] = cursor.fetchone()[0]
    
    # Get total data points
    cursor.execute("SELECT COUNT(*) FROM land_use_projections")
    stats['data_point_count'] = cursor.fetchone()[0]
    
    # Get min and max years
    cursor.execute("SELECT MIN(year), MAX(year) FROM land_use_projections")
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
        st.markdown('<div class="metric-label">Regions</div>', unsafe_allow_html=True)
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
    
    # Main content
    st.markdown('<div class="card">', unsafe_allow_html=True)
    
    # Sidebar filters
    st.sidebar.header("Data Filters")
    
    # Get data for filters
    scenarios = get_scenarios()
    years = get_years()
    
    # Scenario selection
    scenario_options = [f"{s['name']} ({s['gcm']}, {s['rcp']}-{s['ssp']})" for s in scenarios]
    selected_scenario = st.sidebar.selectbox("Select Default Scenario", scenario_options, index=0)
    selected_scenario_id = scenarios[scenario_options.index(selected_scenario)]['id']
    
    # Year selection
    selected_year = st.sidebar.selectbox("Select Year", years, index=0)
    
    # Get data based on selection
    summary_data = get_national_summary()
    
    # Timeline chart
    st.subheader("National Land Use Trends")
    timeline_chart = create_timeline_chart(summary_data, selected_scenario_id)
    if timeline_chart:
        st.plotly_chart(timeline_chart, use_container_width=True)
    else:
        st.info("No data available for the timeline chart.")
    
    # Distribution and comparison charts
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