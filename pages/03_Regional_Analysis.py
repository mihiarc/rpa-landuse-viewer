import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from dotenv import load_dotenv
import sys
import os
import json
from datetime import datetime

# Add parent directory to path to access shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.db.database import DatabaseConnection
from src.db.queries import LandUseQueries

# Page config
st.set_page_config(
    page_title="Regional Analysis | RPA Viewer",
    page_icon="🌏",
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
        font-size: 2.5rem;
        font-weight: bold;
        margin-bottom: 1rem;
    }
    .subtitle-text {
        color: #4b5563;
        font-size: 1.25rem;
        margin-bottom: 2rem;
    }
    .card {
        padding: 20px;
        border-radius: 5px;
        background-color: white;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 20px;
    }
    .comparison-container {
        display: flex;
        justify-content: space-between;
        gap: 20px;
    }
    .metric-container {
        display: flex;
        flex-wrap: wrap;
        gap: 15px;
    }
    .metric-card {
        padding: 15px;
        border-radius: 5px;
        background-color: white;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        flex: 1;
        min-width: 200px;
        text-align: center;
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
def get_years():
    """Get a list of all available years from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT year FROM land_use_projections ORDER BY year")
    years = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return years

@st.cache_data
def get_region_types():
    """Get a list of all region types from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT region_type FROM regions ORDER BY region_type")
    region_types = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return region_types

@st.cache_data
def get_regions(region_type=None):
    """Get a list of regions, optionally filtered by type."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT region_id, region_name, region_type 
        FROM regions 
    """
    
    params = []
    if region_type:
        query += " WHERE region_type = ?"
        params.append(region_type)
    
    query += " ORDER BY region_type, region_name"
    
    cursor.execute(query, params)
    regions = [{'id': row[0], 'name': row[1], 'type': row[2]} for row in cursor.fetchall()]
    
    DatabaseConnection.close_connection(conn)
    return regions

@st.cache_data
def get_land_use_distribution(scenario_id, year, region_ids):
    """Get land use distribution for multiple regions for comparison."""
    if not region_ids:
        return pd.DataFrame()
    
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    # Build query with parameterized IN clause for region_ids
    placeholders = ', '.join(['?' for _ in region_ids])
    query = f"""
        SELECT r.region_id, r.region_name, r.region_type, 
               lp.land_use_type, lp.acres, lp.percent_of_region,
               s.scenario_name, lp.year
        FROM land_use_projections lp
        JOIN regions r ON lp.region_id = r.region_id
        JOIN scenarios s ON lp.scenario_id = s.scenario_id
        WHERE lp.scenario_id = ? 
          AND lp.year = ?
          AND r.region_id IN ({placeholders})
        ORDER BY r.region_name, lp.land_use_type
    """
    
    params = [scenario_id, year] + region_ids
    cursor.execute(query, params)
    
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    
    DatabaseConnection.close_connection(conn)
    return df

@st.cache_data
def get_land_use_trends(scenario_id, years, region_id, land_use_type=None):
    """Get land use trends over time for a specific region."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT r.region_id, r.region_name, r.region_type, 
               lp.land_use_type, lp.acres, lp.percent_of_region,
               s.scenario_name, lp.year
        FROM land_use_projections lp
        JOIN regions r ON lp.region_id = r.region_id
        JOIN scenarios s ON lp.scenario_id = s.scenario_id
        WHERE lp.scenario_id = ? 
          AND r.region_id = ?
    """
    
    params = [scenario_id, region_id]
    
    if land_use_type:
        query += " AND lp.land_use_type = ?"
        params.append(land_use_type)
    
    if years and len(years) > 0:
        year_placeholders = ', '.join(['?' for _ in years])
        query += f" AND lp.year IN ({year_placeholders})"
        params.extend(years)
    
    query += " ORDER BY lp.year, lp.land_use_type"
    
    cursor.execute(query, params)
    
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    
    DatabaseConnection.close_connection(conn)
    return df

def create_distribution_comparison(df, value_column='percent_of_region', title="Land Use Distribution Comparison"):
    """Create a bar chart comparing land use distribution across regions."""
    if df.empty:
        return None
    
    fig = px.bar(
        df, 
        x='land_use_type', 
        y=value_column, 
        color='region_name',
        barmode='group',
        labels={
            'land_use_type': 'Land Use Type',
            'percent_of_region': 'Percent of Region (%)',
            'acres': 'Area (acres)',
            'region_name': 'Region'
        },
        title=title
    )
    
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=500,
        margin=dict(l=40, r=40, t=80, b=80)
    )
    
    return fig

def create_stacked_comparison(df, value_column='percent_of_region', title="Land Use Composition by Region"):
    """Create a stacked bar chart showing land use composition for each region."""
    if df.empty:
        return None
    
    fig = px.bar(
        df, 
        x='region_name', 
        y=value_column, 
        color='land_use_type',
        labels={
            'land_use_type': 'Land Use Type',
            'percent_of_region': 'Percent of Region (%)',
            'acres': 'Area (acres)',
            'region_name': 'Region'
        },
        title=title
    )
    
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=500,
        margin=dict(l=40, r=40, t=80, b=80)
    )
    
    return fig

def create_time_series(df, value_column='percent_of_region', title="Land Use Change Over Time"):
    """Create a line chart showing land use changes over time."""
    if df.empty:
        return None
    
    fig = px.line(
        df, 
        x='year', 
        y=value_column, 
        color='land_use_type',
        markers=True,
        labels={
            'year': 'Year',
            'percent_of_region': 'Percent of Region (%)',
            'acres': 'Area (acres)',
            'land_use_type': 'Land Use Type'
        },
        title=title
    )
    
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=500,
        margin=dict(l=40, r=40, t=80, b=80)
    )
    
    return fig

def create_regional_trend_chart(df, region_name, land_use_type, value_column='percent_of_region'):
    """Create a chart showing the trend of a specific land use type in a region."""
    if df.empty:
        return None
    
    filtered_df = df[df['land_use_type'] == land_use_type].copy()
    if filtered_df.empty:
        return None
    
    # Calculate changes between consecutive years
    filtered_df = filtered_df.sort_values('year')
    filtered_df['change'] = filtered_df[value_column].diff()
    filtered_df['growth_rate'] = filtered_df[value_column].pct_change() * 100
    
    # Create figure with primary Y axis
    fig = go.Figure()
    
    # Add line for the main value
    fig.add_trace(go.Scatter(
        x=filtered_df['year'],
        y=filtered_df[value_column],
        name=value_column.replace('_', ' ').title(),
        line=dict(color='royalblue', width=3),
        mode='lines+markers'
    ))
    
    # Add bar chart for the change on secondary Y axis
    fig.add_trace(go.Bar(
        x=filtered_df['year'],
        y=filtered_df['change'],
        name='Change',
        marker=dict(color='rgba(255, 99, 71, 0.6)'),
        yaxis='y2'
    ))
    
    # Set up the layout with two Y axes
    fig.update_layout(
        title=f"{land_use_type} Trends in {region_name}",
        xaxis=dict(title='Year'),
        yaxis=dict(
            title=value_column.replace('_', ' ').title(),
            titlefont=dict(color='royalblue'),
            tickfont=dict(color='royalblue')
        ),
        yaxis2=dict(
            title='Change',
            titlefont=dict(color='tomato'),
            tickfont=dict(color='tomato'),
            anchor='x',
            overlaying='y',
            side='right'
        ),
        height=500,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        margin=dict(l=40, r=40, t=80, b=40)
    )
    
    return fig

def create_land_use_pie_chart(df, region_name, year, value_column='percent_of_region'):
    """Create a pie chart showing land use distribution for a region in a specific year."""
    if df.empty:
        return None
    
    fig = px.pie(
        df,
        values=value_column,
        names='land_use_type',
        title=f"Land Use Distribution in {region_name} ({year})",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Bold
    )
    
    fig.update_layout(
        height=500,
        margin=dict(l=20, r=20, t=60, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
    )
    
    return fig

def calculate_metrics(df, region_name, year):
    """Calculate key metrics for a region in a specific year."""
    if df.empty:
        return []
    
    region_df = df[df['region_name'] == region_name].copy()
    if region_df.empty:
        return []
    
    # Total area
    total_area = region_df['acres'].sum()
    
    # Most common land use
    most_common = region_df.loc[region_df['percent_of_region'].idxmax()]
    most_common_type = most_common['land_use_type']
    most_common_pct = most_common['percent_of_region']
    
    # Calculate diversity (number of land use types with >5% share)
    diverse_types = len(region_df[region_df['percent_of_region'] > 5])
    
    return [
        {"label": "Total Area", "value": f"{total_area:,.0f} acres"},
        {"label": "Dominant Land Use", "value": f"{most_common_type} ({most_common_pct:.1f}%)"},
        {"label": "Land Use Diversity", "value": f"{diverse_types} types >5%"}
    ]

def main():
    # Page header
    st.markdown('<div class="title-text">Regional Land Use Analysis</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="subtitle-text">Compare land use distributions and changes across different regions</div>', 
        unsafe_allow_html=True
    )
    
    # Sidebar for filters
    with st.sidebar:
        st.header("Analysis Parameters")
        
        # Get data for filters
        scenarios = get_scenarios()
        scenario_options = {s['name']: s['id'] for s in scenarios}
        selected_scenario_name = st.selectbox("Select Scenario", options=list(scenario_options.keys()))
        selected_scenario_id = scenario_options[selected_scenario_name]
        
        # Get years for the selected scenario
        years = get_years()
        selected_year = st.selectbox("Select Year", options=years, index=0)
        
        # Get region types and filter regions
        region_types = get_region_types()
        selected_region_type = st.selectbox("Region Type", options=region_types)
        
        # Get regions based on type
        regions = get_regions(selected_region_type)
        region_options = {r['name']: r['id'] for r in regions}
        
        # Region selection for comparison
        selected_region_names = st.multiselect(
            "Select Regions to Compare (max 5)",
            options=list(region_options.keys()),
            default=list(region_options.keys())[:2] if len(region_options) >= 2 else list(region_options.keys())
        )
        
        # Limit to 5 regions
        if len(selected_region_names) > 5:
            selected_region_names = selected_region_names[:5]
            st.warning("Maximum 5 regions can be compared at once")
        
        selected_region_ids = [region_options[name] for name in selected_region_names]
        
        # Value type selection
        value_type = st.radio(
            "Value Type",
            options=["Percentage", "Absolute"],
            horizontal=True
        )
        value_column = "percent_of_region" if value_type == "Percentage" else "acres"
        
        # Land use types for trending analysis
        land_use_types = get_land_use_types()
        selected_land_use = st.selectbox("Land Use Type for Time Trends", options=land_use_types)
        
        # Year range for time series
        year_range = st.select_slider(
            "Year Range for Time Series",
            options=years,
            value=(years[0], years[-1]) if len(years) > 1 else (years[0], years[0])
        )
        selected_years = [y for y in years if year_range[0] <= y <= year_range[1]]
    
    # Main content
    if not selected_region_ids:
        st.warning("Please select at least one region to analyze")
    else:
        # Get data for current year comparison
        comparison_data = get_land_use_distribution(
            selected_scenario_id, 
            selected_year, 
            selected_region_ids
        )
        
        if comparison_data.empty:
            st.error("No data available for the selected parameters")
        else:
            # Display the comparison section
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.subheader(f"Land Use Comparison ({selected_year})")
            
            # Create tabs for different visualizations
            tab1, tab2 = st.tabs(["By Land Use Type", "By Region"])
            
            with tab1:
                distribution_fig = create_distribution_comparison(
                    comparison_data, 
                    value_column=value_column,
                    title=f"Land Use Distribution Comparison ({selected_year})"
                )
                st.plotly_chart(distribution_fig, use_container_width=True)
            
            with tab2:
                stacked_fig = create_stacked_comparison(
                    comparison_data, 
                    value_column=value_column,
                    title=f"Land Use Composition by Region ({selected_year})"
                )
                st.plotly_chart(stacked_fig, use_container_width=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Individual region analysis
            if len(selected_region_names) <= 3:  # Show detailed analysis if 3 or fewer regions selected
                st.markdown('<div class="card">', unsafe_allow_html=True)
                st.subheader("Region Details")
                
                cols = st.columns(len(selected_region_names))
                
                for i, region_name in enumerate(selected_region_names):
                    region_id = region_options[region_name]
                    
                    # Get time series data for this region
                    region_time_data = get_land_use_trends(
                        selected_scenario_id, 
                        selected_years, 
                        region_id
                    )
                    
                    with cols[i]:
                        st.markdown(f"#### {region_name}")
                        
                        # Display metrics
                        metrics = calculate_metrics(comparison_data, region_name, selected_year)
                        for metric in metrics:
                            st.metric(metric['label'], metric['value'])
                        
                        # Pie chart for current distribution
                        region_current_data = comparison_data[comparison_data['region_name'] == region_name]
                        pie_fig = create_land_use_pie_chart(
                            region_current_data, 
                            region_name, 
                            selected_year,
                            value_column=value_column
                        )
                        st.plotly_chart(pie_fig, use_container_width=True)
                
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Time series analysis section
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.subheader("Time Series Analysis")
            
            if len(selected_years) <= 1:
                st.info("Please select a wider year range for time series analysis")
            else:
                # Time series tab for selected land use type
                selected_region_id = region_options[selected_region_names[0]]
                time_series_data = get_land_use_trends(
                    selected_scenario_id, 
                    selected_years, 
                    selected_region_id,
                    selected_land_use
                )
                
                if not time_series_data.empty:
                    trend_fig = create_regional_trend_chart(
                        time_series_data, 
                        selected_region_names[0],
                        selected_land_use,
                        value_column=value_column
                    )
                    st.plotly_chart(trend_fig, use_container_width=True)
                    
                    # Compare trends across regions
                    if len(selected_region_ids) > 1:
                        st.subheader(f"{selected_land_use} Trends Across Regions")
                        
                        # Get data for all selected regions
                        multi_region_data = pd.DataFrame()
                        for region_id, region_name in zip(selected_region_ids, selected_region_names):
                            region_data = get_land_use_trends(
                                selected_scenario_id, 
                                selected_years, 
                                region_id,
                                selected_land_use
                            )
                            if not region_data.empty:
                                multi_region_data = pd.concat([multi_region_data, region_data])
                        
                        if not multi_region_data.empty:
                            trends_fig = px.line(
                                multi_region_data,
                                x='year',
                                y=value_column,
                                color='region_name',
                                markers=True,
                                title=f"{selected_land_use} Trends ({year_range[0]}-{year_range[1]})"
                            )
                            st.plotly_chart(trends_fig, use_container_width=True)
                else:
                    st.info(f"No {selected_land_use} data available for the selected parameters")
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Download section
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.subheader("Download Data")
            
            # Prepare data for download
            csv = comparison_data.to_csv(index=False)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"regional_comparison_{timestamp}.csv"
            
            st.download_button(
                label="Download Comparison Data (CSV)",
                data=csv,
                file_name=filename,
                mime="text/csv",
            )
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Footer
    st.markdown("""
    <div style="margin-top: 50px; text-align: center; color: #6b7280; font-size: 0.8rem;">
        <p>Data source: RPA Land Use Projections Database</p>
        <p>Analysis and visualization by RPA Land Use Viewer</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main() 