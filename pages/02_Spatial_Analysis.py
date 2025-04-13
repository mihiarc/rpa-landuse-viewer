import streamlit as st
import pandas as pd
import folium
import geopandas as gpd
from streamlit_folium import folium_static
import plotly.express as px
import numpy as np
import branca.colormap as cm
from folium.plugins import MarkerCluster, HeatMap
import sys
import os

# Add parent directory to path to access shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.db.database import DatabaseConnection
from src.db.queries import LandUseQueries

# Page config
st.set_page_config(
    page_title="Spatial Analysis | RPA Viewer",
    page_icon="🗺️",
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
    .map-container {
        height: 600px;
        width: 100%;
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
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
def get_regions():
    """Get a list of all available regions from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT region_id, region_name, region_type 
        FROM regions 
        ORDER BY region_type, region_name
    """)
    regions = [{'id': row[0], 'name': row[1], 'type': row[2]} for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return regions

@st.cache_data
def get_region_geojson(region_type):
    """Get GeoJSON for regions of a specific type."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT region_id, region_name, region_type, ST_AsGeoJSON(geometry) as geojson
        FROM regions
        WHERE region_type = ?
    """, (region_type,))
    
    features = []
    for row in cursor.fetchall():
        region_id, region_name, region_type, geojson_str = row
        geometry = json.loads(geojson_str)
        features.append({
            "type": "Feature",
            "properties": {
                "region_id": region_id,
                "region_name": region_name,
                "region_type": region_type
            },
            "geometry": geometry
        })
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    DatabaseConnection.close_connection(conn)
    return geojson

@st.cache_data
def get_spatial_data(scenario_id, year, land_use_type=None, region_type=None, region_id=None):
    """Get spatial data for mapping."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT r.region_id, r.region_name, r.region_type, lp.land_use_type, 
           lp.acres, lp.percent_of_region, s.scenario_name, lp.year,
           ST_AsGeoJSON(r.geometry) as geojson
    FROM land_use_projections lp
    JOIN regions r ON lp.region_id = r.region_id
    JOIN scenarios s ON lp.scenario_id = s.scenario_id
    WHERE lp.scenario_id = ? AND lp.year = ?
    """
    params = [scenario_id, year]
    
    if land_use_type:
        query += " AND lp.land_use_type = ?"
        params.append(land_use_type)
    
    if region_type:
        query += " AND r.region_type = ?"
        params.append(region_type)
        
    if region_id:
        query += " AND r.region_id = ?"
        params.append(region_id)
    
    query += " ORDER BY r.region_name, lp.land_use_type"
    
    cursor.execute(query, params)
    data = cursor.fetchall()
    
    # Get column names
    columns = [desc[0] for desc in cursor.description]
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    
    # Convert geojson strings to objects
    if 'geojson' in df.columns:
        df['geometry'] = df['geojson'].apply(json.loads)
        df = df.drop('geojson', axis=1)
    
    DatabaseConnection.close_connection(conn)
    return df

def create_choropleth_map(df, value_column='percent_of_region', title="Land Use Distribution"):
    """Create a choropleth map visualization."""
    if df.empty:
        return None
    
    # Create a GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_json(df['geometry']))
    
    # Get center point for map
    center_lat = gdf.geometry.centroid.y.mean()
    center_lon = gdf.geometry.centroid.x.mean()
    
    # Create map
    m = folium.Map(location=[center_lat, center_lon], zoom_start=8,
                  tiles='CartoDB positron', control_scale=True)
    
    # Add choropleth layer
    min_value = df[value_column].min()
    max_value = df[value_column].max()
    
    # Custom color map based on the land use type if present
    if 'land_use_type' in df.columns and len(df['land_use_type'].unique()) == 1:
        land_use = df['land_use_type'].iloc[0]
        if 'forest' in land_use.lower():
            colormap = cm.LinearColormap(colors=['#edf8e9', '#c7e9c0', '#a1d99b', '#74c476', '#41ab5d', '#238b45', '#005a32'],
                                        vmin=min_value, vmax=max_value)
        elif 'crop' in land_use.lower() or 'agriculture' in land_use.lower():
            colormap = cm.LinearColormap(colors=['#ffffd4', '#fee391', '#fec44f', '#fe9929', '#ec7014', '#cc4c02', '#8c2d04'],
                                        vmin=min_value, vmax=max_value)
        elif 'urban' in land_use.lower() or 'developed' in land_use.lower():
            colormap = cm.LinearColormap(colors=['#f7f7f7', '#d9d9d9', '#bdbdbd', '#969696', '#737373', '#525252', '#252525'],
                                        vmin=min_value, vmax=max_value)
        elif 'water' in land_use.lower():
            colormap = cm.LinearColormap(colors=['#f7fbff', '#deebf7', '#c6dbef', '#9ecae1', '#6baed6', '#4292c6', '#2171b5', '#084594'],
                                        vmin=min_value, vmax=max_value)
        else:
            colormap = cm.LinearColormap(colors=['#efedf5', '#dadaeb', '#bcbddc', '#9e9ac8', '#807dba', '#6a51a3', '#54278f'],
                                        vmin=min_value, vmax=max_value)
    else:
        colormap = cm.LinearColormap(colors=['#ffffcc', '#a1dab4', '#41b6c4', '#2c7fb8', '#253494'],
                                    vmin=min_value, vmax=max_value)
    
    # Create GeoJSON data
    for _, row in df.iterrows():
        geo_json = {"type": "Feature", 
                  "geometry": row['geometry'],
                  "properties": {"region_name": row['region_name'],
                                "value": float(row[value_column])}}
        
        folium.GeoJson(
            geo_json,
            style_function=lambda x: {
                'fillColor': colormap(x['properties']['value']),
                'color': 'black',
                'weight': 1,
                'fillOpacity': 0.7
            },
            tooltip=folium.Tooltip(
                f"{row['region_name']}<br>"
                f"{row['land_use_type'] if 'land_use_type' in row else ''}<br>"
                f"{value_column}: {row[value_column]:.2f}"
            )
        ).add_to(m)
    
    # Add color map legend
    colormap.caption = f'{title} ({value_column})'
    colormap.add_to(m)
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    return m

def create_land_use_comparison_chart(df, column='acres', groupby='land_use_type'):
    """Create a bar chart comparing land use distribution."""
    if df.empty:
        return None
    
    # Group data
    grouped_df = df.groupby(groupby)[column].sum().reset_index()
    
    # Sort by value
    grouped_df = grouped_df.sort_values(column, ascending=False)
    
    # Create bar chart
    fig = px.bar(
        grouped_df,
        x=groupby,
        y=column,
        title=f"{groupby.replace('_', ' ').title()} Distribution by {column.replace('_', ' ').title()}",
        labels={groupby: groupby.replace('_', ' ').title(), column: column.replace('_', ' ').title()},
        color=column,
        color_continuous_scale='Viridis'
    )
    
    # Update layout
    fig.update_layout(
        height=500,
        margin=dict(l=50, r=50, t=50, b=100),
        xaxis=dict(tickangle=45)
    )
    
    return fig

def create_regional_comparison_chart(df, regions, land_use_type, value_column='percent_of_region'):
    """Create a chart comparing a land use type across different regions."""
    if df.empty:
        return None
    
    # Filter data for the selected land use type
    filtered_df = df[df['land_use_type'] == land_use_type].copy()
    
    if filtered_df.empty:
        return None
    
    # Sort by value
    filtered_df = filtered_df.sort_values(value_column, ascending=False)
    
    # Create bar chart
    fig = px.bar(
        filtered_df,
        x='region_name',
        y=value_column,
        title=f"{land_use_type} Distribution by Region ({value_column.replace('_', ' ').title()})",
        labels={'region_name': 'Region', value_column: value_column.replace('_', ' ').title()},
        color=value_column,
        color_continuous_scale='Viridis'
    )
    
    # Update layout
    fig.update_layout(
        height=500,
        margin=dict(l=50, r=50, t=50, b=100),
        xaxis=dict(tickangle=45)
    )
    
    return fig

def main():
    # Page header
    st.markdown('<h1 class="title-text">Spatial Land Use Analysis</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle-text">Explore the geographic distribution of projected land use changes</p>', unsafe_allow_html=True)

    # Sidebar filters
    st.sidebar.header("Map Filters")
    
    # Get data for filters
    scenarios = get_scenarios()
    years = get_years()
    land_use_types = get_land_use_types()
    regions = get_regions()
    
    # Group regions by type
    region_types = sorted(list(set(r['type'] for r in regions)))
    
    # Scenario selection
    scenario_options = [f"{s['name']} ({s['gcm']}, {s['rcp']}-{s['ssp']})" for s in scenarios]
    selected_scenario = st.sidebar.selectbox("Select Scenario", scenario_options)
    selected_scenario_id = scenarios[scenario_options.index(selected_scenario)]['id']
    
    # Year selection
    selected_year = st.sidebar.selectbox("Select Year", years)
    
    # Land use type selection
    selected_land_use_type = st.sidebar.selectbox("Select Land Use Type", land_use_types)
    
    # Region type selection
    selected_region_type = st.sidebar.selectbox("Select Region Type", region_types)
    
    # Display value selection
    display_value = st.sidebar.radio(
        "Display Value", 
        ["percent_of_region", "acres"], 
        format_func=lambda x: "Percentage of Region" if x == "percent_of_region" else "Acres"
    )
    
    # Get spatial data based on filters
    spatial_data = get_spatial_data(
        scenario_id=selected_scenario_id,
        year=selected_year,
        land_use_type=selected_land_use_type,
        region_type=selected_region_type
    )
    
    # Display map
    st.markdown('<div class="card">', unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.markdown(f"### Geographic Distribution of {selected_land_use_type}")
        st.markdown('<div class="map-container">', unsafe_allow_html=True)
        
        if not spatial_data.empty:
            map_title = f"{selected_land_use_type} Distribution ({selected_year})"
            folium_map = create_choropleth_map(spatial_data, value_column=display_value, title=map_title)
            if folium_map:
                folium_static(folium_map, width=800, height=500)
            else:
                st.info("Could not create map with the selected filters.")
        else:
            st.info("No spatial data available for the selected filters.")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown("### Map Legend")
        
        if not spatial_data.empty:
            # Display some helpful statistics about the map
            st.markdown("**Summary Statistics:**")
            
            # Calculate statistics
            total_regions = spatial_data['region_id'].nunique()
            avg_value = spatial_data[display_value].mean()
            max_value = spatial_data[display_value].max()
            max_region = spatial_data.loc[spatial_data[display_value].idxmax(), 'region_name']
            
            # Display metrics
            st.metric("Total Regions", f"{total_regions}")
            st.metric(f"Average {display_value.replace('_', ' ').title()}", f"{avg_value:.2f}")
            st.metric(f"Maximum {display_value.replace('_', ' ').title()}", f"{max_value:.2f}")
            st.metric("Region with Maximum Value", max_region)
            
            # Explanation
            st.markdown("""
            **How to interpret:**
            
            - Darker colors indicate higher concentration
            - Hover over regions to see exact values
            - Use the filter panel to explore different scenarios
            """)
        else:
            st.info("No data to display in legend.")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Display charts
    st.markdown('<div class="card">', unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["Regional Distribution", "Land Use Comparison"])
    
    with tab1:
        if not spatial_data.empty:
            fig_regional = create_regional_comparison_chart(
                spatial_data, 
                regions, 
                selected_land_use_type, 
                value_column=display_value
            )
            if fig_regional:
                st.plotly_chart(fig_regional, use_container_width=True)
            else:
                st.info("Could not create regional comparison chart with the selected filters.")
        else:
            st.info("No data available for the regional comparison chart.")
    
    with tab2:
        # Get data for all land use types in the selected region type
        all_land_use_data = get_spatial_data(
            scenario_id=selected_scenario_id,
            year=selected_year,
            region_type=selected_region_type
        )
        
        if not all_land_use_data.empty:
            fig_land_use = create_land_use_comparison_chart(
                all_land_use_data, 
                column=display_value, 
                groupby='land_use_type'
            )
            if fig_land_use:
                st.plotly_chart(fig_land_use, use_container_width=True)
            else:
                st.info("Could not create land use comparison chart with the selected filters.")
        else:
            st.info("No data available for the land use comparison chart.")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Data table
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### Data Table")
    
    if not spatial_data.empty:
        # Format data for display
        display_data = spatial_data.drop('geometry', axis=1).copy()
        
        # Format numerical columns
        if 'acres' in display_data.columns:
            display_data['acres'] = display_data['acres'].round(2)
        if 'percent_of_region' in display_data.columns:
            display_data['percent_of_region'] = display_data['percent_of_region'].round(2)
        
        # Display table
        st.dataframe(display_data, use_container_width=True)
        
        # Add download button
        csv = display_data.to_csv(index=False)
        st.download_button(
            label="Download Data as CSV",
            data=csv,
            file_name=f"land_use_spatial_{selected_land_use_type}_{selected_year}.csv",
            mime="text/csv"
        )
    else:
        st.info("No data available for the data table.")
    
    st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    import json  # Import needed for handling GeoJSON
    main() 