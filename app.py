import os
import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
from streamlit_folium import folium_static
from dotenv import load_dotenv
import json
import openai
import plotly.express as px
import pandasai
from pandasai.llm import OpenAI
import branca.colormap as cm
import re

# Import functions from the db module
from src.db.database import DatabaseConnection
from src.db.queries import LandUseQueries

# Load environment variables
load_dotenv()

# Set page configuration
st.set_page_config(
    page_title="New York Land Use Change Viewer",
    page_icon="🌆",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main {
        background-color: #f8f9fa;
    }
    .st-bx {
        background-color: #ffffff;
        border-radius: 5px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
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
    .metric-container {
        background-color: #ffffff;
        border-radius: 5px;
        padding: 15px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        margin-bottom: 10px;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: bold;
        color: #1e3a8a;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #4b5563;
    }
</style>
""", unsafe_allow_html=True)

# Function to get counties from database
def get_counties():
    """Get a list of all counties from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT county_name FROM counties ORDER BY county_name")
    counties = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return counties

# Function to get land use changes based on filters
def get_land_use_changes(county=None, start_year=None, end_year=None, land_use_type=None):
    """Get land use changes data with optional filters."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT lt.transition_id, lt.scenario_id, lt.time_step_id, lt.fips_code, 
           c.county_name as county, ts.start_year as year, 
           lt.from_land_use, lt.to_land_use, lt.acres
    FROM land_use_transitions lt
    JOIN counties c ON lt.fips_code = c.fips_code
    JOIN time_steps ts ON lt.time_step_id = ts.time_step_id
    WHERE 1=1
    """
    params = []
    
    if county:
        query += " AND c.county_name = ?"
        params.append(county)
    
    if start_year:
        query += " AND ts.start_year >= ?"
        params.append(start_year)
    
    if end_year:
        query += " AND ts.start_year <= ?"
        params.append(end_year)
    
    if land_use_type:
        query += " AND (lt.from_land_use = ? OR lt.to_land_use = ?)"
        params.append(land_use_type)
        params.append(land_use_type)
    
    cursor.execute(query, params)
    data = cursor.fetchall()
    
    # Get column names
    columns = [desc[0] for desc in cursor.description]
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    
    DatabaseConnection.close_connection(conn)
    return df

# Helper function to get county FIPS code
def get_county_fips(county_name):
    """Get the FIPS code for a given county name."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT fips_code FROM counties WHERE county_name = ?", (county_name,))
    result = cursor.fetchone()
    DatabaseConnection.close_connection(conn)
    return result[0] if result else None

# Function to get top counties by change
def get_top_counties_by_change(land_use_type, direction="increase", limit=5):
    """Get top counties with the most change in a specific land use type"""
    try:
        # Get available scenarios
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT scenario_name FROM scenarios LIMIT 1")
        scenario = cursor.fetchone()[0]
        DatabaseConnection.close_connection(conn)
        
        # Use the queries class to get data
        data = LandUseQueries.top_counties_by_change(
            scenario_name=scenario,
            land_use_type=land_use_type,
            direction=direction,
            limit=limit
        )
        
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error fetching top counties: {str(e)}")
        return pd.DataFrame()  # Return empty dataframe on error

# Function to get major transitions
def get_major_transitions(start_year=None, end_year=None, limit=10):
    """Get major land use transitions"""
    try:
        # Get available scenarios
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT scenario_name FROM scenarios LIMIT 1")
        scenario = cursor.fetchone()[0]
        DatabaseConnection.close_connection(conn)
        
        # Use the queries class to get data
        data = LandUseQueries.major_transitions(
            scenario_name=scenario,
            start_year=start_year,
            end_year=end_year,
            limit=limit
        )
        
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error fetching major transitions: {str(e)}")
        return pd.DataFrame()  # Return empty dataframe on error

# Function to load county geometry data
@st.cache_data
def load_county_geo():
    """Load county geometry data from GeoJSON file."""
    with open('data/counties.geojson', 'r') as f:
        return json.load(f)

# Function to get land use types
def get_land_use_types():
    """Get a list of all land use types from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT from_land_use as land_use_type 
        FROM land_use_transitions
        UNION
        SELECT DISTINCT to_land_use as land_use_type
        FROM land_use_transitions
        ORDER BY land_use_type
    """)
    land_use_types = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return land_use_types

# Function to get years
def get_years():
    """Get a list of all available years from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT start_year FROM time_steps ORDER BY start_year")
    years = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return years

# Function to analyze data with OpenAI
def analyze_with_openai(df, query):
    """Analyze data using PandasAI with OpenAI."""
    # Check if OpenAI API key is available
    api_key = os.getenv("OPENAI_API_KEY")
    
    # If API key is not available or empty
    if not api_key:
        return "Error: OpenAI API key is not set. Please add it to your .env file."
    
    # Check if the API key has a valid format (starts with "sk-")
    if not (api_key.startswith("sk-") or api_key.startswith("sk-proj-")):
        # Handle Anthropic key format detection
        if api_key.startswith("sk-ant-"):
            return "Error: You appear to be using an Anthropic API key for OpenAI. Please provide a valid OpenAI API key."
        return "Error: Invalid OpenAI API key format. The key should start with 'sk-' or 'sk-proj-'."
    
    try:
        # Force reload of environment variables to ensure we have the latest API key
        load_dotenv(override=True)
        api_key = os.getenv("OPENAI_API_KEY")
        
        # Initialize OpenAI and PandasAI
        llm = OpenAI(api_token=api_key, model="gpt-3.5-turbo")
        df_ai = pandasai.DataFrame(df)
        
        # Prepare data context for better analysis
        context = f"This is land use data with columns: {', '.join(df.columns)}. "
        context += "The data shows land use changes across different counties and years. "
        
        # Combine context with user query
        full_query = context + query
        
        # Generate response
        response = pandasai.chat(full_query, df_ai)
        return response
    except Exception as e:
        return f"Error during analysis: {str(e)}"

def create_choropleth_map(df, selected_county=None, selected_land_use=None, map_title="Land Use Change by County"):
    """Create a choropleth map of land use changes by county."""
    # Load county geometries
    counties_geo = load_county_geo()
    
    # Summarize data by county (total area)
    if selected_land_use:
        # For specific land use type, look at transitions to or from that type
        filtered_df = df[(df['from_land_use'] == selected_land_use) | (df['to_land_use'] == selected_land_use)]
        if filtered_df.empty:
            st.warning(f"No data available for {selected_land_use} land use type with current filters.")
            filtered_df = df  # Fallback to all data
    else:
        filtered_df = df
    
    county_data = filtered_df.groupby('county')['acres'].sum().reset_index()
    
    # Create a folium map centered on New York
    m = folium.Map(location=[42.9538, -75.5268], zoom_start=7, tiles="CartoDB positron")
    
    # Add a title to the map
    title_html = f'''
        <h3 align="center" style="font-size:16px"><b>{map_title}</b></h3>
    '''
    m.get_root().html.add_child(folium.Element(title_html))
    
    # Create a colormap with better color scale
    min_value = county_data['acres'].min() if not county_data.empty else 0
    max_value = county_data['acres'].max() if not county_data.empty else 100
    
    # Use viridis colormap for better accessibility
    colormap = cm.LinearColormap(
        colors=['#440154', '#3b528b', '#21908c', '#5dc963', '#fde725'],
        vmin=min_value, vmax=max_value,
        caption=f'Land Use Change Area (acres) {selected_land_use if selected_land_use else "All Types"}'
    )
    
    # Add the choropleth layer with tooltips
    choropleth = folium.Choropleth(
        geo_data=counties_geo,
        name='choropleth',
        data=county_data,
        columns=['county', 'acres'],
        key_on='feature.properties.NAME',
        fill_color='YlGnBu',
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name=f'Land Use Change Area (acres) {selected_land_use if selected_land_use else "All Types"}',
        highlight=True
    ).add_to(m)
    
    # Add tooltips to the choropleth layer
    choropleth.geojson.add_child(
        folium.features.GeoJsonTooltip(
            fields=['NAME'],
            aliases=['County:'],
            style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;")
        )
    )
    
    # Add county-specific data with popups
    style_function = lambda x: {'fillColor': '#ffffff', 
                                'color': '#000000', 
                                'fillOpacity': 0.1, 
                                'weight': 0.5}
    highlight_function = lambda x: {'fillColor': '#000000', 
                                    'color': '#000000', 
                                    'fillOpacity': 0.50, 
                                    'weight': 0.7}
    
    # Add the county data with tooltips and popups
    for idx, row in county_data.iterrows():
        county_name = row['county']
        acres = row['acres']
        
        # Find the county in the GeoJSON
        for feature in counties_geo['features']:
            if feature['properties']['NAME'] == county_name:
                popup_content = f"""
                <div style="font-family: Arial; width: 150px;">
                    <h4>{county_name} County</h4>
                    <p><b>Total Change:</b> {acres:.2f} acres</p>
                </div>
                """
                
                # Highlight the selected county if applicable
                if selected_county and county_name == selected_county:
                    # Create a GeoJson layer for the selected county with custom style
                    folium.GeoJson(
                        data=feature,
                        style_function=lambda x: {
                            'fillColor': '#ff7800',
                            'color': '#000000',
                            'fillOpacity': 0.7,
                            'weight': 2
                        },
                        tooltip=folium.Tooltip(county_name),
                        popup=folium.Popup(popup_content, max_width=300)
                    ).add_to(m)
                break
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    return m

def main():
    """Main function to run the Streamlit app."""
    # Page header with title and description
    st.markdown('<h1 class="title-text">NY State Land Use Change Projections</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle-text">Explore projected changes in land use across New York counties from 2020-2070</p>', unsafe_allow_html=True)
    
    # Sidebar for filters
    st.sidebar.header("Data Filters")
    
    # Get filter options from database
    counties = ["All Counties"] + get_counties()
    land_use_types = ["All Types"] + get_land_use_types()
    years = get_years()
    
    # Add filters in sidebar
    selected_county = st.sidebar.selectbox("Select County", counties)
    selected_land_use = st.sidebar.selectbox("Land Use Type", land_use_types)
    
    # Year range slider
    min_year = min(years) if years else 2020
    max_year = max(years) if years else 2070
    year_range = st.sidebar.slider(
        "Year Range", 
        min_value=min_year, 
        max_value=max_year,
        value=(min_year, max_year)
    )
    
    # Convert "All" selections to None for the query
    county_filter = None if selected_county == "All Counties" else selected_county
    land_use_filter = None if selected_land_use == "All Types" else selected_land_use
    
    # Get filtered data
    filtered_data = get_land_use_changes(
        county=county_filter,
        start_year=year_range[0],
        end_year=year_range[1],
        land_use_type=land_use_filter
    )
    
    # Display key metrics in a row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_acres = filtered_data['acres'].sum()
        st.markdown(f"""
        <div class="metric-container">
            <div class="metric-value">{total_acres:,.0f}</div>
            <div class="metric-label">Total Acres Changed</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        county_count = filtered_data['county'].nunique()
        st.markdown(f"""
        <div class="metric-container">
            <div class="metric-value">{county_count}</div>
            <div class="metric-label">Counties Affected</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        if not filtered_data.empty:
            max_transition = filtered_data.groupby(['from_land_use', 'to_land_use'])['acres'].sum().idxmax()
            max_transition_str = f"{max_transition[0]} → {max_transition[1]}"
        else:
            max_transition_str = "N/A"
        st.markdown(f"""
        <div class="metric-container">
            <div class="metric-value">{max_transition_str}</div>
            <div class="metric-label">Largest Transition Type</div>
        </div>
        """, unsafe_allow_html=True)
        
    with col4:
        if not filtered_data.empty:
            years_span = year_range[1] - year_range[0]
        else:
            years_span = 0
        st.markdown(f"""
        <div class="metric-container">
            <div class="metric-value">{years_span}</div>
            <div class="metric-label">Years Covered</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Create tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["Map View", "Data Tables", "Chart Analysis", "AI Analysis"])
    
    with tab1:
        st.subheader("Land Use Change Map")
        map_title = f"Land Use Change for {selected_county if county_filter else 'All NY Counties'}"
        if land_use_filter:
            map_title += f" - {land_use_filter} Land Type"
        
        # Create and display the map
        m = create_choropleth_map(
            filtered_data, 
            selected_county=county_filter, 
            selected_land_use=land_use_filter,
            map_title=map_title
        )
        folium_static(m, width=1100, height=600)
        
        # Add some contextual information below the map
        st.markdown("""
        ### Map Information
        
        This choropleth map displays the projected land use changes across New York counties. 
        Darker shades indicate counties with greater land use change in acres.
        
        * **Click on a county** to see detailed information.
        * **Adjust filters** in the sidebar to focus on specific counties, land use types, or time periods.
        * The projections are based on empirical econometric models of observed land-use transitions.
        """)
    
    with tab2:
        st.subheader("Land Use Change Data")
        
        # Create tabs for different data views
        data_tab1, data_tab2, data_tab3 = st.tabs(["Filtered Data", "Top Counties", "Major Transitions"])
        
        with data_tab1:
            if not filtered_data.empty:
                # Add a download button for the filtered data
                csv = filtered_data.to_csv(index=False)
                st.download_button(
                    label="Download Filtered Data as CSV",
                    data=csv,
                    file_name="land_use_changes.csv",
                    mime="text/csv"
                )
                
                # Display filtered data with formatting
                st.dataframe(filtered_data, use_container_width=True)
            else:
                st.info("No data matches current filter criteria.")
        
        with data_tab2:
            # Get top counties for selected land use type
            if land_use_filter:
                st.subheader(f"Top Counties by {land_use_filter} Change")
                top_increase = get_top_counties_by_change(land_use_filter, direction="increase", limit=5)
                top_decrease = get_top_counties_by_change(land_use_filter, direction="decrease", limit=5)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.write("Top Increases:")
                    st.dataframe(top_increase, use_container_width=True)
                with col2:
                    st.write("Top Decreases:")
                    st.dataframe(top_decrease, use_container_width=True)
            else:
                st.info("Select a specific land use type to see top counties by change.")
        
        with data_tab3:
            # Get major transitions in the selected time period
            major_transitions = get_major_transitions(
                start_year=year_range[0],
                end_year=year_range[1],
                limit=10
            )
            
            st.subheader("Major Land Use Transitions")
            if not major_transitions.empty:
                st.dataframe(major_transitions, use_container_width=True)
            else:
                st.info("No major transitions data available for the selected filters.")
    
    with tab3:
        st.subheader("Visual Analysis")
        
        if not filtered_data.empty:
            # Create summary data for charts
            county_summary = filtered_data.groupby('county')['acres'].sum().reset_index()
            county_summary = county_summary.sort_values('acres', ascending=False)
            
            # Type transitions summary
            type_transitions = filtered_data.groupby(['from_land_use', 'to_land_use'])['acres'].sum().reset_index()
            type_transitions = type_transitions.sort_values('acres', ascending=False).head(10)
            type_transitions['transition'] = type_transitions['from_land_use'] + ' → ' + type_transitions['to_land_use']
            
            # Create charts
            chart_col1, chart_col2 = st.columns(2)
            
            with chart_col1:
                # Bar chart of top counties
                fig1 = px.bar(
                    county_summary.head(10), 
                    x='county', 
                    y='acres',
                    title='Top 10 Counties by Land Use Change (acres)',
                    color='acres',
                    color_continuous_scale=px.colors.sequential.Viridis
                )
                fig1.update_layout(xaxis_title='County', yaxis_title='Acres Changed')
                st.plotly_chart(fig1, use_container_width=True)
            
            with chart_col2:
                # Bar chart of top transitions
                fig2 = px.bar(
                    type_transitions, 
                    x='transition', 
                    y='acres',
                    title='Top 10 Land Use Transitions (acres)',
                    color='acres',
                    color_continuous_scale=px.colors.sequential.Viridis
                )
                fig2.update_layout(xaxis_title='Transition Type', yaxis_title='Acres Changed')
                st.plotly_chart(fig2, use_container_width=True)
            
            # Time series analysis if multiple years are selected
            year_summary = filtered_data.groupby('year')['acres'].sum().reset_index()
            if len(year_summary) > 1:
                fig3 = px.line(
                    year_summary, 
                    x='year', 
                    y='acres',
                    title='Land Use Change Over Time',
                    markers=True
                )
                fig3.update_layout(xaxis_title='Year', yaxis_title='Total Acres Changed')
                st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No data available for visual analysis with current filters.")
    
    with tab4:
        st.subheader("AI-Powered Analysis")
        
        if not filtered_data.empty:
            # Input field for user queries
            user_query = st.text_input("Ask a question about the land use data:", 
                                      "What are the main trends in land use change?")
            
            if st.button("Analyze"):
                with st.spinner("Analyzing data with AI..."):
                    result = analyze_with_openai(filtered_data, user_query)
                    st.write(result)
            
            st.markdown("""
            ### Example questions:
            - What counties show the largest conversion to urban land?
            - Which land use type is experiencing the greatest decline?
            - What percentage of forest land is being converted to urban land?
            - What are the top 5 counties with increasing cropland?
            """)
        else:
            st.info("No data available for AI analysis with current filters.")

if __name__ == "__main__":
    main() 