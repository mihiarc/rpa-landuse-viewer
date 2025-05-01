"""
RPA Land Use Viewer - Transition Analysis

This page allows for the analysis of land use transitions over time.
"""

import streamlit as st
import pandas as pd
import sys
import os

# Import shared utility modules
from src.utils.shared import (
    get_scenarios,
    get_years,
    get_land_use_types,
    load_css,
    set_page_config,
    display_metrics
)
from src.utils.visualizations import (
    create_sankey_diagram,
    create_bar_chart,
    create_pie_chart
)
from src.db.database import DatabaseConnection

# Set page configuration
set_page_config(
    title="Land Use Transitions",
    icon="🔄",
    layout="wide"
)

# Load custom CSS
load_css()

@st.cache_data(ttl=1800)  # Cache for 30 minutes
def get_transition_data(scenario_id=None, start_year=None, end_year=None):
    """Get land use transition data for analysis.
    
    Args:
        scenario_id (int, optional): ID of the scenario to filter by. Defaults to None.
        start_year (int, optional): Starting year to filter by. Defaults to None.
        end_year (int, optional): Ending year to filter by. Defaults to None.
        
    Returns:
        pd.DataFrame: DataFrame containing transition data
    """
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT lt.from_land_use, lt.to_land_use, SUM(lt.acres) as total_acres, 
           ts.start_year, s.scenario_name, s.gcm, s.rcp, s.ssp
    FROM land_use_transitions lt
    JOIN time_steps ts ON lt.time_step_id = ts.time_step_id
    JOIN scenarios s ON lt.scenario_id = s.scenario_id
    WHERE 1=1
    """
    params = []
    
    if scenario_id:
        query += " AND lt.scenario_id = ?"
        params.append(scenario_id)
    
    if start_year:
        query += " AND ts.start_year >= ?"
        params.append(start_year)
    
    if end_year:
        query += " AND ts.start_year <= ?"
        params.append(end_year)
    
    query += " GROUP BY lt.from_land_use, lt.to_land_use, ts.start_year, s.scenario_name, s.gcm, s.rcp, s.ssp"
    query += " ORDER BY ts.start_year, total_acres DESC"
    
    cursor.execute(query, params)
    data = cursor.fetchall()
    
    # Get column names
    columns = [desc[0] for desc in cursor.description]
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    
    DatabaseConnection.close_connection(conn)
    return df

@st.cache_data(ttl=1800)
def create_net_change_chart(df):
    """Create a bar chart showing net changes in land use.
    
    Args:
        df (pd.DataFrame): DataFrame containing transition data
        
    Returns:
        go.Figure: Plotly bar chart figure showing net changes
    """
    if df.empty:
        return None
        
    # Calculate the net change for each land use type
    from_acres = df.groupby('from_land_use')['total_acres'].sum().reset_index()
    from_acres.columns = ['land_use_type', 'acres_from']
    
    to_acres = df.groupby('to_land_use')['total_acres'].sum().reset_index()
    to_acres.columns = ['land_use_type', 'acres_to']
    
    # Merge the two dataframes
    net_change = pd.merge(from_acres, to_acres, on='land_use_type', how='outer').fillna(0)
    
    # Calculate the net change (positive value means land use increased, negative means decreased)
    net_change['net_change'] = net_change['acres_to'] - net_change['acres_from']
    
    # Sort by net_change
    net_change = net_change.sort_values('net_change')
    
    # Create a bar chart showing net changes
    fig = create_bar_chart(
        net_change,
        x_col='land_use_type',
        y_col='net_change',
        title="Net Change in Land Use Area (Acres)",
        orientation='h'
    )
    
    return fig

def main():
    """Main function to run the Transition Analysis page."""
    # Page header
    st.markdown('<h1 class="title-text">Land Use Transition Analysis</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle-text">Explore how land use changes over time across different scenarios.</p>', unsafe_allow_html=True)
    
    # Sidebar filters
    st.sidebar.header("Data Filters")
    
    # Scenario selection
    scenarios = get_scenarios()
    scenario_names = [f"{s['name']} ({s['gcm']}_{s['rcp']}_{s['ssp']})" for s in scenarios]
    selected_scenario_idx = st.sidebar.selectbox(
        "Select Scenario",
        range(len(scenarios)),
        format_func=lambda i: scenario_names[i]
    )
    selected_scenario = scenarios[selected_scenario_idx]
    
    # Time period selection
    available_years = get_years()
    year_range = st.sidebar.select_slider(
        "Select Time Period",
        options=available_years,
        value=(min(available_years), max(available_years))
    )
    start_year, end_year = year_range
    
    # Visualization options
    st.sidebar.header("Visualization Options")
    show_sankey = st.sidebar.checkbox("Show Sankey Diagram", value=True)
    show_net_change = st.sidebar.checkbox("Show Net Change Chart", value=True)
    
    # Load data
    with st.spinner("Loading transition data..."):
        transition_data = get_transition_data(
            scenario_id=selected_scenario['id'],
            start_year=start_year,
            end_year=end_year
        )
    
        if transition_data.empty:
            st.error("No transition data available for the selected scenario and time period.")
            st.stop()
    
    # Show basic metrics
    st.header("Transition Summary")
    col1, col2, col3 = st.columns(3)
    
    # Total acres transitioned
    total_acres = transition_data['total_acres'].sum()
    col1.metric("Total Acres Transitioned", f"{total_acres:,.0f}")
    
    # Number of different types of transitions
    num_transitions = len(transition_data)
    col2.metric("Number of Transitions", f"{num_transitions:,}")
    
    # Number of years in analysis
    year_span = end_year - start_year
    col3.metric("Analysis Period", f"{year_span} years")
    
    # Visualizations
    st.header("Visualizations")
    
    if show_sankey:
        st.subheader("Land Use Transition Flow")
        
        with st.container():
            st.markdown('<div class="card">', unsafe_allow_html=True)
            
            sankey_fig = create_sankey_diagram(
                transition_data,
                source_col='from_land_use',
                target_col='to_land_use',
                value_col='total_acres',
                title=f"Land Use Transitions ({start_year} to {end_year})"
            )
            
            if sankey_fig:
                st.plotly_chart(sankey_fig, use_container_width=True)
            else:
                st.info("Not enough transition data to create a Sankey diagram.")
                
            st.markdown('</div>', unsafe_allow_html=True)
    
    if show_net_change:
        st.subheader("Net Land Use Change")
        
        with st.container():
            st.markdown('<div class="card">', unsafe_allow_html=True)
            
            net_change_fig = create_net_change_chart(transition_data)
            
            if net_change_fig:
                st.plotly_chart(net_change_fig, use_container_width=True)
            else:
                st.info("Not enough data to calculate net land use changes.")
                
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Additional analysis section
    st.header("Detailed Analysis")
    
    # Let user select specific land use types to analyze
    st.subheader("Specific Land Use Analysis")
    
    land_use_types = get_land_use_types()
    selected_land_use = st.selectbox(
        "Select land use type to analyze",
        options=land_use_types
    )
    
    # Filter for the selected land use (as source)
    source_df = transition_data[transition_data['from_land_use'] == selected_land_use]
    
    if not source_df.empty:
        st.markdown(f"#### Land transitioned from {selected_land_use}")
        
        # Create a pie chart of where this land use went
        to_pie_data = source_df.groupby('to_land_use')['total_acres'].sum().reset_index()
        to_pie_data.columns = ['land_use_type', 'acres']
        
        to_pie_fig = create_pie_chart(
            to_pie_data,
            values_col='acres',
            names_col='land_use_type',
            title=f"Where {selected_land_use} Transitioned To"
        )
        
        st.plotly_chart(to_pie_fig, use_container_width=True)
    else:
        st.info(f"No transitions found with {selected_land_use} as the source.")
    
    # Filter for the selected land use (as target)
    target_df = transition_data[transition_data['to_land_use'] == selected_land_use]
    
    if not target_df.empty:
        st.markdown(f"#### Land transitioned to {selected_land_use}")
        
        # Create a pie chart of where this land use came from
        from_pie_data = target_df.groupby('from_land_use')['total_acres'].sum().reset_index()
        from_pie_data.columns = ['land_use_type', 'acres']
        
        from_pie_fig = create_pie_chart(
            from_pie_data,
            values_col='acres',
            names_col='land_use_type',
            title=f"Where {selected_land_use} Transitioned From"
        )
        
        st.plotly_chart(from_pie_fig, use_container_width=True)
    else:
        st.info(f"No transitions found with {selected_land_use} as the target.")
    
    # Include raw data in an expandable section
    with st.expander("View Raw Transition Data"):
        st.dataframe(transition_data, use_container_width=True)

# Run the main function
if __name__ == "__main__":
    main() 