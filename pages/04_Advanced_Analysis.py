"""
Advanced Analysis - RPA Land Use Viewer
This page provides advanced analytics capabilities for comparing scenarios and exploring land use transitions.
"""

import streamlit as st
import pandas as pd
from datetime import datetime

# Import shared modules
from src.utils.shared import (
    get_scenarios, 
    get_years, 
    get_states, 
    get_counties_by_state,
    set_page_config, 
    load_css
)
from src.db.advanced_queries import AdvancedQueries
from src.utils.advanced_visualizations import (
    create_sankey_diagram,
    create_scenario_comparison_chart,
    create_time_series_comparison
)

# Set page configuration
set_page_config(
    title="Advanced Analysis - RPA Land Use Viewer",
    icon="🔍",
    layout="wide"
)

# Load custom CSS
load_css()

# Page title
st.title("Advanced Land Use Analysis")
st.markdown("""
This page provides advanced analysis tools to explore land use transitions, compare scenarios, 
and identify critical change periods in the land use dataset.
""")

# Create tabs for different analysis types
tab1, tab2, tab3 = st.tabs([
    "Land Use Transitions",
    "Scenario Comparison",
    "Peak Change Analysis"
])

# === Tab 1: Land Use Transitions (Sankey) ===
with tab1:
    st.header("Land Use Transition Analysis")
    st.markdown("""
    Visualize the transitions between different land use types over time.
    This Sankey diagram shows how land flows from one use to another during the selected time period.
    """)
    
    # Input controls
    col1, col2 = st.columns(2)
    
    with col1:
        # Scenario selection
        scenarios = get_scenarios()
        scenario_names = [f"{s['name']} ({s['gcm']}_{s['rcp']}_{s['ssp']})" for s in scenarios]
        selected_scenario_idx = st.selectbox(
            "Select Scenario",
            range(len(scenarios)),
            format_func=lambda i: scenario_names[i],
            key="transition_scenario"
        )
        selected_scenario = scenarios[selected_scenario_idx]
    
    with col2:
        # Year range selection
        years = get_years()
        min_year, max_year = min(years), max(years)
        
        year_range = st.select_slider(
            "Select Year Range",
            options=years,
            value=(min_year, max_year),
            key="transition_years"
        )
        
        start_year, end_year = year_range
    
    # Number of transitions to show
    num_transitions = st.slider(
        "Number of Transitions to Display",
        min_value=5,
        max_value=30,
        value=15,
        help="Higher numbers show more transitions but may make the diagram more complex"
    )
    
    # Generate Sankey diagram
    with st.spinner("Generating transition diagram..."):
        transitions = AdvancedQueries.major_transitions(
            start_year=start_year,
            end_year=end_year,
            scenario_id=selected_scenario['id'],
            limit=num_transitions
        )
        
        if transitions:
            title = f"Land Use Transitions ({start_year}-{end_year}): {selected_scenario['name']}"
            fig = create_sankey_diagram(transitions, title=title)
            st.plotly_chart(fig, use_container_width=True)
            
            # Show data table
            if st.checkbox("Show Transition Data Table"):
                df = pd.DataFrame(transitions)
                df = df.rename(columns={
                    'from_land_use': 'From Land Use',
                    'to_land_use': 'To Land Use',
                    'acres_changed': 'Acres Changed'
                })
                df['Acres Changed'] = df['Acres Changed'].map('{:,.0f}'.format)
                st.dataframe(df, use_container_width=True)
        else:
            st.warning("No transition data available for the selected time period and scenario.")

# === Tab 2: Scenario Comparison ===
with tab2:
    st.header("Scenario Comparison")
    st.markdown("""
    Compare different climate and socioeconomic scenarios to understand how they affect land use change.
    """)
    
    # Input controls
    col1, col2 = st.columns(2)
    
    with col1:
        # First scenario
        scenarios = get_scenarios()
        scenario_names = [f"{s['name']} ({s['gcm']}_{s['rcp']}_{s['ssp']})" for s in scenarios]
        selected_scenario1_idx = st.selectbox(
            "Select First Scenario",
            range(len(scenarios)),
            format_func=lambda i: scenario_names[i],
            key="compare_scenario1"
        )
        selected_scenario1 = scenarios[selected_scenario1_idx]
    
    with col2:
        # Second scenario
        selected_scenario2_idx = st.selectbox(
            "Select Second Scenario",
            range(len(scenarios)),
            format_func=lambda i: scenario_names[i],
            index=1 if len(scenarios) > 1 else 0,
            key="compare_scenario2"
        )
        selected_scenario2 = scenarios[selected_scenario2_idx]
    
    # Year range and land use type
    col3, col4 = st.columns(2)
    
    with col3:
        # Year range selection
        years = get_years()
        min_year, max_year = min(years), max(years)
        
        year_range = st.select_slider(
            "Select Year Range",
            options=years,
            value=(min_year, max_year),
            key="compare_years"
        )
        
        start_year, end_year = year_range
    
    with col4:
        # Land use type selection
        land_use_types = ["Urban", "Forest", "Crop", "Pasture", "Range", "Water"]
        selected_land_use = st.selectbox(
            "Select Land Use Type",
            land_use_types,
            key="compare_land_use"
        )
    
    # Generate comparison
    with st.spinner("Generating scenario comparison..."):
        comparison_data = AdvancedQueries.compare_scenarios(
            start_year=start_year,
            end_year=end_year,
            land_use_type=selected_land_use,
            scenario1_id=selected_scenario1['id'],
            scenario2_id=selected_scenario2['id']
        )
        
        if comparison_data and comparison_data.get('scenarios'):
            year_range_str = f"{start_year}-{end_year}"
            fig = create_scenario_comparison_chart(
                comparison_data=comparison_data,
                land_use_type=selected_land_use,
                year_range=year_range_str
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Show explanation
            if 'difference' in comparison_data:
                diff = comparison_data['difference']
                st.markdown(f"""
                ### Key Insights
                
                - The difference in net change between scenarios is **{diff['net_change_diff']:,.0f} acres**.
                - This represents a **{diff['percent_diff']:.1f}%** difference between scenarios.
                - The annual rate difference is **{diff['annual_rate_diff']:.1f} acres/year**.
                
                These differences reflect how varying climate and socioeconomic assumptions affect land use change.
                """)
        else:
            st.warning("Unable to compare the selected scenarios. Please try different selections.")

# === Tab 3: Peak Change Analysis ===
with tab3:
    st.header("Peak Change Period Analysis")
    st.markdown("""
    Identify the time periods with the most significant land use changes.
    This analysis helps pinpoint when major transitions are projected to occur.
    """)
    
    # Input controls
    col1, col2 = st.columns(2)
    
    with col1:
        # Scenario selection
        scenarios = get_scenarios()
        scenario_names = [f"{s['name']} ({s['gcm']}_{s['rcp']}_{s['ssp']})" for s in scenarios]
        selected_scenario_idx = st.selectbox(
            "Select Scenario",
            range(len(scenarios)),
            format_func=lambda i: scenario_names[i],
            key="peak_scenario"
        )
        selected_scenario = scenarios[selected_scenario_idx]
    
    with col2:
        # Land use type selection
        land_use_types = ["Urban", "Forest", "Crop", "Pasture", "Range", "Water"]
        selected_land_use = st.selectbox(
            "Select Land Use Type",
            land_use_types,
            key="peak_land_use"
        )
    
    # Generate peak period analysis
    with st.spinner("Analyzing peak change periods..."):
        peak_period = AdvancedQueries.peak_change_period(
            land_use_type=selected_land_use,
            scenario_id=selected_scenario['id']
        )
        
        if peak_period:
            st.markdown(f"""
            ### Peak Change Period: {peak_period['start_year']} - {peak_period['end_year']}
            
            - **Net change**: {peak_period['net_change']:,.0f} acres
            - **Annual rate**: {peak_period['annual_rate']:,.0f} acres/year
            """)
            
            # Add a visualization or other analysis here
            st.info(f"""
            This analysis shows that the most significant changes in {selected_land_use} land use are 
            projected to occur between {peak_period['start_year']} and {peak_period['end_year']} 
            under the {selected_scenario['name']} scenario.
            
            Understanding when peak changes occur can help with planning and policy development.
            """)
            
            # Add call to action
            st.success("""
            **Next Steps**:
            - Explore the Land Use Transitions tab to see what other land use types are changing during this peak period.
            - Compare this scenario with others to see if peak change periods differ across climate scenarios.
            """)
        else:
            st.warning("No peak change period data available for the selected land use type and scenario.")

# Footer with data source information
st.markdown("""
---
### About the Data
The data used in these analyses comes from the USDA Forest Service's Resources Planning Act (RPA) 
land use change projection dataset. It provides county-level land use transition projections from 
2020 to 2070 under different climate and socioeconomic scenarios.
""") 