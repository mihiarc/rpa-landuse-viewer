import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import os
import sys

# Add parent directory to path to access shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.db.database import DatabaseConnection
from src.db.queries import LandUseQueries

# Set page configuration
st.set_page_config(
    page_title="Land Use Transition Analysis | RPA Viewer",
    page_icon="🔄",
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
        margin-bottom: 0.5rem;
    }
    .subtitle-text {
        color: #4b5563;
        font-size: 1.2rem;
        margin-bottom: 1.5rem;
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
        font-size: 2rem;
        font-weight: bold;
        color: #1e3a8a;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #4b5563;
    }
    .stat-card {
        background-color: white;
        border-radius: 5px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        padding: 10px;
        text-align: center;
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
def get_regions():
    """Get a list of all available regions from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT region_type 
        FROM regions
        ORDER BY region_type
    """)
    region_types = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return region_types

@st.cache_data
def get_regions_by_type(region_type):
    """Get regions of a specific type."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT region_id, region_name 
        FROM regions
        WHERE region_type = ?
        ORDER BY region_name
    """, (region_type,))
    regions = [{'id': row[0], 'name': row[1]} for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return regions

@st.cache_data
def get_land_use_data(scenario_id, years=None, region_type=None, region_id=None):
    """Get land use data for specified scenario, years and regions."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT lp.land_use_type, lp.acres, lp.year, r.region_name, r.region_type
    FROM land_use_projections lp
    JOIN regions r ON lp.region_id = r.region_id
    WHERE lp.scenario_id = ?
    """
    params = [scenario_id]
    
    if years:
        placeholders = ', '.join(['?' for _ in years])
        query += f" AND lp.year IN ({placeholders})"
        params.extend(years)
    
    if region_type:
        query += " AND r.region_type = ?"
        params.append(region_type)
    
    if region_id:
        query += " AND r.region_id = ?"
        params.append(region_id)
    
    query += " ORDER BY lp.year, lp.land_use_type"
    
    cursor.execute(query, params)
    data = cursor.fetchall()
    
    # Get column names
    columns = [desc[0] for desc in cursor.description]
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    
    DatabaseConnection.close_connection(conn)
    return df

@st.cache_data
def get_transition_data(scenario_id, start_year, end_year, region_type=None, region_id=None):
    """
    Get transition data between two years.
    This is a simplified version - in a real database you would have a transitions table.
    Here we'll simulate transitions based on the available data.
    """
    # Get data for both years
    years = [start_year, end_year]
    df = get_land_use_data(scenario_id, years, region_type, region_id)
    
    if df.empty:
        return pd.DataFrame()
    
    # Split data by year
    start_df = df[df['year'] == start_year]
    end_df = df[df['year'] == end_year]
    
    if start_df.empty or end_df.empty:
        return pd.DataFrame()
    
    # Get unique land use types
    land_use_types = df['land_use_type'].unique()
    
    # Create a transition matrix
    transitions = []
    
    # Group by region and land use type
    start_grouped = start_df.groupby(['region_name', 'land_use_type'])['acres'].sum().reset_index()
    end_grouped = end_df.groupby(['region_name', 'land_use_type'])['acres'].sum().reset_index()
    
    # Get unique regions
    regions = df['region_name'].unique()
    
    # For each region, simulate transitions
    for region in regions:
        start_region = start_grouped[start_grouped['region_name'] == region]
        end_region = end_grouped[end_grouped['region_name'] == region]
        
        # Calculate total acres in the region for each year
        start_total = start_region['acres'].sum()
        end_total = end_region['acres'].sum()
        
        # Skip if region has no data for either year
        if start_total == 0 or end_total == 0:
            continue
        
        # For each land use type in start year
        for _, start_row in start_region.iterrows():
            source_type = start_row['land_use_type']
            source_acres = start_row['acres']
            
            # For each land use type in end year
            for _, end_row in end_region.iterrows():
                target_type = end_row['land_use_type']
                target_acres = end_row['acres']
                
                # Estimate transition based on relative proportions
                # This is a simplification and doesn't represent actual transitions
                # In a real system, you would have actual transition data
                transition_acres = (source_acres / start_total) * (target_acres / end_total) * min(start_total, end_total)
                
                # Add to transitions list
                transitions.append({
                    'source': source_type,
                    'target': target_type,
                    'value': transition_acres,
                    'region': region
                })
    
    # Create DataFrame from transitions
    transition_df = pd.DataFrame(transitions)
    
    # Aggregate by source and target
    aggregated = transition_df.groupby(['source', 'target'])['value'].sum().reset_index()
    
    return aggregated

def create_sankey_diagram(transition_df, title="Land Use Transitions"):
    """Create a Sankey diagram from transition data."""
    if transition_df.empty:
        return None
    
    # Get unique sources and targets
    sources = transition_df['source'].unique()
    targets = transition_df['target'].unique()
    
    # Create a mapping of land use types to indices
    labels = list(set(sources).union(set(targets)))
    label_dict = {label: i for i, label in enumerate(labels)}
    
    # Create source and target indices
    source_indices = [label_dict[s] for s in transition_df['source']]
    target_indices = [label_dict[t] for t in transition_df['target']]
    
    # Create Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=labels
        ),
        link=dict(
            source=source_indices,
            target=target_indices,
            value=transition_df['value'],
            hovertemplate='%{source.label} → %{target.label}<br>Acres: %{value:,.0f}<extra></extra>'
        )
    )])
    
    # Update layout
    fig.update_layout(
        title=title,
        font=dict(size=12),
        height=600,
        margin=dict(l=25, r=25, t=70, b=25)
    )
    
    return fig

def create_net_change_chart(transition_df, start_year, end_year):
    """Create a chart showing net change in land use between two years."""
    if transition_df.empty:
        return None
    
    # Calculate net change for each land use type
    source_sum = transition_df.groupby('source')['value'].sum().reset_index()
    source_sum.columns = ['land_use_type', 'outflow']
    
    target_sum = transition_df.groupby('target')['value'].sum().reset_index()
    target_sum.columns = ['land_use_type', 'inflow']
    
    # Merge the two dataframes
    net_change = pd.merge(source_sum, target_sum, on='land_use_type', how='outer').fillna(0)
    
    # Calculate net change
    net_change['net_change'] = net_change['inflow'] - net_change['outflow']
    
    # Sort by net change
    net_change = net_change.sort_values('net_change')
    
    # Create a bar chart
    fig = px.bar(
        net_change,
        y='land_use_type',
        x='net_change',
        title=f'Net Change in Land Use ({start_year} to {end_year})',
        labels={'land_use_type': 'Land Use Type', 'net_change': 'Net Change (acres)'},
        color='net_change',
        color_continuous_scale=['red', 'white', 'green'],
        color_continuous_midpoint=0,
        height=500
    )
    
    # Update layout
    fig.update_layout(
        yaxis=dict(categoryorder='total ascending'),
        margin=dict(l=25, r=25, t=70, b=25),
        coloraxis_showscale=False
    )
    
    # Add annotations for values
    for i, row in net_change.iterrows():
        fig.add_annotation(
            x=row['net_change'],
            y=row['land_use_type'],
            text=f"{row['net_change']:,.0f}",
            showarrow=False,
            xanchor='left' if row['net_change'] < 0 else 'right',
            xshift=5 if row['net_change'] < 0 else -5,
            font=dict(color='black')
        )
    
    return fig

def create_transition_matrix(transition_df, land_use_types):
    """Create a heatmap showing transition matrix between land use types."""
    if transition_df.empty:
        return None
    
    # Get all land use types from transition data
    sources = transition_df['source'].unique()
    targets = transition_df['target'].unique()
    all_types = list(set(sources).union(set(targets)))
    
    # If specific land use types are provided, filter to those
    if land_use_types:
        all_types = [t for t in all_types if t in land_use_types]
    
    # Create an empty matrix
    matrix = pd.DataFrame(0, index=all_types, columns=all_types)
    
    # Fill the matrix with transition values
    for _, row in transition_df.iterrows():
        source = row['source']
        target = row['target']
        if source in all_types and target in all_types:
            matrix.loc[source, target] = row['value']
    
    # Create heatmap
    fig = px.imshow(
        matrix,
        labels=dict(x="End Land Use", y="Start Land Use", color="Acres"),
        x=all_types,
        y=all_types,
        color_continuous_scale='YlGnBu',
        aspect="auto",
        title="Transition Matrix (acres)"
    )
    
    # Update layout
    fig.update_layout(
        height=600,
        margin=dict(l=25, r=25, t=70, b=25),
        xaxis=dict(tickangle=45)
    )
    
    # Add text annotations
    for i in range(len(all_types)):
        for j in range(len(all_types)):
            value = matrix.iloc[i, j]
            if value > 0:
                fig.add_annotation(
                    x=j,
                    y=i,
                    text=f"{value:,.0f}",
                    showarrow=False,
                    font=dict(color='black' if value < matrix.values.max()/2 else 'white')
                )
    
    return fig

def create_top_transitions_chart(transition_df, n=10):
    """Create a bar chart showing top N transitions."""
    if transition_df.empty:
        return None
    
    # Sort by value
    sorted_df = transition_df.sort_values('value', ascending=False).head(n)
    
    # Add combined source-target column
    sorted_df['transition'] = sorted_df['source'] + ' → ' + sorted_df['target']
    
    # Create a bar chart
    fig = px.bar(
        sorted_df,
        y='transition',
        x='value',
        title=f'Top {n} Land Use Transitions',
        labels={'transition': 'Transition', 'value': 'Area (acres)'},
        height=500,
        orientation='h'
    )
    
    # Update layout
    fig.update_layout(
        yaxis=dict(categoryorder='total ascending'),
        margin=dict(l=25, r=25, t=70, b=25)
    )
    
    # Add annotations for values
    for i, row in sorted_df.iterrows():
        fig.add_annotation(
            x=row['value'],
            y=row['transition'],
            text=f"{row['value']:,.0f}",
            showarrow=False,
            xanchor='left',
            xshift=5,
            font=dict(color='black')
        )
    
    return fig

def main():
    # Page header
    st.markdown('<h1 class="title-text">Land Use Transition Analysis</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle-text">Analyze how land use patterns change over time and identify key transitions</p>', unsafe_allow_html=True)
    
    # Sidebar filters
    st.sidebar.header("Analysis Parameters")
    
    # Get data for filters
    scenarios = get_scenarios()
    years = get_years()
    region_types = get_regions()
    
    # Scenario selection
    scenario_options = [f"{s['name']} ({s['gcm']}, {s['rcp']}-{s['ssp']})" for s in scenarios]
    selected_scenario = st.sidebar.selectbox("Select Scenario", scenario_options, index=0)
    selected_scenario_id = scenarios[scenario_options.index(selected_scenario)]['id']
    
    # Year selection (start and end)
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_year = st.selectbox("Start Year", years, index=0)
    with col2:
        # Default to the last year if available
        end_index = len(years) - 1 if len(years) > 1 else 0
        end_year = st.selectbox("End Year", years, index=end_index)
    
    # Region type selection
    selected_region_type = st.sidebar.selectbox(
        "Region Type", 
        ["National"] + region_types,
        index=0
    )
    
    # Region selection (if not National)
    selected_region_id = None
    if selected_region_type != "National":
        regions = get_regions_by_type(selected_region_type)
        region_options = [r['name'] for r in regions]
        selected_region = st.sidebar.selectbox("Select Region", region_options, index=0)
        selected_region_id = regions[region_options.index(selected_region)]['id']
    
    # Land use filter for matrix
    land_use_types = get_land_use_types()
    selected_land_use_types = st.sidebar.multiselect(
        "Filter Land Use Types (for Matrix)", 
        land_use_types, 
        default=land_use_types if len(land_use_types) <= 8 else land_use_types[:8]
    )
    
    # Information card
    st.markdown('<div class="info-card">', unsafe_allow_html=True)
    st.markdown("""
    This analysis page helps you understand how land use is projected to change over time. The visualizations show:
    
    - **Sankey Diagram**: Flow of land between different uses over the selected time period
    - **Net Change**: Areas that are projected to gain or lose acreage between years
    - **Transition Matrix**: Detailed breakdown of all transitions between land use types
    - **Top Transitions**: The largest projected changes in land use by area
    
    Use the sidebar to select different scenarios, time periods, and regions.
    """)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Get transition data
    transition_data = get_transition_data(
        selected_scenario_id, 
        start_year, 
        end_year, 
        None if selected_region_type == "National" else selected_region_type,
        selected_region_id
    )
    
    # Main analysis
    if transition_data.empty:
        st.error("No transition data available for the selected parameters.")
    else:
        # Key metrics
        st.markdown('<div class="card">', unsafe_allow_html=True)
        
        total_transitioned = transition_data['value'].sum()
        unique_sources = transition_data['source'].nunique()
        unique_targets = transition_data['target'].nunique()
        max_transition = transition_data['value'].max()
        max_transition_pair = transition_data.loc[transition_data['value'].idxmax()]
        
        st.markdown("### Transition Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f'<div class="metric-value">{total_transitioned:,.0f}</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-label">Total Acres Transitioned</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f'<div class="metric-value">{len(transition_data)}</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-label">Unique Transitions</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f'<div class="metric-value">{max_transition:,.0f}</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-label">Largest Transition (acres)</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col4:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(f'<div class="metric-value">{max_transition_pair["source"]} → {max_transition_pair["target"]}</div>', unsafe_allow_html=True)
            st.markdown('<div class="metric-label">Largest Transition Type</div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Sankey diagram
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("Land Use Transition Flows")
        
        region_label = "National" if selected_region_type == "National" else f"{selected_region_type}: {selected_region}"
        sankey_title = f"Land Use Transitions ({start_year} to {end_year}) - {region_label}"
        
        sankey_chart = create_sankey_diagram(transition_data, title=sankey_title)
        if sankey_chart:
            st.plotly_chart(sankey_chart, use_container_width=True)
        else:
            st.info("Could not create Sankey diagram with the current data.")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Net change chart and Top transitions
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.subheader("Net Change by Land Use Type")
            
            net_change_chart = create_net_change_chart(transition_data, start_year, end_year)
            if net_change_chart:
                st.plotly_chart(net_change_chart, use_container_width=True)
            else:
                st.info("Could not create net change chart with the current data.")
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.subheader("Top Land Use Transitions")
            
            top_transitions_chart = create_top_transitions_chart(transition_data)
            if top_transitions_chart:
                st.plotly_chart(top_transitions_chart, use_container_width=True)
            else:
                st.info("Could not create top transitions chart with the current data.")
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Transition matrix
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("Transition Matrix")
        
        matrix_chart = create_transition_matrix(transition_data, selected_land_use_types)
        if matrix_chart:
            st.plotly_chart(matrix_chart, use_container_width=True)
        else:
            st.info("Could not create transition matrix with the current data.")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Data table
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.subheader("Transition Data")
        
        # Add percentage column
        transition_data_display = transition_data.copy()
        transition_data_display['percentage'] = (transition_data_display['value'] / transition_data_display['value'].sum()) * 100
        
        # Sort by value
        transition_data_display = transition_data_display.sort_values('value', ascending=False)
        
        # Format columns
        transition_data_display['value'] = transition_data_display['value'].map('{:,.2f}'.format)
        transition_data_display['percentage'] = transition_data_display['percentage'].map('{:.2f}%'.format)
        
        # Rename columns
        transition_data_display.columns = ['Source Land Use', 'Target Land Use', 'Acres', 'Percentage']
        
        # Display table
        st.dataframe(transition_data_display, use_container_width=True)
        
        # Download button
        csv = transition_data.to_csv(index=False)
        st.download_button(
            label="Download Transition Data (CSV)",
            data=csv,
            file_name=f"land_use_transitions_{start_year}_to_{end_year}.csv",
            mime="text/csv",
        )
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Footer
    st.markdown('<div class="footer">', unsafe_allow_html=True)
    st.markdown("Data source: National Land Use Projection Database | Developed for Regional Plan Association")
    st.markdown("© 2023 | All Rights Reserved")
    st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main() 