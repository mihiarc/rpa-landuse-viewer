import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from dotenv import load_dotenv
import sys
import os

# Add parent directory to path to access shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.db.database import DatabaseConnection
from src.db.queries import LandUseQueries

# Page config
st.set_page_config(
    page_title="Land Use Transitions | RPA Viewer",
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

@st.cache_data
def get_years():
    """Get a list of all available years from the database."""
    conn = DatabaseConnection.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT start_year FROM time_steps ORDER BY start_year")
    years = [row[0] for row in cursor.fetchall()]
    DatabaseConnection.close_connection(conn)
    return years

def get_transition_data(scenario_id=None, start_year=None, end_year=None):
    """Get land use transition data for analysis."""
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

def create_sankey_diagram(df, title="Land Use Transitions (Acres)"):
    """Create a Sankey diagram showing land use transitions."""
    if df.empty:
        return None
    
    # Create a copy of the dataframe to modify
    df = df.copy()
    
    # Normalize land use names (trim whitespace, convert to lowercase for comparison)
    df['from_land_use_norm'] = df['from_land_use'].str.strip().str.lower()
    df['to_land_use_norm'] = df['to_land_use'].str.strip().str.lower()
    
    # Filter out transitions where land use doesn't change (e.g., forest stays forest)
    df = df[df['from_land_use_norm'] != df['to_land_use_norm']]
    
    if df.empty:
        return None
    
    # Get unique land use types
    all_nodes = sorted(list(set(df['from_land_use'].unique()) | set(df['to_land_use'].unique())))
    
    # Create mapping dict for node indices
    node_indices = {node: i for i, node in enumerate(all_nodes)}
    
    # Create source, target, and value lists
    sources = [node_indices[row['from_land_use']] for _, row in df.iterrows()]
    targets = [node_indices[row['to_land_use']] for _, row in df.iterrows()]
    values = [row['total_acres'] for _, row in df.iterrows()]
    
    # Create node labels and colors
    node_labels = all_nodes
    node_colors = px.colors.qualitative.Bold[:len(all_nodes)]
    
    # Create the Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=node_labels,
            color=node_colors
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            hoverlabel=dict(bgcolor="white", font_size=14),
            hovertemplate="<b>%{source.label}</b> → <b>%{target.label}</b><br>" +
                          "Acres: %{value:,.0f}<extra></extra>"
        )
    )])
    
    # Update layout
    fig.update_layout(
        title_text=title,
        font_size=12,
        height=600,
        margin=dict(l=25, r=25, t=50, b=25)
    )
    
    return fig

def create_transition_matrix(df):
    """Create a transition matrix heatmap."""
    if df.empty:
        return None
    
    # Create a copy of the dataframe to modify
    df = df.copy()
    
    # Normalize land use names (trim whitespace, convert to lowercase for comparison)
    df['from_land_use_norm'] = df['from_land_use'].str.strip().str.lower()
    df['to_land_use_norm'] = df['to_land_use'].str.strip().str.lower()
    
    # Filter out transitions where land use doesn't change (e.g., forest stays forest)
    df = df[df['from_land_use_norm'] != df['to_land_use_norm']]
    
    if df.empty:
        return None
    
    # Create a pivot table for the transition matrix
    matrix = df.pivot_table(
        index='from_land_use', 
        columns='to_land_use', 
        values='total_acres', 
        aggfunc='sum',
        fill_value=0
    )
    
    # Create a heatmap
    fig = px.imshow(
        matrix,
        text_auto='.2s',  # Show values with SI prefix
        color_continuous_scale='Viridis',
        aspect="equal",
        labels=dict(x="To Land Use", y="From Land Use", color="Acres"),
        title="Land Use Transition Matrix (Acres)"
    )
    
    # Create custom text array with empty cells for diagonal
    text_array = []
    for i, row_idx in enumerate(matrix.index):
        text_row = []
        for j, col_idx in enumerate(matrix.columns):
            # If diagonal element (same from/to land use), show empty text
            if row_idx == col_idx:
                text_row.append("")
            else:
                # Format the value with SI prefix
                val = matrix.iloc[i, j]
                if val == 0:
                    text_row.append("")
                elif val >= 1e9:
                    text_row.append(f"{val/1e9:.1f}G")
                elif val >= 1e6:
                    text_row.append(f"{val/1e6:.1f}M")
                elif val >= 1e3:
                    text_row.append(f"{val/1e3:.1f}k")
                else:
                    text_row.append(f"{val:.1f}")
        text_array.append(text_row)
    
    # Update traces to use custom text
    fig.update_traces(
        text=text_array,
        texttemplate="%{text}",
        hovertemplate="From: %{y}<br>To: %{x}<br>Acres: %{z:,.2f}<extra></extra>"
    )
    
    # Update layout
    fig.update_layout(
        height=600,
        margin=dict(l=50, r=50, t=50, b=50),
        xaxis=dict(tickangle=45)
    )
    
    return fig

def create_net_change_chart(df):
    """Create a bar chart showing net change for each land use type."""
    if df.empty:
        return None
    
    # Calculate gains (transitions to a land use type)
    gains = df.groupby('to_land_use')['total_acres'].sum().reset_index()
    gains.rename(columns={'to_land_use': 'land_use_type', 'total_acres': 'gain'}, inplace=True)
    
    # Calculate losses (transitions from a land use type)
    losses = df.groupby('from_land_use')['total_acres'].sum().reset_index()
    losses.rename(columns={'from_land_use': 'land_use_type', 'total_acres': 'loss'}, inplace=True)
    
    # Merge gains and losses
    net_change = pd.merge(gains, losses, on='land_use_type', how='outer').fillna(0)
    
    # Calculate net change
    net_change['net_change'] = net_change['gain'] - net_change['loss']
    
    # Sort by absolute net change
    net_change = net_change.sort_values('net_change', key=abs, ascending=False)
    
    # Create a bar chart
    fig = px.bar(
        net_change,
        x='land_use_type',
        y='net_change',
        title="Net Land Use Change (Acres)",
        color='net_change',
        color_continuous_scale='RdBu_r',  # Blue for positive, Red for negative
        labels={'land_use_type': 'Land Use Type', 'net_change': 'Net Change (Acres)'}
    )
    
    # Update layout
    fig.update_layout(
        height=500,
        xaxis_title="Land Use Type",
        yaxis_title="Net Change (Acres)",
        yaxis=dict(zeroline=True, zerolinewidth=2, zerolinecolor='black'),
        coloraxis_showscale=False
    )
    
    return fig

def main():
    # Page header
    st.markdown('<h1 class="title-text">Land Use Transition Analysis</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle-text">Detailed analysis of land use transitions between categories</p>', unsafe_allow_html=True)

    # Sidebar filters
    st.sidebar.header("Filter Options")
    
    # Get data for filters
    scenarios = get_scenarios()
    years = get_years()
    
    # Scenario selection
    scenario_options = [f"{s['name']} ({s['gcm']}, {s['rcp']}-{s['ssp']})" for s in scenarios]
    selected_scenario = st.sidebar.selectbox("Select Scenario", scenario_options)
    selected_scenario_id = scenarios[scenario_options.index(selected_scenario)]['id']
    
    # Year range selection
    min_year = min(years) if years else 2020
    max_year = max(years) if years else 2070
    year_range = st.sidebar.slider(
        "Year Range", 
        min_value=min_year, 
        max_value=max_year,
        value=(min_year, max_year)
    )
    
    # Get filtered data
    transition_data = get_transition_data(
        scenario_id=selected_scenario_id,
        start_year=year_range[0],
        end_year=year_range[1]
    )
    
    # Summary metrics
    st.markdown('<div class="card">', unsafe_allow_html=True)
    
    # Calculate total transitions, acres, and major paths
    total_transitions = len(transition_data)
    total_acres = transition_data['total_acres'].sum()
    
    # Create three columns
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Transitions", f"{total_transitions:,}")
    
    with col2:
        st.metric("Total Acres Changed", f"{total_acres:,.0f}")
    
    with col3:
        # Find the most significant transition
        if not transition_data.empty:
            max_transition = transition_data.loc[transition_data['total_acres'].idxmax()]
            max_transition_text = f"{max_transition['from_land_use']} → {max_transition['to_land_use']}"
            max_transition_acres = f"{max_transition['total_acres']:,.0f} acres"
            st.metric("Largest Transition", max_transition_text, max_transition_acres)
        else:
            st.metric("Largest Transition", "N/A")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Display visualizations in tabs
    tab1, tab2, tab3, tab4 = st.tabs(["Sankey Diagram", "Transition Matrix", "Net Change", "Data Table"])
    
    with tab1:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        sankey_title = f"Land Use Transitions ({year_range[0]}-{year_range[1]})"
        fig_sankey = create_sankey_diagram(transition_data, title=sankey_title)
        if fig_sankey:
            st.plotly_chart(fig_sankey, use_container_width=True)
        else:
            st.info("No transition data available for the selected filters.")
        
        st.markdown("""
        ### About Sankey Diagrams
        
        This Sankey diagram visualizes the flow of land between different use categories:
        
        - Each **node** represents a land use category
        - The **links** (flows) show transitions from one category to another
        - The **width** of each flow corresponds to the acreage of land transitioning
        - **Hover** over the flows to see exact acreage values
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        fig_matrix = create_transition_matrix(transition_data)
        if fig_matrix:
            st.plotly_chart(fig_matrix, use_container_width=True)
        else:
            st.info("No transition data available for the selected filters.")
        
        st.markdown("""
        ### Understanding the Matrix
        
        This heatmap shows the intensity of transitions between land uses:
        
        - **Rows** represent the source land use category
        - **Columns** represent the destination land use category
        - **Color intensity** indicates the magnitude of change (acres)
        - The **diagonal** (if present) represents land that remained in the same category
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab3:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        fig_net_change = create_net_change_chart(transition_data)
        if fig_net_change:
            st.plotly_chart(fig_net_change, use_container_width=True)
        else:
            st.info("No transition data available for the selected filters.")
        
        st.markdown("""
        ### Net Change Analysis
        
        This chart shows the net gain or loss for each land use type:
        
        - **Blue bars** indicate net gains (more land converted to this type than from it)
        - **Red bars** indicate net losses (more land converted from this type than to it)
        - The **magnitude** of each bar shows the total net change in acres
        """)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with tab4:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        if not transition_data.empty:
            # Format the data for display
            display_data = transition_data.copy()
            display_data['total_acres'] = display_data['total_acres'].round(2)
            
            # Add a transition label column
            display_data['transition'] = display_data['from_land_use'] + ' → ' + display_data['to_land_use']
            
            # Reorder columns for better display
            display_cols = ['transition', 'total_acres', 'start_year', 'scenario_name', 'gcm', 'rcp', 'ssp']
            st.dataframe(display_data[display_cols], use_container_width=True)
            
            # Add download button
            csv = display_data.to_csv(index=False)
            st.download_button(
                label="Download Data as CSV",
                data=csv,
                file_name=f"land_use_transitions_{year_range[0]}-{year_range[1]}.csv",
                mime="text/csv"
            )
        else:
            st.info("No transition data available for the selected filters.")
        st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main() 