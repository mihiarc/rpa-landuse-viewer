"""
Advanced visualization functions for RPA Land Use Viewer.

This module contains specialized visualization functions that extend the basic
visualizations in the main visualization module.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from typing import Dict, List, Optional, Any, Union

@st.cache_data
def create_sankey_diagram(
    transitions: List[Dict[str, Any]],
    title: str = "Land Use Transitions"
) -> go.Figure:
    """
    Create a Sankey diagram showing land use transitions.
    
    Args:
        transitions: List of dictionaries containing from_land_use, to_land_use, and acres_changed
        title: Title for the diagram
        
    Returns:
        Plotly figure object
    """
    # Create lists for Sankey diagram
    land_uses = list(set(
        [t['from_land_use'] for t in transitions] + 
        [t['to_land_use'] for t in transitions]
    ))
    
    # Create node labels
    label_dict = {lu: i for i, lu in enumerate(land_uses)}
    
    # Create source, target, and value lists
    sources = [label_dict[t['from_land_use']] for t in transitions]
    targets = [label_dict[t['to_land_use']] for t in transitions]
    values = [t['acres_changed'] for t in transitions]
    
    # Create node colors (optional)
    node_colors = [
        "#1f77b4" if "Urban" in lu else  # blue
        "#ff7f0e" if "Crop" in lu else   # orange
        "#2ca02c" if "Forest" in lu else # green
        "#d62728" if "Range" in lu else  # red
        "#9467bd" if "Pasture" in lu else # purple
        "#8c564b"  # brown (default)
        for lu in land_uses
    ]
    
    # Simplify link colors to avoid f-string complexity
    link_colors = ["rgba(31, 119, 180, 0.4)" for _ in sources]  # Default blue with transparency
    
    # Create Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=land_uses,
            color=node_colors
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            color=link_colors
        )
    )])
    
    # Update layout
    fig.update_layout(
        title_text=title,
        font_size=10,
        margin=dict(l=25, r=25, t=50, b=25)
    )
    
    return fig

@st.cache_data
def create_scenario_comparison_chart(
    comparison_data: Dict[str, Any],
    land_use_type: str,
    year_range: str
) -> go.Figure:
    """
    Create a comparison visualization between two scenarios.
    
    Args:
        comparison_data: Dictionary with 'scenarios' and 'difference' keys
        land_use_type: Type of land use being analyzed
        year_range: String indicating the year range (e.g., "2020-2050")
        
    Returns:
        Plotly figure object
    """
    scenarios = comparison_data.get('scenarios', [])
    
    if not scenarios or len(scenarios) < 1:
        # Return empty figure if no data
        return go.Figure()
    
    # Create figure
    fig = go.Figure()
    
    # Add bars for net change
    fig.add_trace(go.Bar(
        name='Net Change',
        x=[s['scenario_name'] for s in scenarios],
        y=[s['net_change'] for s in scenarios],
        text=[f"{s['net_change']:,.0f} acres" for s in scenarios],
        textposition='auto',
        marker_color='#1f77b4'
    ))
    
    # Add markers for annual rate
    fig.add_trace(go.Scatter(
        name='Annual Rate',
        x=[s['scenario_name'] for s in scenarios],
        y=[s['annual_rate'] for s in scenarios],
        mode='markers',
        marker=dict(size=15, symbol='diamond', color='#ff7f0e'),
        yaxis='y2'
    ))
    
    # Update layout
    fig.update_layout(
        title=f'{land_use_type} Land Change Comparison ({year_range})',
        yaxis=dict(
            title='Net Change (acres)',
            titlefont=dict(color="#1f77b4"),
            tickfont=dict(color="#1f77b4")
        ),
        yaxis2=dict(
            title='Annual Rate (acres/year)',
            titlefont=dict(color="#ff7f0e"),
            tickfont=dict(color="#ff7f0e"),
            overlaying='y',
            side='right'
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        ),
        margin=dict(l=50, r=50, t=50, b=50)
    )
    
    # Add difference annotation if comparison available
    if 'difference' in comparison_data and len(scenarios) == 2:
        diff = comparison_data['difference']
        diff_text = (
            f"Difference: {diff['net_change_diff']:,.0f} acres "
            f"({diff['percent_diff']:.1f}%)"
        )
        fig.add_annotation(
            x=0.5,
            y=1.05,
            xref="paper",
            yref="paper",
            text=diff_text,
            showarrow=False,
            font=dict(size=14)
        )
    
    return fig

@st.cache_data
def create_time_series_comparison(
    data: List[Dict[str, Any]],
    start_year: int,
    end_year: int,
    land_use_types: Optional[List[str]] = None
) -> go.Figure:
    """
    Create a time series plot comparing land use changes across years.
    
    Args:
        data: List of dictionaries with year, land_use, and net_change
        start_year: Starting year for the analysis
        end_year: Ending year for the analysis
        land_use_types: Optional list of land use types to include
        
    Returns:
        Plotly figure object
    """
    if not land_use_types:
        # Get unique land use types from data
        land_use_types = list(set([d['land_use'] for d in data]))
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Create figure
    fig = go.Figure()
    
    # Define colors for consistency
    colors = px.colors.qualitative.Plotly
    
    # Add a line for each land use type
    for i, land_use in enumerate(land_use_types):
        land_use_data = df[df['land_use'] == land_use]
        if not land_use_data.empty:
            fig.add_trace(go.Scatter(
                x=land_use_data['year'],
                y=land_use_data['net_change'],
                name=land_use,
                mode='lines+markers',
                line=dict(color=colors[i % len(colors)], width=2),
                marker=dict(size=8)
            ))
    
    # Update layout
    fig.update_layout(
        title=f'Land Use Changes Over Time ({start_year}-{end_year})',
        xaxis_title='Year',
        yaxis_title='Net Change (acres)',
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5
        ),
        margin=dict(l=50, r=50, t=50, b=80)
    )
    
    return fig 