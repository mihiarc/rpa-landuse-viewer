"""
Shared visualization functions for RPA Land Use Viewer.

This module contains common visualization functions used across multiple pages in the Streamlit app.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from typing import Dict, List, Any, Optional, Union

@st.cache_data(ttl=3600)
def create_choropleth_map(
    df: pd.DataFrame,
    geo_col: str = 'state',
    value_col: str = 'total_acres',
    title: str = "Geographic Distribution",
    scope: str = "usa"
) -> go.Figure:
    """Create a choropleth map for geographic data.
    
    Args:
        df (pd.DataFrame): DataFrame containing geographic data
        geo_col (str, optional): Column with geographic entities. Defaults to 'state'.
        value_col (str, optional): Column with values to display. Defaults to 'total_acres'.
        title (str, optional): Chart title. Defaults to "Geographic Distribution".
        scope (str, optional): Geographic scope. Defaults to "usa".
    
    Returns:
        go.Figure: Plotly choropleth map figure
    """
    # Define a better colorscale
    colorscale = px.colors.sequential.Viridis
    
    # Create the choropleth
    fig = px.choropleth(
        df,
        locations=geo_col,
        locationmode='USA-states' if geo_col == 'state' else 'geojson-id',
        color=value_col,
        color_continuous_scale=colorscale,
        scope=scope,
        title=title,
        labels={value_col: value_col.replace('_', ' ').title()}
    )
    
    # Improve layout
    fig.update_layout(
        margin={"r": 0, "t": 40, "l": 0, "b": 0},
        coloraxis_colorbar=dict(
            title=value_col.replace('_', ' ').title(),
            thicknessmode="pixels", 
            thickness=20,
            lenmode="pixels", 
            len=300
        )
    )
    
    return fig

@st.cache_data(ttl=3600)
def create_bar_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str = "Comparative Analysis",
    x_title: Optional[str] = None,
    y_title: Optional[str] = None,
    color_col: Optional[str] = None,
    orientation: str = 'v'
) -> go.Figure:
    """Create a bar chart for comparing values.
    
    Args:
        df (pd.DataFrame): DataFrame containing data
        x_col (str): Column for x-axis
        y_col (str): Column for y-axis
        title (str, optional): Chart title. Defaults to "Comparative Analysis".
        x_title (Optional[str], optional): X-axis title. Defaults to None.
        y_title (Optional[str], optional): Y-axis title. Defaults to None.
        color_col (Optional[str], optional): Column for color. Defaults to None.
        orientation (str, optional): Bar orientation ('v' for vertical, 'h' for horizontal). Defaults to 'v'.
    
    Returns:
        go.Figure: Plotly bar chart figure
    """
    # Format axis titles if not provided
    if not x_title:
        x_title = x_col.replace('_', ' ').title()
    if not y_title:
        y_title = y_col.replace('_', ' ').title()
    
    # Handle potentially large datasets by limiting
    if len(df) > 30:
        if orientation == 'v':
            # For vertical charts, sort by y-value and take top 30
            df = df.sort_values(by=y_col, ascending=False).head(30)
        else:
            # For horizontal charts, sort by y-value and take top 30
            df = df.sort_values(by=y_col, ascending=True).head(30)
    
    # Create the bar chart
    if color_col:
        fig = px.bar(
            df,
            x=x_col if orientation == 'v' else y_col,
            y=y_col if orientation == 'v' else x_col,
            color=color_col,
            title=title,
            orientation=orientation,
            labels={
                x_col: x_title,
                y_col: y_title,
                color_col: color_col.replace('_', ' ').title()
            }
        )
    else:
        fig = px.bar(
            df,
            x=x_col if orientation == 'v' else y_col,
            y=y_col if orientation == 'v' else x_col,
            title=title,
            orientation=orientation,
            labels={
                x_col: x_title,
                y_col: y_title
            }
        )
    
    # Improve layout
    fig.update_layout(
        margin={"r": 20, "t": 40, "l": 20, "b": 20},
        xaxis_title=x_title if orientation == 'v' else y_title,
        yaxis_title=y_title if orientation == 'v' else x_title
    )
    
    return fig

@st.cache_data(ttl=3600)
def create_pie_chart(
    df: pd.DataFrame,
    values_col: str,
    names_col: str,
    title: str = "Proportional Analysis"
) -> go.Figure:
    """Create a pie chart for showing proportions.
    
    Args:
        df (pd.DataFrame): DataFrame containing data
        values_col (str): Column for values/sizes
        names_col (str): Column for segment names/labels
        title (str, optional): Chart title. Defaults to "Proportional Analysis".
    
    Returns:
        go.Figure: Plotly pie chart figure
    """
    # Create the pie chart
    fig = px.pie(
        df,
        values=values_col,
        names=names_col,
        title=title,
        hole=0.4,  # Creates a donut chart which is often more readable
        labels={
            values_col: values_col.replace('_', ' ').title(),
            names_col: names_col.replace('_', ' ').title()
        }
    )
    
    # Improve layout
    fig.update_layout(
        margin={"r": 20, "t": 40, "l": 20, "b": 20},
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        )
    )
    
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        insidetextfont=dict(size=12)
    )
    
    return fig

@st.cache_data(ttl=3600)
def create_line_chart(
    df: pd.DataFrame,
    x_col: str,
    y_cols: List[str],
    title: str = "Temporal Analysis",
    x_title: Optional[str] = None,
    y_title: Optional[str] = None,
    color_discrete_map: Optional[Dict[str, str]] = None
) -> go.Figure:
    """Create a line chart for showing trends over time.
    
    Args:
        df (pd.DataFrame): DataFrame containing time series data
        x_col (str): Column for x-axis (usually time/date)
        y_cols (List[str]): Columns for y-axis values
        title (str, optional): Chart title. Defaults to "Temporal Analysis".
        x_title (Optional[str], optional): X-axis title. Defaults to None.
        y_title (Optional[str], optional): Y-axis title. Defaults to None.
        color_discrete_map (Optional[Dict[str, str]], optional): Color mapping for lines. Defaults to None.
    
    Returns:
        go.Figure: Plotly line chart figure
    """
    # Format axis titles if not provided
    if not x_title:
        x_title = x_col.replace('_', ' ').title()
    if not y_title:
        y_title = "Value"
    
    # Create the line chart
    fig = go.Figure()
    
    # Add each line
    for y_col in y_cols:
        y_label = y_col.replace('_', ' ').title()
        color = None if color_discrete_map is None else color_discrete_map.get(y_col)
        
        fig.add_trace(
            go.Scatter(
                x=df[x_col],
                y=df[y_col],
                mode='lines+markers',
                name=y_label,
                marker=dict(size=8),
                line=dict(width=3, color=color)
            )
        )
    
    # Improve layout
    fig.update_layout(
        title=title,
        xaxis_title=x_title,
        yaxis_title=y_title,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        ),
        margin={"r": 20, "t": 40, "l": 20, "b": 20},
        hovermode="x unified"
    )
    
    return fig

@st.cache_data(ttl=3600)
def create_sankey_diagram(
    df: pd.DataFrame,
    source_col: str,
    target_col: str,
    value_col: str,
    title: str = "Flow Diagram"
) -> go.Figure:
    """Create a Sankey diagram showing transitions between states.
    
    Args:
        df (pd.DataFrame): DataFrame containing transition data
        source_col (str): Column for source nodes
        target_col (str): Column for target nodes
        value_col (str): Column for link values/weights
        title (str, optional): Chart title. Defaults to "Flow Diagram".
    
    Returns:
        go.Figure: Plotly Sankey diagram figure
    """
    # Get unique source and target values
    all_nodes = sorted(list(set(df[source_col].unique()) | set(df[target_col].unique())))
    
    # Create mapping dict for node indices
    node_indices = {node: i for i, node in enumerate(all_nodes)}
    
    # Create source, target, and value lists
    sources = [node_indices[row[source_col]] for _, row in df.iterrows()]
    targets = [node_indices[row[target_col]] for _, row in df.iterrows()]
    values = [row[value_col] for _, row in df.iterrows()]
    
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
                          f"{value_col}: %{{value:,.0f}}<extra></extra>"
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

# Additional visualization functions from visualization.py

@st.cache_data(ttl=3600)
def create_heatmap(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    z_col: str,
    title: str = "Heatmap Analysis",
    color_scale: str = "Viridis",
    x_title: Optional[str] = None,
    y_title: Optional[str] = None
) -> go.Figure:
    """Create a heatmap visualization.
    
    Args:
        df (pd.DataFrame): DataFrame containing data
        x_col (str): Column for x-axis
        y_col (str): Column for y-axis
        z_col (str): Column for z-axis (values)
        title (str, optional): Chart title. Defaults to "Heatmap Analysis".
        color_scale (str, optional): Color scale name. Defaults to "Viridis".
        x_title (Optional[str], optional): X-axis title. Defaults to None.
        y_title (Optional[str], optional): Y-axis title. Defaults to None.
    
    Returns:
        go.Figure: Plotly heatmap figure
    """
    # Format axis titles if not provided
    if not x_title:
        x_title = x_col.replace('_', ' ').title()
    if not y_title:
        y_title = y_col.replace('_', ' ').title()
    
    # Create pivot table if needed
    if len(df[[x_col, y_col, z_col]].drop_duplicates()) == len(df):
        # Data is already in the right format for a pivot
        pivot_df = df.pivot(index=y_col, columns=x_col, values=z_col)
    else:
        # We need to aggregate
        pivot_df = df.groupby([y_col, x_col])[z_col].sum().unstack()
    
    # Create the heatmap
    fig = px.imshow(
        pivot_df,
        color_continuous_scale=color_scale,
        labels=dict(color=z_col.replace('_', ' ').title()),
        title=title
    )
    
    # Improve layout
    fig.update_layout(
        xaxis_title=x_title,
        yaxis_title=y_title,
        margin={"r": 20, "t": 40, "l": 20, "b": 20}
    )
    
    return fig

@st.cache_data(ttl=3600)
def create_scatter_plot(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str = "Correlation Analysis",
    color_col: Optional[str] = None,
    size_col: Optional[str] = None,
    x_title: Optional[str] = None,
    y_title: Optional[str] = None,
    trendline: Optional[str] = None,
    hover_data: Optional[List[str]] = None
) -> go.Figure:
    """Create a scatter plot for showing relationships between variables.
    
    Args:
        df (pd.DataFrame): DataFrame containing data
        x_col (str): Column for x-axis
        y_col (str): Column for y-axis
        title (str, optional): Chart title. Defaults to "Correlation Analysis".
        color_col (Optional[str], optional): Column for point color. Defaults to None.
        size_col (Optional[str], optional): Column for point size. Defaults to None.
        x_title (Optional[str], optional): X-axis title. Defaults to None.
        y_title (Optional[str], optional): Y-axis title. Defaults to None.
        trendline (Optional[str], optional): Trendline type ('ols', 'lowess'). Defaults to None.
        hover_data (Optional[List[str]], optional): Additional columns to show on hover. Defaults to None.
    
    Returns:
        go.Figure: Plotly scatter plot figure
    """
    # Format axis titles if not provided
    if not x_title:
        x_title = x_col.replace('_', ' ').title()
    if not y_title:
        y_title = y_col.replace('_', ' ').title()
    
    # Create the scatter plot
    fig = px.scatter(
        df,
        x=x_col,
        y=y_col,
        color=color_col,
        size=size_col,
        title=title,
        trendline=trendline,
        hover_data=hover_data,
        labels={
            x_col: x_title,
            y_col: y_title,
            color_col: color_col.replace('_', ' ').title() if color_col else None,
            size_col: size_col.replace('_', ' ').title() if size_col else None
        }
    )
    
    # Improve layout
    fig.update_layout(
        xaxis_title=x_title,
        yaxis_title=y_title,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin={"r": 20, "t": 40, "l": 20, "b": 20}
    )
    
    return fig

@st.cache_data(ttl=3600)
def create_stacked_area_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    color_col: str,
    title: str = "Temporal Composition",
    x_title: Optional[str] = None,
    y_title: Optional[str] = None,
    normalized: bool = False
) -> go.Figure:
    """Create a stacked area chart for showing composition over time.
    
    Args:
        df (pd.DataFrame): DataFrame containing data
        x_col (str): Column for x-axis (usually time)
        y_col (str): Column for y-axis values
        color_col (str): Column for grouping and stacking
        title (str, optional): Chart title. Defaults to "Temporal Composition".
        x_title (Optional[str], optional): X-axis title. Defaults to None.
        y_title (Optional[str], optional): Y-axis title. Defaults to None.
        normalized (bool, optional): Whether to normalize values (0-100%). Defaults to False.
    
    Returns:
        go.Figure: Plotly area chart figure
    """
    # Format axis titles if not provided
    if not x_title:
        x_title = x_col.replace('_', ' ').title()
    if not y_title:
        y_title = y_col.replace('_', ' ').title()
    
    # Create the area chart
    fig = px.area(
        df,
        x=x_col,
        y=y_col,
        color=color_col,
        title=title,
        labels={
            x_col: x_title,
            y_col: y_title,
            color_col: color_col.replace('_', ' ').title()
        }
    )
    
    if normalized:
        fig = px.area(
            df,
            x=x_col,
            y=y_col,
            color=color_col,
            title=title,
            groupnorm='percent',
            labels={
                x_col: x_title,
                y_col: "Percentage (%)",
                color_col: color_col.replace('_', ' ').title()
            }
        )
    
    # Improve layout
    fig.update_layout(
        xaxis_title=x_title,
        yaxis_title=y_title if not normalized else "Percentage (%)",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        margin={"r": 20, "t": 40, "l": 20, "b": 20}
    )
    
    return fig

@st.cache_data(ttl=3600)
def create_box_plot(
    df: pd.DataFrame,
    y_col: str,
    x_col: Optional[str] = None,
    title: str = "Distribution Analysis",
    x_title: Optional[str] = None,
    y_title: Optional[str] = None,
    color_col: Optional[str] = None,
    points: str = "outliers"
) -> go.Figure:
    """Create a box plot for showing distributions.
    
    Args:
        df (pd.DataFrame): DataFrame containing data
        y_col (str): Column for y-axis (values to analyze)
        x_col (Optional[str], optional): Column for x-axis categories. Defaults to None.
        title (str, optional): Chart title. Defaults to "Distribution Analysis".
        x_title (Optional[str], optional): X-axis title. Defaults to None.
        y_title (Optional[str], optional): Y-axis title. Defaults to None.
        color_col (Optional[str], optional): Column for box colors. Defaults to None.
        points (str, optional): Points to show ('all', 'outliers', 'suspectedoutliers', False). Defaults to "outliers".
    
    Returns:
        go.Figure: Plotly box plot figure
    """
    # Format axis titles if not provided
    if x_col and not x_title:
        x_title = x_col.replace('_', ' ').title()
    if not y_title:
        y_title = y_col.replace('_', ' ').title()
    
    # Create the box plot
    fig = px.box(
        df,
        x=x_col,
        y=y_col,
        color=color_col,
        title=title,
        points=points,
        labels={
            x_col: x_title if x_col else "",
            y_col: y_title,
            color_col: color_col.replace('_', ' ').title() if color_col else None
        }
    )
    
    # Improve layout
    fig.update_layout(
        xaxis_title=x_title if x_col else "",
        yaxis_title=y_title,
        showlegend=color_col is not None,
        margin={"r": 20, "t": 40, "l": 20, "b": 20}
    )
    
    return fig

@st.cache_data(ttl=3600)
def create_histogram(
    df: pd.DataFrame,
    x_col: str,
    title: str = "Frequency Distribution",
    x_title: Optional[str] = None,
    y_title: str = "Count",
    color_col: Optional[str] = None,
    nbins: Optional[int] = None,
    histnorm: Optional[str] = None
) -> go.Figure:
    """Create a histogram for showing value distributions.
    
    Args:
        df (pd.DataFrame): DataFrame containing data
        x_col (str): Column for x-axis (values to bin)
        title (str, optional): Chart title. Defaults to "Frequency Distribution".
        x_title (Optional[str], optional): X-axis title. Defaults to None.
        y_title (str, optional): Y-axis title. Defaults to "Count".
        color_col (Optional[str], optional): Column for bar colors. Defaults to None.
        nbins (Optional[int], optional): Number of bins. Defaults to None.
        histnorm (Optional[str], optional): Histogram normalization ('percent', 'probability', 'density', 'probability density'). Defaults to None.
    
    Returns:
        go.Figure: Plotly histogram figure
    """
    # Format axis titles if not provided
    if not x_title:
        x_title = x_col.replace('_', ' ').title()
    
    if histnorm == 'percent':
        y_title = "Percentage (%)"
    elif histnorm == 'probability':
        y_title = "Probability"
    elif histnorm == 'density' or histnorm == 'probability density':
        y_title = "Density"
    
    # Create the histogram
    fig = px.histogram(
        df,
        x=x_col,
        color=color_col,
        title=title,
        nbins=nbins,
        histnorm=histnorm,
        labels={
            x_col: x_title,
            color_col: color_col.replace('_', ' ').title() if color_col else None
        }
    )
    
    # Improve layout
    fig.update_layout(
        xaxis_title=x_title,
        yaxis_title=y_title,
        showlegend=color_col is not None,
        bargap=0.1,
        margin={"r": 20, "t": 40, "l": 20, "b": 20}
    )
    
    return fig

@st.cache_data(ttl=3600)
def create_sunburst_chart(
    df: pd.DataFrame,
    path_cols: List[str],
    values_col: str,
    title: str = "Hierarchical Breakdown",
    color_col: Optional[str] = None
) -> go.Figure:
    """Create a sunburst chart for showing hierarchical data.
    
    Args:
        df (pd.DataFrame): DataFrame containing data
        path_cols (List[str]): List of columns defining the hierarchical path
        values_col (str): Column for segment sizes
        title (str, optional): Chart title. Defaults to "Hierarchical Breakdown".
        color_col (Optional[str], optional): Column for segment colors. Defaults to None.
    
    Returns:
        go.Figure: Plotly sunburst chart figure
    """
    # Create the sunburst chart
    fig = px.sunburst(
        df,
        path=path_cols,
        values=values_col,
        color=color_col or path_cols[-1],
        title=title,
        labels={
            col: col.replace('_', ' ').title() for col in path_cols + [values_col] + ([color_col] if color_col else [])
        }
    )
    
    # Improve layout
    fig.update_layout(
        margin=dict(t=40, l=0, r=0, b=0)
    )
    
    # Add percentage text
    fig.update_traces(
        textinfo="label+percent parent+value",
        insidetextorientation='radial'
    )
    
    return fig 