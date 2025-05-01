"""
Visualization utilities for the RPA Land Use Viewer.

This module provides functions for creating maps, charts, and other visualizations
for land use data analysis.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import geopandas as gpd
from typing import Any, Dict, List, Optional, Tuple, Union
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from .config import get_map_config, get_chart_config

def create_choropleth_map(
    data: pd.DataFrame,
    geojson: Dict[str, Any],
    value_column: str,
    location_column: str = "GEOID",
    feature_id_property: str = "GEOID",
    color_scale: Optional[str] = None,
    title: str = "",
    hover_data: Optional[List[str]] = None,
    zoom: Optional[float] = None,
    center: Optional[List[float]] = None,
    height: Optional[int] = None,
    custom_data: Optional[List[str]] = None,
    custom_hover_template: Optional[str] = None,
) -> go.Figure:
    """
    Create a choropleth map visualization for geographic data.
    
    Args:
        data: DataFrame containing data to visualize
        geojson: GeoJSON data for map boundaries
        value_column: Column containing values to map to colors
        location_column: Column containing location identifiers
        feature_id_property: Property in GeoJSON that matches location_column
        color_scale: Color scale for the map
        title: Map title
        hover_data: Additional columns to show in hover tooltip
        zoom: Initial zoom level
        center: Initial map center [lat, lon]
        height: Map height in pixels
        custom_data: Additional data to include for custom hover template
        custom_hover_template: Custom hover template
        
    Returns:
        Plotly figure object
    """
    map_config = get_map_config()
    
    # Use defaults from config if not specified
    if color_scale is None:
        color_scale = map_config["default_color_scale"]
    if zoom is None:
        zoom = map_config["default_zoom"]
    if center is None:
        center = map_config["default_center"]
    if height is None:
        height = map_config["map_height"]
    
    # Default hover data if not specified
    if hover_data is None:
        hover_data = []
        
    # Create base choropleth map
    fig = px.choropleth_mapbox(
        data,
        geojson=geojson,
        locations=location_column,
        featureidkey=f"properties.{feature_id_property}",
        color=value_column,
        color_continuous_scale=color_scale,
        hover_data=hover_data,
        custom_data=custom_data,
        mapbox_style="carto-positron",
        zoom=zoom,
        center={"lat": center[0], "lon": center[1]},
        opacity=0.7,
        height=height,
        title=title
    )
    
    # Apply custom hover template if provided
    if custom_hover_template:
        fig.update_traces(hovertemplate=custom_hover_template)
    
    # Improve layout
    fig.update_layout(
        margin={"r": 0, "t": 30, "l": 0, "b": 0},
        coloraxis_colorbar={
            "title": value_column,
            "thickness": 15,
            "len": 0.7,
            "xanchor": "left",
            "x": 0.01,
        },
    )
    
    return fig

def create_bar_chart(
    data: pd.DataFrame,
    x: str,
    y: str,
    color: Optional[str] = None,
    title: str = "",
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    height: Optional[int] = None,
    color_discrete_map: Optional[Dict[str, str]] = None,
    barmode: str = "group",
    text_auto: bool = False,
    orientation: str = "v",
) -> go.Figure:
    """
    Create a bar chart visualization.
    
    Args:
        data: DataFrame containing data to visualize
        x: Column to use for x-axis
        y: Column to use for y-axis
        color: Column to use for color grouping
        title: Chart title
        x_label: X-axis label
        y_label: Y-axis label
        height: Chart height in pixels
        color_discrete_map: Mapping of categories to colors
        barmode: Bar mode ('group', 'stack', 'overlay', 'relative')
        text_auto: Automatically show values on bars
        orientation: Orientation ('v' for vertical, 'h' for horizontal)
        
    Returns:
        Plotly figure object
    """
    charts_config = get_chart_config()
    
    # Use defaults from config if not specified
    if height is None:
        height = charts_config["chart_height"]
    
    # Create bar chart
    fig = px.bar(
        data,
        x=x if orientation == "v" else y,
        y=y if orientation == "v" else x,
        color=color,
        title=title,
        height=height,
        color_discrete_map=color_discrete_map,
        barmode=barmode,
        text_auto=text_auto,
        orientation=orientation,
    )
    
    # Update layout with labels
    fig.update_layout(
        xaxis_title=x_label if x_label else (x if orientation == "v" else y),
        yaxis_title=y_label if y_label else (y if orientation == "v" else x),
        showlegend=color is not None,
        plot_bgcolor="white",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Add grid based on config
    if charts_config["show_grid"]:
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="lightgray")
    
    return fig

def create_line_chart(
    data: pd.DataFrame,
    x: str,
    y: Union[str, List[str]],
    color: Optional[str] = None,
    title: str = "",
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    height: Optional[int] = None,
    color_discrete_map: Optional[Dict[str, str]] = None,
    markers: bool = True,
) -> go.Figure:
    """
    Create a line chart visualization.
    
    Args:
        data: DataFrame containing data to visualize
        x: Column to use for x-axis
        y: Column(s) to use for y-axis
        color: Column to use for color grouping
        title: Chart title
        x_label: X-axis label
        y_label: Y-axis label
        height: Chart height in pixels
        color_discrete_map: Mapping of categories to colors
        markers: Whether to show markers on the lines
        
    Returns:
        Plotly figure object
    """
    charts_config = get_chart_config()
    
    # Use defaults from config if not specified
    if height is None:
        height = charts_config["chart_height"]
    
    # Create line chart
    fig = px.line(
        data,
        x=x,
        y=y,
        color=color,
        title=title,
        height=height,
        color_discrete_map=color_discrete_map,
        markers=markers,
    )
    
    # Update layout with labels
    fig.update_layout(
        xaxis_title=x_label if x_label else x,
        yaxis_title=y_label if y_label else (y if isinstance(y, str) else "Value"),
        showlegend=color is not None or isinstance(y, list),
        plot_bgcolor="white",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Add grid based on config
    if charts_config["show_grid"]:
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="lightgray")
    
    return fig

def create_pie_chart(
    data: pd.DataFrame,
    names: str,
    values: str,
    title: str = "",
    color_discrete_map: Optional[Dict[str, str]] = None,
    height: Optional[int] = None,
    hole: float = 0,
) -> go.Figure:
    """
    Create a pie chart visualization.
    
    Args:
        data: DataFrame containing data to visualize
        names: Column with category names
        values: Column with values
        title: Chart title
        color_discrete_map: Mapping of categories to colors
        height: Chart height in pixels
        hole: Size of hole in middle (0-1, 0 for pie, >0 for donut)
        
    Returns:
        Plotly figure object
    """
    charts_config = get_chart_config()
    
    # Use defaults from config if not specified
    if height is None:
        height = charts_config["chart_height"]
    
    # Create pie chart
    fig = px.pie(
        data,
        names=names,
        values=values,
        title=title,
        height=height,
        color=names,
        color_discrete_map=color_discrete_map,
        hole=hole,
    )
    
    # Improve layout
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        )
    )
    
    return fig

def create_heatmap(
    data: pd.DataFrame,
    x: str,
    y: str,
    z: str,
    title: str = "",
    color_scale: Optional[str] = None,
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    height: Optional[int] = None,
    text_auto: bool = True,
) -> go.Figure:
    """
    Create a heatmap visualization.
    
    Args:
        data: DataFrame containing data to visualize
        x: Column to use for x-axis
        y: Column to use for y-axis
        z: Column with values for color intensity
        title: Chart title
        color_scale: Color scale for heatmap
        x_label: X-axis label
        y_label: Y-axis label
        height: Chart height in pixels
        text_auto: Automatically show values in cells
        
    Returns:
        Plotly figure object
    """
    charts_config = get_chart_config()
    
    # Use defaults from config if not specified
    if height is None:
        height = charts_config["chart_height"]
    if color_scale is None:
        color_scale = charts_config["default_color_palette"]
    
    # Pivot data for heatmap
    pivot_data = data.pivot_table(index=y, columns=x, values=z)
    
    # Create heatmap
    fig = px.imshow(
        pivot_data,
        x=pivot_data.columns,
        y=pivot_data.index,
        color_continuous_scale=color_scale,
        title=title,
        height=height,
        text_auto=text_auto,
    )
    
    # Update layout with labels
    fig.update_layout(
        xaxis_title=x_label if x_label else x,
        yaxis_title=y_label if y_label else y,
    )
    
    return fig

def create_scatter_plot(
    data: pd.DataFrame,
    x: str,
    y: str,
    color: Optional[str] = None,
    size: Optional[str] = None,
    title: str = "",
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    height: Optional[int] = None,
    color_discrete_map: Optional[Dict[str, str]] = None,
    trendline: Optional[str] = None,
    hover_data: Optional[List[str]] = None,
) -> go.Figure:
    """
    Create a scatter plot visualization.
    
    Args:
        data: DataFrame containing data to visualize
        x: Column for x-axis
        y: Column for y-axis
        color: Column for point colors
        size: Column for point sizes
        title: Chart title
        x_label: X-axis label
        y_label: Y-axis label
        height: Chart height in pixels
        color_discrete_map: Mapping of categories to colors
        trendline: Type of trendline to add ('ols', 'lowess')
        hover_data: Additional columns to show in hover tooltip
        
    Returns:
        Plotly figure object
    """
    charts_config = get_chart_config()
    
    # Use defaults from config if not specified
    if height is None:
        height = charts_config["chart_height"]
    
    # Create scatter plot
    fig = px.scatter(
        data,
        x=x,
        y=y,
        color=color,
        size=size,
        title=title,
        height=height,
        color_discrete_map=color_discrete_map,
        trendline=trendline,
        hover_data=hover_data,
    )
    
    # Update layout with labels
    fig.update_layout(
        xaxis_title=x_label if x_label else x,
        yaxis_title=y_label if y_label else y,
        showlegend=color is not None,
        plot_bgcolor="white",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Add grid based on config
    if charts_config["show_grid"]:
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="lightgray")
        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="lightgray")
    
    return fig

def create_stacked_area_chart(
    data: pd.DataFrame,
    x: str,
    y: str,
    color: str,
    title: str = "",
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    height: Optional[int] = None,
    color_discrete_map: Optional[Dict[str, str]] = None,
    normalized: bool = False,
) -> go.Figure:
    """
    Create a stacked area chart visualization.
    
    Args:
        data: DataFrame containing data to visualize
        x: Column for x-axis
        y: Column for y-axis values
        color: Column for stacking and coloring
        title: Chart title
        x_label: X-axis label
        y_label: Y-axis label
        height: Chart height in pixels
        color_discrete_map: Mapping of categories to colors
        normalized: Whether to normalize values (percentages)
        
    Returns:
        Plotly figure object
    """
    charts_config = get_chart_config()
    
    # Use defaults from config if not specified
    if height is None:
        height = charts_config["chart_height"]
    
    # Create area chart
    fig = px.area(
        data,
        x=x,
        y=y,
        color=color,
        title=title,
        height=height,
        color_discrete_map=color_discrete_map,
        groupnorm='percent' if normalized else None,
    )
    
    # Update layout with labels
    fig.update_layout(
        xaxis_title=x_label if x_label else x,
        yaxis_title=y_label if y_label else (f"{y} %" if normalized else y),
        showlegend=True,
        plot_bgcolor="white",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Add grid based on config
    if charts_config["show_grid"]:
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="lightgray")
    
    return fig

def create_box_plot(
    data: pd.DataFrame,
    x: Optional[str] = None,
    y: str = None,
    color: Optional[str] = None,
    title: str = "",
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    height: Optional[int] = None,
    color_discrete_map: Optional[Dict[str, str]] = None,
    points: str = "outliers",
) -> go.Figure:
    """
    Create a box plot visualization.
    
    Args:
        data: DataFrame containing data to visualize
        x: Column for x-axis categorical variable
        y: Column for y-axis numeric variable
        color: Column for color grouping
        title: Chart title
        x_label: X-axis label
        y_label: Y-axis label
        height: Chart height in pixels
        color_discrete_map: Mapping of categories to colors
        points: Which points to show ('all', 'outliers', 'suspectedoutliers', False)
        
    Returns:
        Plotly figure object
    """
    charts_config = get_chart_config()
    
    # Use defaults from config if not specified
    if height is None:
        height = charts_config["chart_height"]
    
    # Create box plot
    fig = px.box(
        data,
        x=x,
        y=y,
        color=color,
        title=title,
        height=height,
        color_discrete_map=color_discrete_map,
        points=points,
    )
    
    # Update layout with labels
    fig.update_layout(
        xaxis_title=x_label if x_label else (x if x else ""),
        yaxis_title=y_label if y_label else (y if y else ""),
        showlegend=color is not None,
        plot_bgcolor="white",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Add grid based on config
    if charts_config["show_grid"]:
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="lightgray")
    
    return fig

def create_histogram(
    data: pd.DataFrame,
    x: str,
    color: Optional[str] = None,
    title: str = "",
    x_label: Optional[str] = None,
    y_label: Optional[str] = "Count",
    height: Optional[int] = None,
    color_discrete_map: Optional[Dict[str, str]] = None,
    nbins: Optional[int] = None,
    histnorm: Optional[str] = None,
    barmode: str = "group",
) -> go.Figure:
    """
    Create a histogram visualization.
    
    Args:
        data: DataFrame containing data to visualize
        x: Column for x-axis 
        color: Column for color grouping
        title: Chart title
        x_label: X-axis label
        y_label: Y-axis label
        height: Chart height in pixels
        color_discrete_map: Mapping of categories to colors
        nbins: Number of bins
        histnorm: Histogram normalization ('percent', 'probability', 'density', 'probability density')
        barmode: Bar mode ('group', 'stack', 'overlay', 'relative')
        
    Returns:
        Plotly figure object
    """
    charts_config = get_chart_config()
    
    # Use defaults from config if not specified
    if height is None:
        height = charts_config["chart_height"]
    
    # Create histogram
    fig = px.histogram(
        data,
        x=x,
        color=color,
        title=title,
        height=height,
        color_discrete_map=color_discrete_map,
        nbins=nbins,
        histnorm=histnorm,
        barmode=barmode,
    )
    
    # Update layout with labels
    fig.update_layout(
        xaxis_title=x_label if x_label else x,
        yaxis_title=y_label,
        showlegend=color is not None,
        plot_bgcolor="white",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Add grid based on config
    if charts_config["show_grid"]:
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="lightgray")
    
    return fig

def create_sunburst_chart(
    data: pd.DataFrame,
    path: List[str],
    values: str,
    title: str = "",
    color: Optional[str] = None,
    height: Optional[int] = None,
    color_discrete_map: Optional[Dict[str, str]] = None,
) -> go.Figure:
    """
    Create a sunburst chart visualization for hierarchical data.
    
    Args:
        data: DataFrame containing data to visualize
        path: List of columns defining the hierarchical levels
        values: Column with values for size
        title: Chart title
        color: Column to determine segment colors
        height: Chart height in pixels
        color_discrete_map: Mapping of categories to colors
        
    Returns:
        Plotly figure object
    """
    charts_config = get_chart_config()
    
    # Use defaults from config if not specified
    if height is None:
        height = charts_config["chart_height"]
    
    # Create sunburst chart
    fig = px.sunburst(
        data,
        path=path,
        values=values,
        title=title,
        height=height,
        color=color or path[-1],
        color_discrete_map=color_discrete_map,
    )
    
    # Improve layout
    fig.update_layout(
        margin=dict(t=30, l=0, r=0, b=0),
    )
    
    return fig 