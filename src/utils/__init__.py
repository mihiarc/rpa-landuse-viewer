"""
RPA Land Use Viewer - Utilities Package

This package contains utility modules for the RPA Land Use Viewer application.
"""

# Import commonly used functions from shared module
from .shared import (
    get_scenarios,
    get_years,
    get_states,
    get_counties_by_state,
    get_land_use_types,
    get_national_summary,
    load_css,
    set_page_config,
    display_metrics,
    format_metric_value
)

# Import visualization functions
from .visualizations import (
    create_choropleth_map,
    create_bar_chart,
    create_pie_chart,
    create_line_chart,
    create_sankey_diagram,
    create_heatmap,
    create_scatter_plot,
    create_stacked_area_chart,
    create_box_plot,
    create_histogram,
    create_sunburst_chart
)

# Import data processing functions
from .data_processing import (
    filter_data,
    aggregate_data
)

# Import data analysis functions
from .data import (
    calculate_percentage,
    get_summary_statistics
)

# Define what should be imported with "from src.utils import *"
__all__ = [
    # Shared utilities
    'get_scenarios',
    'get_years',
    'get_states',
    'get_counties_by_state',
    'get_land_use_types',
    'get_national_summary',
    'load_css',
    'set_page_config',
    'display_metrics',
    'format_metric_value',
    
    # Visualizations
    'create_choropleth_map',
    'create_bar_chart',
    'create_pie_chart',
    'create_line_chart',
    'create_sankey_diagram',
    'create_heatmap',
    'create_scatter_plot',
    'create_stacked_area_chart',
    'create_box_plot',
    'create_histogram',
    'create_sunburst_chart',
    
    # Data processing
    'filter_data',
    'aggregate_data',
    
    # Data analysis
    'calculate_percentage',
    'get_summary_statistics'
] 