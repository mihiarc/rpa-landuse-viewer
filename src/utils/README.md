# RPA Land Use Viewer - Shared Utilities

This directory contains shared utility modules used across the RPA Land Use Viewer application.

## Overview

The shared utilities are organized into the following modules:

- `shared.py`: Common functions for data loading and UI elements
- `visualizations.py`: Comprehensive visualization functions for creating charts and maps
- `data_processing.py`: Functions for data filtering and transformation
- `data.py`: Data analysis and statistics functions

## Usage Examples

### Loading Data

```python
from src.utils.shared import get_scenarios, get_years, get_states

# Get list of available scenarios
scenarios = get_scenarios()

# Get available years 
years = get_years()

# Get available states
states = get_states()
```

### Working with UI Components

```python
from src.utils.shared import load_css, set_page_config, display_metrics

# Set up page configuration
set_page_config(title="My Page", icon="📊")

# Load custom CSS
load_css()

# Display metrics
metrics = {
    "total_acres": 125000,
    "developed_acres": 45000,
    "percent_developed": 36.0,
    "population": 2500000
}
display_metrics(metrics)
```

### Creating Visualizations

The `visualizations.py` module provides a comprehensive set of functions for creating various chart types. All visualization functions follow a consistent naming pattern (`create_X`) and are properly cached for performance.

#### Basic Visualizations

```python
from src.utils import create_choropleth_map, create_bar_chart, create_pie_chart, create_line_chart

# Create a choropleth map
fig_map = create_choropleth_map(
    df=state_data,
    geo_col='state',
    value_col='developed_acres',
    title="Developed Acres by State"
)
st.plotly_chart(fig_map, use_container_width=True)

# Create a bar chart
fig_bar = create_bar_chart(
    df=county_data,
    x_col='county',
    y_col='total_acres',
    title="Total Land Area by County"
)
st.plotly_chart(fig_bar, use_container_width=True)

# Create a pie chart for land use distribution
land_use_cols = [col for col in df.columns if col.endswith('_acres') and col != 'total_acres']
pie_data = pd.DataFrame({
    'land_use_type': [col.replace('_acres', '').replace('_', ' ').title() for col in land_use_cols],
    'acres': [df[col].sum() for col in land_use_cols]
})
fig_pie = create_pie_chart(
    df=pie_data,
    values_col='acres',
    names_col='land_use_type',
    title="Land Use Distribution"
)
st.plotly_chart(fig_pie, use_container_width=True)

# Create a line chart for temporal trends
fig_line = create_line_chart(
    df=time_series_data,
    x_col='year',
    y_cols=['developed_acres', 'forest_acres', 'agricultural_acres'],
    title="Land Use Trends Over Time"
)
st.plotly_chart(fig_line, use_container_width=True)
```

#### Advanced Visualizations

```python
from src.utils import (
    create_sankey_diagram, create_heatmap, create_scatter_plot,
    create_stacked_area_chart, create_box_plot, create_histogram,
    create_sunburst_chart
)

# Create a Sankey diagram for land use transitions
fig_sankey = create_sankey_diagram(
    df=transition_data,
    source_col='source_land_use',
    target_col='target_land_use',
    value_col='acres_converted',
    title="Land Use Transitions"
)
st.plotly_chart(fig_sankey, use_container_width=True)

# Create a heatmap for correlation analysis
fig_heatmap = create_heatmap(
    df=matrix_data,
    x_col='year',
    y_col='land_use_type',
    z_col='acres',
    title="Land Use Change Matrix"
)
st.plotly_chart(fig_heatmap, use_container_width=True)

# Create a scatter plot with trend line
fig_scatter = create_scatter_plot(
    df=correlation_data,
    x_col='population',
    y_col='developed_acres',
    color_col='region',
    trendline='ols',
    title="Population vs. Development"
)
st.plotly_chart(fig_scatter, use_container_width=True)

# Create a stacked area chart
fig_area = create_stacked_area_chart(
    df=time_series_data,
    x_col='year',
    y_col='acres',
    color_col='land_use_type',
    normalized=True,
    title="Land Use Composition Over Time"
)
st.plotly_chart(fig_area, use_container_width=True)

# Create a histogram for distribution analysis
fig_hist = create_histogram(
    df=distribution_data,
    x_col='parcel_size_acres',
    nbins=20,
    title="Parcel Size Distribution"
)
st.plotly_chart(fig_hist, use_container_width=True)

# Create a box plot for comparative analysis
fig_box = create_box_plot(
    df=comparative_data,
    x_col='region',
    y_col='developed_acres',
    title="Development by Region"
)
st.plotly_chart(fig_box, use_container_width=True)

# Create a sunburst chart for hierarchical data
fig_sunburst = create_sunburst_chart(
    df=hierarchical_data,
    path_cols=['region', 'state', 'county'],
    values_col='acres',
    title="Land Area Hierarchy"
)
st.plotly_chart(fig_sunburst, use_container_width=True)
```

## Best Practices

1. **Use Cache Decorators**: All data loading and visualization functions use `@st.cache_data` with appropriate `ttl` values to optimize performance.

2. **Consistent Naming**: Use the established naming conventions for functions and parameters. All visualization functions follow the `create_X` pattern.

3. **Type Annotations**: Always include type annotations for function parameters and return values.

4. **Documentation**: Maintain detailed docstrings for all functions.

5. **Error Handling**: Add appropriate error handling for database connections and data processing.

6. **Parameter Defaults**: Provide sensible defaults for optional parameters.

## Extending the Modules

When adding new utility functions:

1. Place the function in the most appropriate module based on its purpose
2. Add proper type annotations and comprehensive docstrings
3. Use `@st.cache_data` with an appropriate `ttl` for data loading or processing functions
4. Maintain consistency with existing coding style and naming conventions
5. Update this README with examples if adding significant new functionality 