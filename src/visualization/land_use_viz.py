"""Visualization module for land use change analysis."""

import plotly.graph_objects as go
import plotly.express as px
import folium
from folium import plugins
import pandas as pd
from typing import List, Dict, Optional
from ..api.queries import LandUseQueries

class LandUseVisualization:
    """Class for creating visualizations of land use change data."""
    
    @staticmethod
    def create_county_choropleth(
        start_year: int,
        end_year: int,
        land_use_type: str,
        scenario_name: str,
        title: Optional[str] = None
    ) -> folium.Map:
        """
        Create a county-level choropleth map showing net changes in specified land use type.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            land_use_type: Type of land use to analyze
            scenario_name: Scenario to analyze
            title: Optional title for the map
            
        Returns:
            Folium map object
        """
        # Get county-level changes
        counties = LandUseQueries.top_counties_by_change(
            start_year, end_year, land_use_type, scenario_name, 
            limit=3000,  # Large limit to get all counties
            change_type='increase'  # Will be sorted later
        )
        
        # Convert to DataFrame
        df = pd.DataFrame(counties)
        
        # Create base map centered on US
        m = folium.Map(
            location=[39.8283, -98.5795],
            zoom_start=4,
            tiles='cartodbpositron'
        )
        
        # Add choropleth layer
        folium.Choropleth(
            geo_data='data/counties.geojson',  # You'll need to provide this
            name='choropleth',
            data=df,
            columns=['fips_code', 'net_change'],
            key_on='feature.properties.GEOID',
            fill_color='RdYlBu',
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name=f'Net Change in {land_use_type} Land (acres)',
            highlight=True
        ).add_to(m)
        
        # Add hover functionality
        folium.LayerControl().add_to(m)
        
        return m

    @staticmethod
    def create_time_series(
        start_year: int,
        end_year: int,
        scenario_name: str,
        land_use_types: Optional[List[str]] = None
    ) -> go.Figure:
        """
        Create an interactive time series plot showing changes in land use over time.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            scenario_name: Scenario to analyze
            land_use_types: Optional list of land use types to include
            
        Returns:
            Plotly figure object
        """
        if not land_use_types:
            land_use_types = ['Urban', 'Crop', 'Pasture', 'Forest', 'Range']
        
        # Get data for each time period
        time_periods = [(2012, 2030), (2030, 2050), (2050, 2070)]
        data = []
        
        for period_start, period_end in time_periods:
            if period_start >= start_year and period_end <= end_year:
                results = LandUseQueries.total_net_change(
                    period_start, period_end, scenario_name
                )
                for r in results:
                    if r['land_use'] in land_use_types:
                        data.append({
                            'year': period_end,
                            'land_use': r['land_use'],
                            'net_change': r['net_change']
                        })
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Create figure
        fig = go.Figure()
        
        for land_use in land_use_types:
            land_use_data = df[df['land_use'] == land_use]
            fig.add_trace(go.Scatter(
                x=land_use_data['year'],
                y=land_use_data['net_change'],
                name=land_use,
                mode='lines+markers'
            ))
        
        # Update layout
        fig.update_layout(
            title=f'Land Use Changes Over Time ({start_year}-{end_year})',
            xaxis_title='Year',
            yaxis_title='Net Change (acres)',
            hovermode='x unified',
            showlegend=True
        )
        
        return fig

    @staticmethod
    def create_sankey_diagram(
        start_year: int,
        end_year: int,
        scenario_name: str
    ) -> go.Figure:
        """
        Create a Sankey diagram showing land use transitions.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            scenario_name: Scenario to analyze
            
        Returns:
            Plotly figure object
        """
        # Get major transitions
        transitions = LandUseQueries.major_transitions(
            start_year, end_year, scenario_name, limit=20
        )
        
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
        
        # Create figure
        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=land_uses,
                color="blue"
            ),
            link=dict(
                source=sources,
                target=targets,
                value=values
            )
        )])
        
        # Update layout
        fig.update_layout(
            title_text=f"Land Use Transitions ({start_year}-{end_year})",
            font_size=10
        )
        
        return fig

    @staticmethod
    def create_scenario_comparison(
        start_year: int,
        end_year: int,
        land_use_type: str,
        scenario1: str,
        scenario2: str
    ) -> go.Figure:
        """
        Create a comparison visualization between two scenarios.
        
        Args:
            start_year: Starting year for analysis
            end_year: Ending year for analysis
            land_use_type: Type of land use to analyze
            scenario1: First scenario name
            scenario2: Second scenario name
            
        Returns:
            Plotly figure object
        """
        # Get comparison data
        results = LandUseQueries.compare_scenarios(
            start_year, end_year, land_use_type, scenario1, scenario2
        )
        
        # Create figure
        fig = go.Figure()
        
        # Add bars for net change
        fig.add_trace(go.Bar(
            name='Net Change',
            x=[r['scenario_name'] for r in results],
            y=[r['net_change'] for r in results],
            text=[f"{r['net_change']:,.0f} acres" for r in results],
            textposition='auto',
        ))
        
        # Add markers for annual rate
        fig.add_trace(go.Scatter(
            name='Annual Rate',
            x=[r['scenario_name'] for r in results],
            y=[r['annual_rate'] for r in results],
            mode='markers',
            marker=dict(size=15, symbol='diamond'),
            yaxis='y2'
        ))
        
        # Update layout
        fig.update_layout(
            title=f'{land_use_type} Land Change Comparison ({start_year}-{end_year})',
            yaxis=dict(
                title='Net Change (acres)',
                titlefont=dict(color="#1f77b4"),
                tickfont=dict(color="#1f77b4")
            ),
            yaxis2=dict(
                title='Annual Rate (acres/year)',
                titlefont=dict(color="#ff7f0e"),
                tickfont=dict(color="#ff7f0e"),
                anchor="x",
                overlaying="y",
                side="right"
            ),
            showlegend=True
        )
        
        return fig 

    @staticmethod
    def create_scenario_ranking_plot(
        target_year: int,
        climate_model: str = 'NorESM1_M'
    ) -> go.Figure:
        """
        Create a visualization of scenario rankings based on forest loss.
        
        Args:
            target_year: Target year for analysis
            climate_model: Climate model to analyze
            
        Returns:
            Plotly figure object
        """
        # Get scenario rankings
        rankings = LandUseQueries.rank_scenarios_by_forest_loss(target_year, climate_model)
        
        # Create figure
        fig = go.Figure()
        
        # Add bars for net forest loss
        fig.add_trace(go.Bar(
            name='Net Forest Loss',
            x=[f"{r['scenario_name']}<br>({r['emissions_forcing']}, {r['socioeconomic_pathway']})" 
               for r in rankings],
            y=[r['net_forest_loss'] for r in rankings],
            text=[f"Rank {r['loss_rank']}" for r in rankings],
            textposition='auto',
            marker_color=['#ff9999' if r['loss_rank'] <= 3 else '#99ff99' 
                         for r in rankings]
        ))
        
        # Add markers for forest gain
        fig.add_trace(go.Scatter(
            name='Forest Gain',
            x=[f"{r['scenario_name']}<br>({r['emissions_forcing']}, {r['socioeconomic_pathway']})" 
               for r in rankings],
            y=[r['forest_gain'] for r in rankings],
            mode='markers',
            marker=dict(
                size=12,
                symbol='diamond',
                color='green'
            )
        ))
        
        # Update layout
        fig.update_layout(
            title=f'Scenario Rankings by Forest Loss (through {target_year})<br>Climate Model: {climate_model}',
            xaxis_title='Scenario',
            yaxis_title='Acres',
            barmode='relative',
            showlegend=True,
            height=600,
            xaxis=dict(tickangle=45)
        )
        
        return fig 