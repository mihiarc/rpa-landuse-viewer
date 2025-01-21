import pytest
import pandas as pd
import plotly.graph_objects as go
from src.api.queries import LandUseQueries
from src.visualization.land_use_viz import LandUseVisualization

class TestLandUseAnalysis:
    """Test suite for land use analysis functionality."""
    
    @pytest.fixture
    def sample_scenario(self):
        """Provide a sample scenario for testing."""
        return {
            'name': 'rcp85_ssp3',  # High emissions, low growth
            'climate_model': 'NorESM1_M',  # Middle model
            'start_year': 2020,
            'end_year': 2050
        }
    
    def test_scenario_ranking(self, sample_scenario):
        """Test scenario ranking by forest loss."""
        # Get rankings
        rankings = LandUseQueries.rank_scenarios_by_forest_loss(
            target_year=sample_scenario['end_year'],
            climate_model=sample_scenario['climate_model']
        )
        
        # Basic validation
        assert isinstance(rankings, list), "Rankings should be a list"
        assert len(rankings) > 0, "Should return at least one scenario"
        
        # Validate ranking structure
        first_rank = rankings[0]
        required_fields = {
            'scenario_name', 'emissions_forcing', 'socioeconomic_pathway',
            'forest_loss', 'forest_gain', 'net_forest_loss', 'loss_rank'
        }
        assert all(field in first_rank for field in required_fields), \
            "Ranking should contain all required fields"
        
        # Validate ranking order
        assert all(rankings[i]['loss_rank'] <= rankings[i+1]['loss_rank'] 
                  for i in range(len(rankings)-1)), \
            "Rankings should be in ascending order"
    
    def test_scenario_ranking_visualization(self, sample_scenario):
        """Test creation of scenario ranking visualization."""
        # Create visualization
        fig = LandUseVisualization.create_scenario_ranking_plot(
            target_year=sample_scenario['end_year'],
            climate_model=sample_scenario['climate_model']
        )
        
        # Validate figure
        assert isinstance(fig, go.Figure), "Should return a Plotly figure"
        assert len(fig.data) == 2, "Should have two traces (bar and scatter)"
        assert fig.data[0].type == 'bar', "First trace should be a bar chart"
        assert fig.data[1].type == 'scatter', "Second trace should be a scatter plot"
    
    def test_time_series_analysis(self, sample_scenario):
        """Test time series analysis functionality."""
        # Get time series data
        results = LandUseQueries.total_net_change(
            start_year=sample_scenario['start_year'],
            end_year=sample_scenario['end_year'],
            scenario_name=sample_scenario['name']
        )
        
        # Basic validation
        assert isinstance(results, list), "Results should be a list"
        assert len(results) > 0, "Should return at least one land use type"
        
        # Create visualization
        fig = LandUseVisualization.create_time_series(
            start_year=sample_scenario['start_year'],
            end_year=sample_scenario['end_year'],
            scenario_name=sample_scenario['name']
        )
        
        # Validate visualization
        assert isinstance(fig, go.Figure), "Should return a Plotly figure"
        assert len(fig.data) > 0, "Should have at least one trace"
    
    def test_sankey_diagram(self, sample_scenario):
        """Test Sankey diagram creation."""
        # Create Sankey diagram
        fig = LandUseVisualization.create_sankey_diagram(
            start_year=sample_scenario['start_year'],
            end_year=sample_scenario['end_year'],
            scenario_name=sample_scenario['name']
        )
        
        # Validate diagram
        assert isinstance(fig, go.Figure), "Should return a Plotly figure"
        assert len(fig.data) == 1, "Should have one Sankey trace"
        assert fig.data[0].type == 'sankey', "Trace should be a Sankey diagram"
    
    def test_data_integrity(self, sample_scenario):
        """Test data integrity checks."""
        # Run integrity checks
        results = LandUseQueries.check_data_integrity(
            scenario_name=sample_scenario['name']
        )
        
        # Validate results
        assert isinstance(results, dict), "Should return a dictionary"
        assert 'area_inconsistencies' in results, "Should check area consistency"
        assert 'negative_acres' in results, "Should check for negative acres"
        
        # Validate data quality
        assert len(results['negative_acres']) == 0, \
            "Should not have negative acre values"
    
    def test_regional_analysis(self, sample_scenario):
        """Test regional analysis functionality."""
        # Get regional changes
        results = LandUseQueries.change_by_region(
            start_year=sample_scenario['start_year'],
            end_year=sample_scenario['end_year'],
            scenario_name=sample_scenario['name'],
            land_use_type='Forest'
        )
        
        # Basic validation
        assert isinstance(results, list), "Results should be a list"
        assert len(results) > 0, "Should return at least one region"
        
        # Validate regional structure
        first_region = results[0]
        assert 'region' in first_region, "Should include region name"
        assert 'net_change' in first_region, "Should include net change"
        
        # Validate sorting
        assert all(abs(results[i]['net_change']) >= abs(results[i+1]['net_change']) 
                  for i in range(len(results)-1)), \
            "Results should be sorted by absolute net change"

if __name__ == '__main__':
    pytest.main([__file__, '-v']) 