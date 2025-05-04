import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json
import os
from pathlib import Path

# Set page configuration
st.set_page_config(
    page_title="RPA Land Use Viewer",
    page_icon="🌳",
    layout="wide"
)

# Title and description
st.title("Resources Planning Act (RPA) Land Use Viewer")
st.markdown("""
This application visualizes land use transition projections from the USDA Forest Service's Resources Planning Act (RPA) Assessment.
Explore how land use is expected to change across the United States from 2020 to 2070 under different climate and socioeconomic scenarios.
""")

# Load the parquet files
@st.cache_data
def load_parquet_data():
    data = {
        "reference": pd.read_parquet("semantic_layers/base_analysis/reference.parquet"),
        "transitions_summary": pd.read_parquet("semantic_layers/base_analysis/transitions_summary.parquet"),
        "transitions_changes_only": pd.read_parquet("semantic_layers/base_analysis/transitions_changes_only.parquet"),
        "urbanization_trends": pd.read_parquet("semantic_layers/base_analysis/urbanization_trends.parquet"),
        "to_urban_transitions": pd.read_parquet("semantic_layers/base_analysis/to_urban_transitions.parquet"),
        "from_forest_transitions": pd.read_parquet("semantic_layers/base_analysis/from_forest_transitions.parquet"),
        "county_transitions": pd.read_parquet("semantic_layers/base_analysis/county_transitions.parquet")
    }
    
    # Extract specific reference dataframes from consolidated reference file
    reference_df = data["reference"]
    data["scenarios"] = reference_df[reference_df["info_type"] == "Scenario"]
    data["landuse_types"] = reference_df[reference_df["info_type"] == "Land Use Type"]
    data["decades"] = reference_df[reference_df["info_type"] == "Time Period"]
    
    return data

# Load RPA documentation
@st.cache_data
def load_rpa_docs():
    try:
        with open("docs/rpa_text/gtr_wo102_Chap4_chunks.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        st.warning("RPA documentation file not found")
        return []

# Main layout with tabs
tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Urbanization Trends", "Forest Transitions", "Data Explorer"])

# Load data
try:
    data = load_parquet_data()
    rpa_docs = load_rpa_docs()
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# ---- OVERVIEW TAB ----
with tab1:
    st.header("Land Use Projections Overview")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("RPA Assessment Scenarios")
        # Convert object columns to string to avoid PyArrow conversion issues
        scenarios_df = data["scenarios"].copy()
        for col in scenarios_df.select_dtypes(include=['object']).columns:
            scenarios_df[col] = scenarios_df[col].astype(str)
        st.dataframe(scenarios_df)
        
        st.subheader("Land Use Categories")
        # Convert object columns to string to avoid PyArrow conversion issues
        landuse_df = data["landuse_types"].copy()
        for col in landuse_df.select_dtypes(include=['object']).columns:
            landuse_df[col] = landuse_df[col].astype(str)
        st.dataframe(landuse_df)
    
    with col2:
        st.subheader("Key Findings from RPA Assessment")
        st.markdown("""
        ### Key Findings
        - Developed land area is projected to increase in the future, while all non-developed land uses are projected to lose area. The most common source of new developed land is forest land.
        - Forest land area is projected to decrease under all scenarios, although at lower rates than projected by the 2010 Assessment.
        - Higher projected population and income growth lead to relatively less forest land, while hotter projected future climates lead to relatively more forest land.
        - Projected future land use change is more sensitive to the variation in economic factors across RPA scenarios than to the variation among climate projections.
        """)
        
        st.subheader("Time Periods")
        # Convert object columns to string to avoid PyArrow conversion issues
        decades_df = data["decades"].copy()
        for col in decades_df.select_dtypes(include=['object']).columns:
            decades_df[col] = decades_df[col].astype(str)
        st.dataframe(decades_df)

# ---- URBANIZATION TRENDS TAB ----
with tab2:
    st.header("Urbanization Trends")
    
    # Filter controls
    st.subheader("Explore Urbanization Trends")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        urbanization_df = data["urbanization_trends"]
        # Convert to string for display in selectbox
        scenarios = urbanization_df["scenario_name"].unique().tolist()
        scenarios = [str(s) for s in scenarios]
        selected_scenario = st.selectbox("Select Scenario", options=scenarios)
    
    # Filter data based on selection
    filtered_urban = urbanization_df[urbanization_df["scenario_name"] == selected_scenario]
    
    # Plot the data
    st.subheader(f"Land Conversion to Urban Areas: {selected_scenario}")
    
    fig, ax = plt.figure(figsize=(10, 6)), plt.subplot()
    ax.plot(filtered_urban["decade_name"], filtered_urban["forest_to_urban"], marker='o', label="Forest to Urban")
    ax.plot(filtered_urban["decade_name"], filtered_urban["cropland_to_urban"], marker='s', label="Cropland to Urban")
    ax.plot(filtered_urban["decade_name"], filtered_urban["pasture_to_urban"], marker='^', label="Pasture to Urban")
    ax.set_xlabel("Time Period")
    ax.set_ylabel("Area (hundred acres)")
    ax.set_title(f"Land Conversion to Urban Areas: {selected_scenario}")
    ax.legend()
    plt.tight_layout()
    
    st.pyplot(fig)
    
    with st.expander("Show Data Table"):
        # Convert object columns to string to avoid PyArrow conversion issues
        display_df = filtered_urban.copy()
        for col in display_df.select_dtypes(include=['object']).columns:
            display_df[col] = display_df[col].astype(str)
        st.dataframe(display_df)
    
    st.subheader("Top Counties Converting to Urban Land")
    
    # Get county transitions data
    county_df = data["county_transitions"]
    # Filter for urban transitions only (where to_category is 'Urban')
    urban_counties_df = county_df[county_df["to_category"] == "Urban"]
    
    # Group by county and sum total area
    urban_by_county = urban_counties_df.groupby(["county_name", "state_name"])["total_area"].sum().reset_index()
    urban_by_county = urban_by_county.sort_values("total_area", ascending=False).head(10)
    
    fig2, ax2 = plt.figure(figsize=(10, 6)), plt.subplot()
    ax2.bar(urban_by_county["county_name"] + ", " + urban_by_county["state_name"], urban_by_county["total_area"])
    ax2.set_xlabel("County")
    ax2.set_ylabel("Area (hundred acres)")
    ax2.set_title("Top 10 Counties by Urbanization")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    
    st.pyplot(fig2)

# ---- FOREST TRANSITIONS TAB ----
with tab3:
    st.header("Forest Land Transitions")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        from_forest_df = data["from_forest_transitions"]
        # Convert to string for display in selectbox
        forest_scenarios = from_forest_df["scenario_name"].unique().tolist()
        forest_scenarios = [str(s) for s in forest_scenarios]
        selected_scenario_forest = st.selectbox("Select Scenario", 
                                               options=forest_scenarios,
                                               key="forest_scenario")
    
    # Filter data
    filtered_forest = from_forest_df[from_forest_df["scenario_name"] == selected_scenario_forest]
    
    # Aggregate data by destination land use
    forest_to_use = filtered_forest.groupby(["to_category", "decade_name"])["total_area"].sum().reset_index()
    
    # Pivot table for plotting
    pivot_forest = forest_to_use.pivot(index="decade_name", columns="to_category", values="total_area")
    
    # Plot the data
    st.subheader(f"Forest Land Conversion: {selected_scenario_forest}")
    
    fig3, ax3 = plt.figure(figsize=(10, 6)), plt.subplot()
    pivot_forest.plot(kind="bar", ax=ax3)
    ax3.set_xlabel("Time Period")
    ax3.set_ylabel("Area (hundred acres)")
    ax3.set_title(f"Forest Land Conversion by Destination: {selected_scenario_forest}")
    plt.tight_layout()
    
    st.pyplot(fig3)
    
    with st.expander("Show Data Table"):
        # Convert object columns to string to avoid PyArrow conversion issues
        display_forest_df = filtered_forest.copy()
        for col in display_forest_df.select_dtypes(include=['object']).columns:
            display_forest_df[col] = display_forest_df[col].astype(str)
        st.dataframe(display_forest_df)
    
    # Add additional information from RPA docs
    st.subheader("Forest Land Projections from RPA Assessment")
    rpa_forest_info = """
    Over the 50-year period from 2020 to 2070, the RPA Assessment projects a total net forest land loss of between 7.6 and 15.0 million acres. 
    When averaging results across RPA scenario-climate futures, approximately 91 percent of current forest land is projected to remain in forest use by 2070.
    
    Most of the gross forest loss (19.8 to 26.0 million acres) is projected to convert to developed land, which is assumed to be a permanent change.
    About 25.3 million acres of new forest land will be added from conversions out of pasture land (17.4 million), crop land (2.4 million acres), and rangeland (5.5 million acres).
    """
    st.markdown(rpa_forest_info)
    
    # Add a section for county-level forest loss
    st.subheader("Top Counties with Forest Land Loss")
    
    # Get county transitions with forest as source
    county_df = data["county_transitions"]
    forest_loss_counties = county_df[(county_df["from_category"] == "Forest") & (county_df["to_category"] != "Forest")]
    
    # Group by county and sum total area
    forest_loss_by_county = forest_loss_counties.groupby(["county_name", "state_name"])["total_area"].sum().reset_index()
    forest_loss_by_county = forest_loss_by_county.sort_values("total_area", ascending=False).head(10)
    
    fig4, ax4 = plt.figure(figsize=(10, 6)), plt.subplot()
    ax4.bar(forest_loss_by_county["county_name"] + ", " + forest_loss_by_county["state_name"], forest_loss_by_county["total_area"])
    ax4.set_xlabel("County")
    ax4.set_ylabel("Area (hundred acres)")
    ax4.set_title("Top 10 Counties by Forest Land Loss")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    
    st.pyplot(fig4)

# ---- DATA EXPLORER TAB ----
with tab4:
    st.header("Data Explorer")
    
    # Select dataset to explore
    dataset_options = list(data.keys())
    selected_dataset = st.selectbox("Select Dataset", options=dataset_options)
    
    # Show dataset
    st.subheader(f"Exploring: {selected_dataset}")
    selected_df = data[selected_dataset]
    
    # Show basic stats
    col1, col2 = st.columns([1, 1])
    with col1:
        st.metric("Number of Rows", selected_df.shape[0])
    with col2:
        st.metric("Number of Columns", selected_df.shape[1])
    
    # Show column info
    st.subheader("Column Information")
    # Convert object types to string to avoid PyArrow conversion issues
    col_df = pd.DataFrame({
        "Column": selected_df.columns,
        "Type": [str(dtype) for dtype in selected_df.dtypes],
        "Sample Values": [str(selected_df[col].iloc[0]) if len(selected_df) > 0 else "Empty" for col in selected_df.columns]
    })
    st.dataframe(col_df)
    
    # Show data
    st.subheader("Data Preview")
    # Convert object columns to string to avoid PyArrow conversion issues
    preview_df = selected_df.head(100).copy()
    for col in preview_df.select_dtypes(include=['object']).columns:
        preview_df[col] = preview_df[col].astype(str)
    st.dataframe(preview_df)
    
    # Allow download
    csv = selected_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name=f'{selected_dataset}.csv',
        mime='text/csv',
    )

# Footer
st.markdown("---")
st.markdown("""
**RPA Land Use Viewer** - Built with Streamlit

Data Source: This dataset was developed by Mihiar, Lewis & Coulston for the USDA Forest Service for the Resources Planning Act (RPA) 2020 Assessment.
""") 