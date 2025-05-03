# Core Query Functions for RPA Land Use Change Analysis

This document outlines a set of interesting and insightful queries to test the core functionality of the RPA land use change application. These queries will be implemented as functions and validated using integration tests.

**I. Overall Change and Trends:**

1.  **Total Net Change by Land Use Type:** "What is the total net change (gain or loss in acres) for each land use type (cropland, pasture, rangeland, forest, urban) across all counties over the entire projection period (2012-2070)?" (High-level overview of national trends.)

2.  **Annualized Change Rate:** "What is the average annual rate of change (in acres per year) for each land use type during each projection period (2020-2030, 2030-2040, etc.)?" (Helps understand the pace of change.)

3.  **Peak Change Time Period:** "During which 10-year period is the largest net change observed for each land use type?" (Identifies periods of rapid change.)

**II. Geographic Analysis:**

4.  **Change by State:** "What is the net change for each land use type within each state over the entire projection period?" (Provides state-level insights.)

5.  **Top N Counties by Change:** "Which are the top 10 counties with the largest increase in urban land use by 2070?" (Identifies areas of significant urbanization.)

6.  **Change by Geographic Region:** "What is the net change in forest land in the Western US (or other defined regions) under the rcp85_ssp3 scenario?" (Allows for regional analysis based on custom groupings of counties/states.)

7.  **Spatial Distribution of Change:** "Show the spatial distribution of forest loss across the CONUS in 2050 under the rcp45_ssp1 scenario." (Involves visualization, but the underlying query is essential.)

**III. Scenario Comparison:**

8.  **Impact of Climate Models:** "How does the projected change in cropland differ between the 'wet' (CNRM_CM5) and 'dry' (IPSL_CM5A_MR) climate models under the same emissions scenario (e.g., rcp85_ssp2) by 2060?" (Assesses the influence of different climate projections.)

9.  **Impact of Emissions Scenarios:** "How does the projected increase in urban land use differ between the low emissions (rcp45_ssp1) and high emissions (rcp85_ssp5) scenarios under the same climate model (e.g., HadGEM2_ES365) by 2040?" (Examines the effects of different policy choices.)

10. **Scenario Ranking:** "Rank the emissions scenarios based on their projected total forest loss by 2070 under the 'middle' (NorESM1_M) climate model." (Provides a comparative overview of scenario impacts.)

**IV. Land Use Transition Analysis:**

11. **Major Transitions:** "What are the top 3 most common land use transitions (e.g., forest to urban, rangeland to cropland) observed across the CONUS between 2020 and 2050?" (Identifies dominant land use change patterns.)

12. **Transition by Location:** "What are the primary land use transitions occurring in California between 2030 and 2040 under the rcp85_ssp3 scenario?" (Focuses on specific geographic areas and scenarios.)

13. **Transition Rate Changes:** "Has the rate of conversion from pasture to cropland increased or decreased between the 2020-2030 and 2050-2060 periods across all counties?" (Analyzes changes in transition dynamics over time.)

**V. Data Integrity Checks (Important for Testing):**

14. **Total Area Consistency:** "For each county and time period, does the sum of all land use areas equal the total area at the start of the time period (t1)?" (Checks for data consistency and errors in the transformations).

15. **No Negative Acres:** "Are there any instances of negative acre values in the data?" (Ensures data validity.)

16. **Unique Identifiers:** "Are all scenario names, FIPS codes, and time step IDs unique in their respective tables?" (Verifies data integrity and proper database design.)

**Implementation Notes:**

*   These queries will likely involve SQL queries against your SQLite database.
*   You'll need to implement functions in your `src.db.queries` module to execute these queries.
*   Your integration tests will call these functions and compare the results to expected values.