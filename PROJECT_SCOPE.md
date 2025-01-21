# RPA Landuse App - Project Scope

---

## **IMPORTANT: PROJECT CONTINUITY**  

```
Read CHANGELOG.md and PROJECT_SCOPE.md now, report your findings, and strictly follow all instructions found in these documents.
```

---

## **IMPORTANT: SELF-MAINTENANCE INSTRUCTIONS**  

### **Before Taking Any Action or Making Suggestions**  
1. **Read Both Files**:  
   - Read `CHANGELOG.md` and `PROJECT_SCOPE.md`.  
   - Immediately report:  
     ```
     Initializing new conversation...  
     Read [filename]: [key points relevant to current task]  
     Starting conversation history tracking...
     ```

2. **Review Context**:  
   - Assess existing features, known issues, and architectural decisions.  

3. **Inform Responses**:  
   - Use the gathered context to guide your suggestions or actions.  

4. **Proceed Only After Context Review**:  
   - Ensure all actions align with the project’s scope and continuity requirements.

---

### **After Making ANY Code Changes**  
1. **Update Documentation Immediately**:  
   - Add new features/changes to the `[Unreleased]` section of `CHANGELOG.md`.  
   - Update `PROJECT_SCOPE.md` if there are changes to architecture, features, or limitations.

2. **Report Documentation Updates**:  
   - Use the following format to report updates:  
     ```
     Updated CHANGELOG.md: [details of what changed]  
     Updated PROJECT_SCOPE.md: [details of what changed] (if applicable)
     ```

3. **Ensure Alignment**:  
   - Verify that all changes align with existing architecture and features.

4. **Document All Changes**:  
   - Include specific details about:
     - New features or improvements
     - Bug fixes
     - Error handling changes
     - UI/UX updates
     - Technical implementation details

5. **Adhere to the Read-First/Write-After Approach**:  
   - Maintain explicit update reporting for consistency and continuity.

---

## **Project Overview**
[Provide a brief overview of the project here.]

---

## **Core Objectives**
1. Create a dashboard for exploring the RPA Land-use Change Projections Dataset.

---

## **Technical Architecture**

### **Integrations**
1. **Database Systems**
   - MySQL: Primary data storage for land use transitions
   - Redis: Caching layer for API responses

2. **External Services**
   - Docker: Container management for MySQL database

### **Core Functions**
1. **Data Processing**
   - JSON to Parquet conversion
   - Data loading into MySQL
   - County-level land use transition analysis

2. **API Services**
   - FastAPI-based REST endpoints
   - Cached responses via Redis
   - Interactive API documentation

### **UI Features**
1. **Documentation Interfaces**
   - Swagger UI (/docs)
   - ReDoc alternative documentation (/redoc)
   - Interactive API testing

2. **Data Visualization**
   - County-level projections display
   - Scenario comparison tools
   - Time series analysis views

### **User Features**
1. **Data Access**
   - Query land use transitions by county
   - Filter by scenarios and time periods
   - Compare different climate models
   - Analyze land use changes over time

2. **Analysis Tools**
   - Scenario comparison
   - Time series analysis
   - Geographic filtering
   - Data export capabilities

---

## **Data Structures**
[List data structures here.]

## **Getting Started**

### **1. Initial Setup**

1. **Clone the Repository**
   ```bash
   git clone <repository_url>
   cd rpa_landuse_app
   ```

2. **Create Python Environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Linux/Mac
   # or
   .venv\Scripts\activate  # On Windows
   ```

3. **Install Dependencies**
   ```bash
   pip install -e .
   pip install tqdm plotly folium
   ```

### **2. Database Setup**

1. **Initialize MySQL Container**
   ```bash
   # Build the MySQL image
   cd data/database
   docker build -t rpa-mysql -f Dockerfile.dockerfile .
   
   # Start the container
   docker run -d \
     --name rpa-mysql-container \
     -p 3306:3306 \
     rpa-mysql
   
   # Verify container is running
   docker ps | grep rpa-mysql
   ```

2. **Load Data**
   ```bash
   # Load data from parquet to MySQL
   cd data/scripts
   python load_to_mysql.py
   ```

3. **Verify Data Loading**
   ```bash
   # Check record counts
   docker exec -it rpa-mysql-container mysql -u mihiarc -psurvista683 rpa_mysql_db -e "
   SELECT 'Scenarios' as Table_Name, COUNT(*) as Count FROM scenarios
   UNION ALL
   SELECT 'Time Steps', COUNT(*) FROM time_steps
   UNION ALL
   SELECT 'Counties', COUNT(*) FROM counties
   UNION ALL
   SELECT 'Transitions', COUNT(*) FROM land_use_transitions;
   "
   ```

### **3. Running Visualizations**

1. **Generate Test Visualizations**
   ```bash
   python examples/test_visualizations.py
   ```

2. **View Results**
   - Open the generated HTML files in the `outputs` directory:
     - `scenario_ranking.html`: Compare scenarios by forest loss
     - `time_series.html`: View changes over time
     - `sankey_diagram.html`: Analyze land use transitions
     - `county_choropleth.html`: Explore geographic patterns

### **4. Common Operations**

1. **Database Management**
   ```bash
   # Stop the database
   docker stop rpa-mysql-container
   
   # Start the database
   docker start rpa-mysql-container
   
   # Reset the database
   docker rm -f rpa-mysql-container
   docker run -d --name rpa-mysql-container -p 3306:3306 rpa-mysql
   ```

2. **Running Tests**
   ```bash
   # Run all tests
   pytest tests/test_land_use_analysis.py -v
   
   # Run specific test categories
   pytest tests/test_land_use_analysis.py -v -k "scenario"
   ```

3. **Querying Data**
   ```python
   # Example: Get forest loss rankings
   from src.api.queries import LandUseQueries
   
   rankings = LandUseQueries.rank_scenarios_by_forest_loss(
       target_year=2050,
       climate_model='NorESM1_M'
   )
   ```

### **5. Troubleshooting**

1. **Database Connection Issues**
   - Verify container is running: `docker ps`
   - Check logs: `docker logs rpa-mysql-container`
   - Ensure port 3306 is available: `netstat -tuln | grep 3306`

2. **Data Loading Issues**
   - Verify parquet file exists: `ls rpa_landuse_data.parquet`
   - Check database space: `docker exec rpa-mysql-container df -h`
   - Monitor MySQL logs: `docker logs -f rpa-mysql-container`

3. **Visualization Issues**
   - Ensure all dependencies are installed: `pip install -r requirements.txt`
   - Check output directory permissions: `ls -la outputs/`
   - Verify data exists in database using verification queries
