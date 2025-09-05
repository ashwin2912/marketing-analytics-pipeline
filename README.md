# Marketing Analytics Pipeline

A comprehensive 3-layer data engineering pipeline that transforms raw sales data into actionable business insights, customer segmentation, and campaign targeting recommendations.

## 🎯 Overview

This pipeline processes raw sales data through three analytical layers:
- **Layer 1 (Staging)**: Data ingestion, cleaning, and validation
- **Layer 2 (Warehouse)**: Dimensional modeling with star schema
- **Layer 3 (Business)**: Advanced analytics, customer segmentation, and insights

## 🚀 Quick Start

```bash
# 1. Clone and setup
git clone <repository-url>
cd marketing-analytics-pipeline

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the pipeline
python run_pipeline.py

# 5. Analyze results
cd notebooks
jupyter notebook business_analysis_dashboard.ipynb
```

## 📁 Project Structure

```
marketing-analytics-pipeline/
├── README.md                          # This documentation
├── requirements.txt                   # Python dependencies
├── run_pipeline.py                    # Main pipeline runner
├── test_pipeline.py                   # Comprehensive test suite
│
├── data/                              # Data storage
│   ├── raw/                          # Input data
│   │   └── HEC_testing_data_sample_2_.csv
│   ├── staging.db                    # Layer 1: Cleaned data
│   ├── warehouse.db                  # Layer 2: Dimensional model
│   ├── business.db                   # Layer 3: Business analysis
│   ├── metadata.db                   # Pipeline execution logs
│   └── exports/                      # CSV exports (optional)
│
├── pipeline/                          # Core pipeline modules
│   ├── master_pipeline.py            # Master orchestrator
│   ├── pipeline_orchestrator.py      # Database orchestration
│   ├── layer1_staging.py             # Data ingestion & cleaning
│   ├── layer2_warehouse.py           # Dimensional modeling
│   └── layer3_business.py            # Business analysis & insights
│
└── notebooks/                        # Analysis & visualization
    └── business_analysis_dashboard.ipynb  # Comprehensive dashboard
```

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.8+
- SQLite3 (included with Python)

### Setup Steps

1. **Create virtual environment** (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify installation**:
   ```bash
   python test_pipeline.py --quick
   ```

## 📊 Data Input Format

The pipeline expects CSV data with these columns:
- `Unnamed: 0`: Row index
- `Date`: Transaction date (YYYY-MM-DD format)
- `Customer ID`: Unique customer identifier
- `Order ID`: Unique order identifier  
- `Sales`: Transaction amount (numeric)

**Example:**
```csv
Unnamed: 0,Date,Customer ID,Order ID,Sales
0,2021-01-01,123456,1,150.00
1,2021-01-02,789012,2,75.50
```

## 🔧 Usage

### Running the Pipeline

**Option 1: Use the simple runner**
```bash
python run_pipeline.py [csv_file_path]
```

**Option 2: Use the master pipeline directly**
```bash
cd pipeline
python master_pipeline.py ../data/raw/your_data.csv
```

### Pipeline Options

- **Skip layers**: `--skip-layers staging warehouse`
- **Report only**: `--report-only` (no data processing)
- **Custom CSV**: Provide file path as argument

### Testing

**Quick test** (basic functionality):
```bash
python test_pipeline.py --quick
```

**Comprehensive test** (full pipeline with cleanup):
```bash
python test_pipeline.py --comprehensive
```

**Debug warehouse issues**:
```bash
python test_pipeline.py --debug-warehouse
```

## 🏗️ Architecture Deep Dive

### Layer 1: Staging (Data Ingestion)
**File**: `pipeline/layer1_staging.py`

- **Input**: Raw CSV files
- **Process**: Data cleaning, validation, type conversion
- **Output**: `staging.db` with cleaned data
- **Key Tables**:
  - `stg_sales_raw`: Raw ingested data
  - `stg_sales_clean`: Cleaned and validated data

**Data Quality Checks**:
- ✅ Required columns present
- ✅ Date format validation
- ✅ Numeric field validation
- ✅ Duplicate detection
- ✅ Missing value handling

### Layer 2: Warehouse (Dimensional Modeling)
**File**: `pipeline/layer2_warehouse.py`

- **Input**: Staging database
- **Process**: Star schema dimensional modeling
- **Output**: `warehouse.db` with dimensional model
- **Key Tables**:
  - `dim_date`: Date dimension (day, week, month, quarter, year)
  - `dim_customer`: Customer dimension with metadata
  - `dim_order`: Order dimension
  - `fact_sales`: Sales fact table with metrics

**Features**:
- 🌟 Star schema design for optimal query performance
- 📅 Complete date dimension with business calendar
- 🔗 Surrogate keys for dimension tables
- 📊 Pre-calculated aggregations

### Layer 3: Business Analysis
**File**: `pipeline/layer3_business.py`

- **Input**: Warehouse database
- **Process**: Advanced analytics, ML scoring, segmentation
- **Output**: `business.db` with analysis results
- **Key Tables**:
  - `monthly_metrics`: Revenue and customer trends
  - `cohort_analysis`: Customer retention by acquisition cohort
  - `customer_ltv_analysis`: Lifetime value scoring and churn risk
  - `campaign_targets`: Customers ready for marketing campaigns
  - `business_insights`: Automated insights and recommendations

**Advanced Analytics**:
- 📈 Customer Lifetime Value (LTV) prediction
- ⚠️ Churn risk scoring (0-100 scale)
- 👥 Customer segmentation (High Value, Regular, New, At Risk, Lost)
- 🎯 Campaign targeting with priority levels
- 💡 Automated business insights generation

## 📊 Business Intelligence Features

### Customer Segmentation
- **High Value**: Top 20% by LTV, active within 90 days
- **Regular**: Middle 60% customers with consistent activity
- **New**: Recent customers (< 90 days since first purchase)
- **At Risk**: Declining activity, high churn probability
- **Lost**: No activity > 365 days

### Churn Risk Scoring
Algorithm considers:
- Days since last purchase (recency)
- Purchase frequency decline
- Order value trends
- Historical cohort behavior

### Campaign Targeting
**Campaign Types**:
- **Win-Back**: Lost customers with high historical value
- **Retention**: At-risk customers needing intervention  
- **Cross-Sell**: Regular customers with growth potential
- **Loyalty**: High-value customers for premium offers

### Business Insights Engine
Automatically generates insights for:
- Revenue trends and anomalies
- Customer retention patterns
- High-value customer identification
- Campaign opportunity sizing
- Cohort performance analysis

## 📈 Analysis Dashboard

The Jupyter notebook (`notebooks/business_analysis_dashboard.ipynb`) provides:

1. **📊 Monthly Revenue & Customer Trends**
   - Revenue performance over time
   - Customer acquisition patterns
   - Average order value trends

2. **👥 Customer Cohort Analysis**
   - Retention heatmaps by acquisition month
   - Revenue per customer by cohort
   - Best performing cohort identification

3. **💎 Customer Lifetime Value Analysis**
   - LTV distribution and segmentation
   - Churn risk visualization
   - Order frequency vs. customer value correlation

4. **🎯 Campaign Targeting Analysis**
   - Campaign-ready customers by type
   - Priority level distribution
   - Estimated campaign value

5. **💡 Business Insights & Recommendations**
   - Automated insights prioritization
   - Actionable recommendations
   - Key performance indicators

6. **📈 Executive Summary Dashboard**
   - High-level KPIs
   - Risk assessment
   - Immediate action items

## 🔍 Data Quality & Monitoring

### Built-in Data Quality Checks
- **Completeness**: Required fields validation
- **Consistency**: Data type and format checks
- **Accuracy**: Range and logical validation
- **Timeliness**: Date sequence validation

### Pipeline Monitoring
- **Execution logs**: Detailed logging in `data/metadata.db`
- **Performance metrics**: Duration tracking per layer
- **Error handling**: Graceful failure with detailed messages
- **Data lineage**: Full traceability of data transformations

### Quality Reports
Access via pipeline orchestrator:
```python
from pipeline.pipeline_orchestrator import DataPipelineOrchestrator
orchestrator = DataPipelineOrchestrator()
quality_summary = orchestrator.get_data_quality_summary()
```

## 🧪 Testing Framework

### Test Coverage
- **Unit tests**: Individual component validation
- **Integration tests**: Cross-layer data flow
- **End-to-end tests**: Complete pipeline execution
- **Data quality tests**: Schema and content validation
- **Performance tests**: Execution time benchmarks

### Test Commands
```bash
# Quick smoke test
python test_pipeline.py --quick

# Full comprehensive test
python test_pipeline.py --comprehensive

# Warehouse debugging
python test_pipeline.py --debug-warehouse

# Custom data file test
python test_pipeline.py --comprehensive --csv-file path/to/data.csv
```

### Test Data Management
- ✅ Automatic test data cleanup
- ✅ Isolated test environments
- ✅ Production data protection
- ✅ Reproducible test scenarios

## 🔧 Configuration & Customization

### Pipeline Configuration
Modify behavior in `pipeline/master_pipeline.py`:
- Database connection settings
- Layer execution order
- Error handling preferences
- Logging levels

### Business Logic Customization
Key areas for customization:

**Customer Segmentation** (`layer3_business.py`):
```python
# Modify LTV thresholds
ltv_high_threshold = 500  # High-value customer minimum
ltv_regular_threshold = 100  # Regular customer minimum

# Adjust time windows
recent_activity_days = 90  # Active customer definition
at_risk_days = 180  # At-risk threshold
```

**Campaign Targeting**:
```python
# Campaign eligibility rules
min_days_for_winback = 90
min_ltv_for_retention = 200
high_priority_threshold = 0.7  # Churn risk score
```

### Adding Custom Analysis
To add new business analysis tables:

1. **Create table schema** in `layer3_business.py`
2. **Add analysis logic** with SQL queries
3. **Update summary reporting**
4. **Add notebook visualization**

## 📋 API Reference

### Pipeline Orchestrator
```python
from pipeline.pipeline_orchestrator import DataPipelineOrchestrator

# Initialize
orchestrator = DataPipelineOrchestrator()

# Get pipeline status
status = orchestrator.get_pipeline_status()

# Get data quality summary  
quality = orchestrator.get_data_quality_summary()

# Access databases
staging_conn = orchestrator.databases['staging']
```

### Master Pipeline Runner
```python
from pipeline.master_pipeline import MasterPipelineRunner

# Initialize runner
runner = MasterPipelineRunner()

# Run full pipeline
success = runner.run_full_pipeline('data.csv')

# Skip specific layers
success = runner.run_full_pipeline('data.csv', skip_layers=['staging'])

# Get execution report
report = runner.get_pipeline_report()
```

## 🚀 Performance & Scalability

### Current Performance
- **40K records**: ~25 seconds end-to-end
- **Layer 1**: <1 second (data cleaning)
- **Layer 2**: ~1 second (dimensional modeling)  
- **Layer 3**: ~23 seconds (advanced analytics)

### Optimization Tips
1. **Database indexes**: Auto-created on key fields
2. **Batch processing**: Configurable chunk sizes
3. **Memory management**: Efficient pandas operations
4. **Connection pooling**: Reused database connections

### Scaling Considerations
- **Horizontal scaling**: Process data in chunks
- **Database migration**: Move to PostgreSQL for larger datasets
- **Caching**: Implement Redis for frequent queries
- **Parallel processing**: Multi-threading for independent operations

## 🔒 Security & Data Privacy

### Data Protection
- ✅ Local SQLite databases (no cloud exposure)
- ✅ No external API calls or data transmission
- ✅ Configurable data retention policies
- ✅ Customer data anonymization options

### Access Control
- File-system level security
- Database encryption support
- Audit logging for data access
- GDPR compliance considerations

## 🐛 Troubleshooting

### Common Issues

**"ModuleNotFoundError"**:
```bash
# Ensure virtual environment is activated
source venv/bin/activate
pip install -r requirements.txt
```

**"Database is locked"**:
```bash
# Close all database connections and retry
python test_pipeline.py --cleanup
```

**"Column not found errors"**:
```bash
# Verify CSV format matches expected schema
# Check column names in your data file
```

**"Pipeline fails on Layer 3"**:
```bash
# Debug with warehouse inspection
python test_pipeline.py --debug-warehouse
```

### Debug Mode
Enable detailed logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Getting Help
1. Check the test suite: `python test_pipeline.py --comprehensive`
2. Review pipeline logs in `data/master_pipeline.log`
3. Examine database contents with SQLite browser
4. Run components individually for isolation testing

## 📝 Development


### Adding New Features
1. **Layer 1**: Data ingestion and quality improvements
2. **Layer 2**: New dimensional models or fact tables
3. **Layer 3**: Advanced analytics algorithms
4. **Visualization**: Dashboard enhancements
5. **Testing**: Additional test scenarios

## 📊 Sample Output

### Pipeline Execution Summary
```
✅ Pipeline completed successfully!

LAYER1: SUCCESS (0.13 seconds)
  Raw records: 40,000
  Cleaned records: 40,000

LAYER2: SUCCESS (0.53 seconds)  
  Unique customers: 33,477
  Total revenue: $6,709,020.43

LAYER3: SUCCESS (22.41 seconds)
  Campaign targets: 28,753
  Business insights: 3

🎯 KEY BUSINESS INSIGHTS:
  • High-Value Customers at Risk: 235 customers ($100,461 potential loss)
  • Customer Conversion Rate: 85.89% are one-time buyers  
  • Best Performing Cohort: 2021-07 with 2.0% retention
```

### Database Schema Summary
```
📋 Generated Databases:
  • staging.db - 40K cleaned records (6MB)
  • warehouse.db - Star schema with 4 dimensions (12MB)  
  • business.db - 28K campaign targets + insights (6MB)
  • metadata.db - Execution logs (65KB)
```

## 🎉 Next Steps

After running the pipeline:

1. **📊 Explore the dashboard**: Open the Jupyter notebook for interactive analysis
2. **🎯 Execute campaigns**: Use campaign_targets table for marketing actions  
3. **📈 Monitor performance**: Set up regular pipeline runs
4. **🔄 Iterate and improve**: Customize business logic for your needs
5. **📋 Export insights**: Use built-in CSV export functions


## Acknowledgments

- Built with pandas, SQLite, matplotlib, Jupyter and Claude
- Designed for marketing analytics and customer intelligence
- Inspired by modern data engineering best practices

