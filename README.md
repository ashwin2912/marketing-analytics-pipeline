# Marketing Analytics Pipeline

A comprehensive data engineering pipeline that transforms raw sales data into actionable business insights, customer segmentation, and campaign targeting recommendations using dimensional modeling principles.

## Overview

This pipeline processes sales data through three analytical layers:
- **Layer 1 (Staging)**: Data ingestion, cleaning, and validation
- **Layer 2 (Warehouse)**: Dimensional modeling with star schema design
- **Layer 3 (Business)**: Customer analytics, segmentation, and automated insights

## Project Structure

```
.
├── DATA_DICTIONARY.md              # Complete schema documentation
├── README.md                       # Project documentation
├── data/                          # Data storage and databases
│   ├── business.db                # Layer 3: Business analytics
│   ├── master_pipeline.log        # Pipeline execution logs
│   ├── metadata.db                # Pipeline metadata and monitoring
│   ├── pipeline.log               # Detailed operation logs
│   ├── raw/                       # Input data directory
│   │   └── HEC_testing_data_sample_2_.csv
│   ├── staging.db                 # Layer 1: Cleaned data
│   └── warehouse.db               # Layer 2: Dimensional model
├── notebooks/                     # Analysis and exploration
│   └── database_explorer.ipynb   # Interactive database table explorer
├── pipeline/                      # Core pipeline modules
│   ├── __init__.py               # Package initialization
│   ├── layer1_staging.py         # Data ingestion and cleaning
│   ├── layer2_warehouse.py       # Dimensional modeling
│   ├── layer3_business.py        # Business analytics and insights
│   ├── master_pipeline.py        # Pipeline orchestration
│   └── pipeline_orchestrator.py  # Database and workflow management
├── requirements.txt               # Python dependencies
├── run_pipeline.py               # Simple pipeline runner
└── test_pipeline.py              # Comprehensive test suite
```

## Usage

### Basic Pipeline Execution
```bash
python run_pipeline.py [path/to/your/data.csv]
```

### Testing

**Run the comprehensive pipeline test:**
```bash
python test_pipeline.py
```

**Clean up test environment:**
```bash
python test_pipeline.py --cleanup
```

## Input Data Format

The pipeline expects CSV data with columns:
- `Date`: Transaction date (YYYY-MM-DD)
- `Customer ID`: Unique customer identifier
- `Order ID`: Unique order identifier
- `Sales`: Transaction amount (numeric)

## Pipeline Architecture

### Core Pipeline Modules

**layer1_staging.py**: Data ingestion and quality validation
- Reads raw CSV files from `data/raw/`
- Performs data cleaning and validation
- Creates `staging.db` with cleaned data

**layer2_warehouse.py**: Dimensional modeling
- Builds star schema from staging data
- Creates customer, date, order dimensions
- Generates sales fact table
- Outputs to `warehouse.db`

**layer3_business.py**: Advanced analytics
- Customer segmentation using RFM analysis
- Lifetime value prediction and churn scoring
- Cohort and retention analysis
- Campaign targeting recommendations
- Automated business insights
- Creates `business.db` with 9 analytics tables

**pipeline_orchestrator.py**: Database and workflow management
- Manages SQLite database connections
- Handles data quality logging
- Coordinates cross-layer operations
- Maintains execution metadata

**master_pipeline.py**: End-to-end orchestration
- Coordinates all three layers
- Manages error handling and logging
- Provides execution reporting
- Supports layer skipping and debugging

## Key Outputs

- **Monthly Metrics**: Revenue trends and customer acquisition
- **Cohort Analysis**: Customer retention by acquisition month
- **Cumulative Retention**: 3, 12, and 18-month retention windows
- **Customer Segmentation**: RFM-based segments with marketing strategies
- **Campaign Targets**: Priority-scored customer lists for marketing
- **Business Insights**: Automated recommendations and alerts
- **Lifecycle Tracking**: Customer health monitoring across stages

## Documentation

- [Data Dictionary](DATA_DICTIONARY.md) - Complete schema documentation for all tables and business rules
- [Database Explorer](notebooks/database_explorer.ipynb) - Interactive notebook to explore all pipeline tables and data

## Performance

- Processes 40K records in ~25 seconds
- Generates 9 specialized analytics tables
- Includes comprehensive data quality validation
- Supports incremental updates

## Requirements

- Python 3.8+
- pandas, sqlite3, matplotlib, jupyter
- See `requirements.txt` for complete list

## Troubleshooting

**Module import errors**: Ensure virtual environment is activated and dependencies installed
```bash
pip install -r requirements.txt
```

**Database locked errors**: Clean up test environment and retry
```bash
python test_pipeline.py --cleanup
```

**Column format errors**: Verify CSV format matches expected schema (Date, Customer ID, Order ID, Sales)

**Pipeline failures**: Check logs in `data/pipeline.log` and run comprehensive test
```bash
python test_pipeline.py
```

**Test Results**:
- **Enhanced Components Success Rate**: Should be 100%
- **Data Quality Success Rate**: Should be 100% 
- **Pipeline Success Rate**: Should be 85%+
- **Overall Success Rate**: Should be 70%+
