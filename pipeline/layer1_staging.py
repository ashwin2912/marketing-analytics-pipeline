# Layer 1: Staging Pipeline
# Raw data ingestion and initial data quality checks

import pandas as pd
import sqlite3
import logging
from datetime import datetime
import sys
import os

# Add the parent directory to sys.path to import our orchestrator
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from pipeline_orchestrator import DataPipelineOrchestrator, DataQualityChecker

class StagingLayer:
    """
    Layer 1: Staging Layer
    - Ingests raw data from CSV files
    - Performs basic data validation
    - Creates staging tables with minimal transformation
    """
    
    def __init__(self, orchestrator: DataPipelineOrchestrator):
        self.orchestrator = orchestrator
        self.staging_conn = orchestrator.databases['staging']
        self.dq_checker = DataQualityChecker(orchestrator)
        self.logger = orchestrator.logger
        
    def create_staging_schema(self):
        """Create staging tables with proper schema"""
        
        # Raw sales data staging table
        self.staging_conn.execute("""
            CREATE TABLE IF NOT EXISTS stg_sales_raw (
                unnamed_0 INTEGER,
                date_raw TEXT,
                customer_id INTEGER,
                order_id INTEGER,
                sales REAL,
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_file TEXT
            )
        """)
        
        """
        Staging table with basic cleaning
        Removed unnamed_0 -> might be the original dataframe index that was exported
        Converted date_raw to Date Type
        Renamed sales -> sales_amount for better clarity
        Added data_quality_flag
        """
        self.staging_conn.execute("""
            CREATE TABLE IF NOT EXISTS stg_sales_cleaned (
                date_parsed DATE,
                customer_id INTEGER,
                order_id INTEGER,
                sales_amount REAL,
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_quality_flag TEXT DEFAULT 'VALID'
            )
        """)
        
        self.staging_conn.commit()
        self.logger.info("Staging schema created successfully")
        
    def ingest_raw_data(self, csv_file_path: str) -> str:
        """Ingest raw CSV data into staging"""
        
        run_id = self.orchestrator.log_pipeline_run('STAGING', 'stg_sales_raw', 'STARTED')
        
        try:
            # Read CSV data
            df = pd.read_csv(csv_file_path)
            
            # Map CSV columns to database columns
            column_mapping = {
                'Unnamed: 0': 'unnamed_0',
                'Date': 'date_raw', 
                'Customer ID': 'customer_id',
                'Order ID': 'order_id',
                'Sales': 'sales'
            }
            df = df.rename(columns=column_mapping)
            
            # Add metadata columns to track when the file was loaded and where it came from
            df['load_timestamp'] = datetime.now()
            df['source_file'] = os.path.basename(csv_file_path)
            
            # Load to staging table
            df.to_sql('stg_sales_raw', self.staging_conn, if_exists='replace', index=False)
            
            row_count = len(df)
            self.orchestrator.log_pipeline_run('STAGING', 'stg_sales_raw', 'SUCCESS', row_count)
            
            self.logger.info(f"Successfully loaded {row_count} rows to stg_sales_raw")
            
            # Run basic data quality checks
            self._run_raw_data_quality_checks(run_id)
            
            return run_id
            
        except Exception as e:
            self.orchestrator.log_pipeline_run('STAGING', 'stg_sales_raw', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to ingest raw data: {str(e)}")
            raise
            
    def _run_raw_data_quality_checks(self, run_id: str):
        """Run data quality checks on raw data"""
        
        # Check minimum row count #How do I make this dynamic based on previous runs? Or based on expected data size?
        self.dq_checker.check_row_count(
            self.staging_conn, 'stg_sales_raw', min_rows=1000, run_id=run_id
        )
        
        # Check for null customer IDs 
        self.dq_checker.check_null_percentage(
            self.staging_conn, 'stg_sales_raw', 'customer_id', max_null_pct=0.0, run_id=run_id
        )
        
        # Check for null order IDs
        self.dq_checker.check_null_percentage(
            self.staging_conn, 'stg_sales_raw', 'order_id', max_null_pct=0.0, run_id=run_id
        )
        
        # Check for null sales amounts
        self.dq_checker.check_null_percentage(
            self.staging_conn, 'stg_sales_raw', 'sales', max_null_pct=0.0, run_id=run_id
        )
        
        # Check unique customers count #This needs to be dynamic too
        self.dq_checker.check_unique_count(
            self.staging_conn, 'stg_sales_raw', 'customer_id', min_unique=30000, run_id=run_id
        )
        
    def clean_and_validate_data(self) -> str:
        """Clean raw data and create validated staging table"""
        
        run_id = self.orchestrator.log_pipeline_run('STAGING', 'stg_sales_cleaned', 'STARTED')
        
        try:
            # Clear existing data first, this would ideally 
            self.staging_conn.execute("DELETE FROM stg_sales_cleaned")
            
            # Clean and validate data using SQL
            self.staging_conn.execute("""
                INSERT INTO stg_sales_cleaned (
                    date_parsed, customer_id, order_id, sales_amount, data_quality_flag
                )
                SELECT 
                    date(date_raw) as date_parsed,
                    customer_id,
                    order_id,
                    sales as sales_amount,
                    CASE 
                        WHEN date(date_raw) IS NULL THEN 'INVALID_DATE'
                        WHEN customer_id IS NULL THEN 'INVALID_CUSTOMER'
                        WHEN order_id IS NULL THEN 'INVALID_ORDER'
                        WHEN sales IS NULL OR sales <= 0 THEN 'INVALID_SALES'
                        ELSE 'VALID'
                    END as data_quality_flag
                FROM stg_sales_raw
            """)
            
            # Get row counts
            cursor = self.staging_conn.execute("SELECT COUNT(*) FROM stg_sales_cleaned")
            row_count = cursor.fetchone()[0]
            
            # Get data quality summary
            cursor = self.staging_conn.execute("""
                SELECT data_quality_flag, COUNT(*) 
                FROM stg_sales_cleaned 
                GROUP BY data_quality_flag
            """)
            quality_summary = cursor.fetchall()
            
            self.orchestrator.log_pipeline_run('STAGING', 'stg_sales_cleaned', 'SUCCESS', row_count)
            
            # Log data quality summary
            for flag, count in quality_summary:
                self.logger.info(f"Data quality flag '{flag}': {count} records")
                
            # Run cleaned data quality checks
            self._run_cleaned_data_quality_checks(run_id)
            
            self.staging_conn.commit()
            return run_id
            
        except Exception as e:
            self.orchestrator.log_pipeline_run('STAGING', 'stg_sales_cleaned', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to clean data: {str(e)}")
            raise
            
    def _run_cleaned_data_quality_checks(self, run_id: str):
        """Run data quality checks on cleaned data"""
        
        # Check that majority of records are valid
        try:
            cursor = self.staging_conn.execute("""
                SELECT 
                    SUM(CASE WHEN data_quality_flag = 'VALID' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as valid_pct
                FROM stg_sales_cleaned
            """)
            valid_pct = cursor.fetchone()[0]
            
            status = "PASSED" if valid_pct >= 95.0 else "FAILED"
            
            self.orchestrator.log_data_quality_check(
                run_id, 'stg_sales_cleaned', 'DATA_VALIDITY', 'valid_records_check',
                ">=95%", f"{valid_pct:.2f}%", status
            )
            
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, 'stg_sales_cleaned', 'DATA_VALIDITY', 'valid_records_check',
                ">=95%", "ERROR", "FAILED", str(e)
            )
            
        # Check date range
        self.dq_checker.check_date_range(
            self.staging_conn, 'stg_sales_cleaned', 'date_parsed', 
            '2020-01-01', '2025-12-31', run_id
        )
        
    def get_staging_summary(self) -> dict:
        """Get summary of staging layer data"""
        
        summary = {}
        
        # Raw data summary
        cursor = self.staging_conn.execute("SELECT COUNT(*) FROM stg_sales_raw")
        summary['raw_records'] = cursor.fetchone()[0]
        
        # Cleaned data summary
        cursor = self.staging_conn.execute("SELECT COUNT(*) FROM stg_sales_cleaned")
        summary['cleaned_records'] = cursor.fetchone()[0]
        
        # Data quality breakdown
        cursor = self.staging_conn.execute("""
            SELECT data_quality_flag, COUNT(*) 
            FROM stg_sales_cleaned 
            GROUP BY data_quality_flag
        """)
        summary['quality_breakdown'] = dict(cursor.fetchall())
        
        # Date range
        cursor = self.staging_conn.execute("""
            SELECT MIN(date_parsed), MAX(date_parsed) 
            FROM stg_sales_cleaned 
            WHERE data_quality_flag = 'VALID'
        """)
        date_range = cursor.fetchone()
        summary['date_range'] = {'min': date_range[0], 'max': date_range[1]}
        
        return summary


def run_staging_pipeline(csv_file_path: str):
    """Main function to run the staging pipeline"""
    
    # Initialize orchestrator
    orchestrator = DataPipelineOrchestrator()
    
    try:
        # Initialize staging layer
        staging = StagingLayer(orchestrator)
        
        # Create schema
        staging.create_staging_schema()
        
        # Ingest raw data
        staging.logger.info("Starting raw data ingestion...")
        raw_run_id = staging.ingest_raw_data(csv_file_path)
        
        # Clean and validate data
        staging.logger.info("Starting data cleaning and validation...")
        clean_run_id = staging.clean_and_validate_data()
        
        # Get summary
        summary = staging.get_staging_summary()
        staging.logger.info(f"Staging pipeline completed successfully: {summary}")
        
        print("STAGING LAYER SUMMARY:")
        print("=" * 50)
        print(f"Raw records loaded: {summary['raw_records']:,}")
        print(f"Cleaned records: {summary['cleaned_records']:,}")
        print(f"Date range: {summary['date_range']['min']} to {summary['date_range']['max']}")
        print("\nData Quality Breakdown:")
        for flag, count in summary['quality_breakdown'].items():
            print(f"  {flag}: {count:,} records")
            
        return orchestrator, summary
        
    except Exception as e:
        orchestrator.logger.error(f"Staging pipeline failed: {str(e)}")
        raise
    finally:
        # Note: Don't close connections here as they'll be used by subsequent layers
        pass


# Example usage
if __name__ == "__main__":
    csv_path = "path/to/your/sales_data.csv"  # Update this path
    
    try:
        orchestrator, summary = run_staging_pipeline(csv_path)
        print("\nStaging pipeline completed successfully!")
        
        # Show pipeline status
        print("\nPipeline Status:")
        status_df = orchestrator.get_pipeline_status()
        print(status_df.to_string(index=False))
        
        # Show data quality summary
        print("\nData Quality Summary:")
        dq_summary = orchestrator.get_data_quality_summary()
        print(dq_summary.to_string(index=False))
        
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
    finally:
        if 'orchestrator' in locals():
            orchestrator.close_connections()