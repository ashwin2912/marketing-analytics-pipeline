# Layer 2: Data Warehouse Pipeline
# Creates Facts and Dimensions from staging data using SQL transformations

import pandas as pd
import sqlite3
import logging
from datetime import datetime
import sys
import os

# Add the parent directory to sys.path to import our orchestrator
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from pipeline_orchestrator import DataPipelineOrchestrator, DataQualityChecker

class WarehouseLayer:
    """
    Layer 2: Data Warehouse Layer
    - Creates dimensional model from staging data
    - Builds fact and dimension tables using SQL transformations
    - Implements data quality checks for warehouse tables
    """
    
    def __init__(self, orchestrator: DataPipelineOrchestrator):
        self.orchestrator = orchestrator
        self.warehouse_conn = orchestrator.databases['warehouse']
        self.staging_conn = orchestrator.databases['staging']
        self.dq_checker = DataQualityChecker(orchestrator)
        self.logger = orchestrator.logger
        
        # Attach staging database to warehouse connection for cross-database queries
        staging_db_path = f'{orchestrator.base_path}/staging.db'
        self.warehouse_conn.execute(f"ATTACH DATABASE '{staging_db_path}' AS staging")
        
    def create_warehouse_schema(self):
        """Create warehouse dimension and fact tables"""
        
        # Date Dimension
        self.warehouse_conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_date (
                date_id INTEGER PRIMARY KEY,
                full_date DATE,
                year INTEGER,
                quarter INTEGER,
                month INTEGER,
                month_name TEXT,
                month_abbr TEXT,
                week_of_year INTEGER,
                day_of_year INTEGER,
                day_of_month INTEGER,
                day_of_week INTEGER,
                day_name TEXT,
                day_abbr TEXT,
                is_weekend INTEGER,
                is_month_start INTEGER,
                is_month_end INTEGER,
                is_quarter_start INTEGER,
                is_quarter_end INTEGER,
                is_year_start INTEGER,
                is_year_end INTEGER,
                date_string TEXT,
                month_year TEXT,
                quarter_year TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Customer Dimension
        self.warehouse_conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_customer (
                customer_id INTEGER PRIMARY KEY,
                first_order_date DATE,
                last_order_date DATE,
                total_transactions INTEGER,
                total_spent REAL,
                avg_order_value REAL,
                total_orders INTEGER,
                first_order_cohort_month TEXT,
                first_order_cohort_quarter TEXT,
                first_order_cohort_year INTEGER,
                days_since_first_order INTEGER,
                customer_vintage_group TEXT,
                days_since_last_order INTEGER,
                customer_segment TEXT,
                customer_status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Order Dimension
        self.warehouse_conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_order (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date DATE,
                date_id INTEGER,
                order_amount REAL,
                order_year INTEGER,
                order_month INTEGER,
                order_quarter INTEGER,
                order_day_of_week INTEGER,
                order_day_name TEXT,
                is_weekend_order INTEGER,
                customer_order_sequence INTEGER,
                is_first_order INTEGER,
                days_since_customer_first_order INTEGER,
                days_since_previous_order INTEGER,
                order_amount_quartile TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Sales Fact Table
        self.warehouse_conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                customer_id INTEGER,
                order_id INTEGER,
                date_id INTEGER,
                sales_amount REAL,
                transaction_count INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (customer_id, order_id, date_id)
            )
        """)
        
        self.warehouse_conn.commit()
        self.logger.info("Data warehouse schema created successfully")
        
    def build_date_dimension(self) -> str:
        """Build date dimension table"""
        
        run_id = self.orchestrator.log_pipeline_run('WAREHOUSE', 'dim_date', 'STARTED')
        
        try:
            # Get date range from staging data
            cursor = self.warehouse_conn.execute("""
                SELECT MIN(date_parsed), MAX(date_parsed) 
                FROM staging.stg_sales_cleaned 
                WHERE data_quality_flag = 'VALID'
            """)
            min_date, max_date = cursor.fetchone()
            
            # Create date dimension using simpler approach from staging dates
            self.warehouse_conn.execute("""
                INSERT OR REPLACE INTO dim_date 
                (date_id, full_date, year, quarter, month, month_name, month_abbr,
                 week_of_year, day_of_year, day_of_month, day_of_week, day_name, day_abbr,
                 is_weekend, is_month_start, is_month_end, is_quarter_start, is_quarter_end,
                 is_year_start, is_year_end, date_string, month_year, quarter_year)
                SELECT DISTINCT
                    CAST(REPLACE(date_parsed, '-', '') AS INTEGER) as date_id,
                    date_parsed as full_date,
                    CAST(strftime('%Y', date_parsed) AS INTEGER) as year,
                    CASE 
                        WHEN CAST(strftime('%m', date_parsed) AS INTEGER) BETWEEN 1 AND 3 THEN 1
                        WHEN CAST(strftime('%m', date_parsed) AS INTEGER) BETWEEN 4 AND 6 THEN 2
                        WHEN CAST(strftime('%m', date_parsed) AS INTEGER) BETWEEN 7 AND 9 THEN 3
                        ELSE 4
                    END as quarter,
                    CAST(strftime('%m', date_parsed) AS INTEGER) as month,
                    CASE strftime('%m', date_parsed)
                        WHEN '01' THEN 'January' WHEN '02' THEN 'February'
                        WHEN '03' THEN 'March' WHEN '04' THEN 'April'
                        WHEN '05' THEN 'May' WHEN '06' THEN 'June'
                        WHEN '07' THEN 'July' WHEN '08' THEN 'August'
                        WHEN '09' THEN 'September' WHEN '10' THEN 'October'
                        WHEN '11' THEN 'November' WHEN '12' THEN 'December'
                    END as month_name,
                    CASE strftime('%m', date_parsed)
                        WHEN '01' THEN 'Jan' WHEN '02' THEN 'Feb'
                        WHEN '03' THEN 'Mar' WHEN '04' THEN 'Apr'
                        WHEN '05' THEN 'May' WHEN '06' THEN 'Jun'
                        WHEN '07' THEN 'Jul' WHEN '08' THEN 'Aug'
                        WHEN '09' THEN 'Sep' WHEN '10' THEN 'Oct'
                        WHEN '11' THEN 'Nov' WHEN '12' THEN 'Dec'
                    END as month_abbr,
                    CAST(strftime('%W', date_parsed) AS INTEGER) as week_of_year,
                    CAST(strftime('%j', date_parsed) AS INTEGER) as day_of_year,
                    CAST(strftime('%d', date_parsed) AS INTEGER) as day_of_month,
                    CAST(strftime('%w', date_parsed) AS INTEGER) + 1 as day_of_week,
                    strftime('%A', date_parsed) as day_name,
                    CASE strftime('%w', date_parsed)
                        WHEN '0' THEN 'Sun' WHEN '1' THEN 'Mon' WHEN '2' THEN 'Tue'
                        WHEN '3' THEN 'Wed' WHEN '4' THEN 'Thu' WHEN '5' THEN 'Fri'
                        WHEN '6' THEN 'Sat'
                    END as day_abbr,
                    CASE WHEN strftime('%w', date_parsed) IN ('0', '6') THEN 1 ELSE 0 END as is_weekend,
                    CASE WHEN strftime('%d', date_parsed) = '01' THEN 1 ELSE 0 END as is_month_start,
                    CASE WHEN date_parsed = date(date_parsed, 'start of month', '+1 month', '-1 day') THEN 1 ELSE 0 END as is_month_end,
                    CASE WHEN strftime('%m-%d', date_parsed) IN ('01-01', '04-01', '07-01', '10-01') THEN 1 ELSE 0 END as is_quarter_start,
                    CASE WHEN strftime('%m-%d', date_parsed) IN ('03-31', '06-30', '09-30', '12-31') THEN 1 ELSE 0 END as is_quarter_end,
                    CASE WHEN strftime('%m-%d', date_parsed) = '01-01' THEN 1 ELSE 0 END as is_year_start,
                    CASE WHEN strftime('%m-%d', date_parsed) = '12-31' THEN 1 ELSE 0 END as is_year_end,
                    date_parsed as date_string,
                    strftime('%b %Y', date_parsed) as month_year,
                    'Q' || CASE 
                        WHEN CAST(strftime('%m', date_parsed) AS INTEGER) BETWEEN 1 AND 3 THEN '1'
                        WHEN CAST(strftime('%m', date_parsed) AS INTEGER) BETWEEN 4 AND 6 THEN '2'
                        WHEN CAST(strftime('%m', date_parsed) AS INTEGER) BETWEEN 7 AND 9 THEN '3'
                        ELSE '4'
                    END as quarter_year
                FROM staging.stg_sales_cleaned 
                WHERE data_quality_flag = 'VALID'
                ORDER BY date_parsed
            """)
            
            # Get row count
            cursor = self.warehouse_conn.execute("SELECT COUNT(*) FROM dim_date")
            row_count = cursor.fetchone()[0]
            
            self.orchestrator.log_pipeline_run('WAREHOUSE', 'dim_date', 'SUCCESS', row_count)
            
            # Run data quality checks
            self._run_date_dimension_quality_checks(run_id)
            
            self.warehouse_conn.commit()
            self.logger.info(f"Date dimension built successfully with {row_count} rows")
            
            return run_id
            
        except Exception as e:
            self.orchestrator.log_pipeline_run('WAREHOUSE', 'dim_date', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to build date dimension: {str(e)}")
            raise
            
    def build_customer_dimension(self) -> str:
        """Build customer dimension table"""
        
        run_id = self.orchestrator.log_pipeline_run('WAREHOUSE', 'dim_customer', 'STARTED')
        
        try:
            # Build customer dimension from staging data
            self.warehouse_conn.execute("""
                INSERT OR REPLACE INTO dim_customer (
                    customer_id, first_order_date, last_order_date, total_transactions,
                    total_spent, avg_order_value, total_orders, first_order_cohort_month,
                    first_order_cohort_quarter, first_order_cohort_year, days_since_first_order,
                    customer_vintage_group, days_since_last_order, customer_segment, customer_status
                )
                WITH customer_metrics AS (
                    SELECT 
                        customer_id,
                        MIN(date_parsed) as first_order_date,
                        MAX(date_parsed) as last_order_date,
                        COUNT(*) as total_transactions,
                        SUM(sales_amount) as total_spent,
                        AVG(sales_amount) as avg_order_value,
                        COUNT(DISTINCT order_id) as total_orders,
                        strftime('%Y-%m', MIN(date_parsed)) as first_order_cohort_month,
                        strftime('%Y', MIN(date_parsed)) || '-Q' || 
                        CASE 
                            WHEN CAST(strftime('%m', MIN(date_parsed)) AS INTEGER) BETWEEN 1 AND 3 THEN '1'
                            WHEN CAST(strftime('%m', MIN(date_parsed)) AS INTEGER) BETWEEN 4 AND 6 THEN '2'
                            WHEN CAST(strftime('%m', MIN(date_parsed)) AS INTEGER) BETWEEN 7 AND 9 THEN '3'
                            ELSE '4'
                        END as first_order_cohort_quarter,
                        CAST(strftime('%Y', MIN(date_parsed)) AS INTEGER) as first_order_cohort_year,
                        julianday('now') - julianday(MIN(date_parsed)) as days_since_first_order,
                        julianday('now') - julianday(MAX(date_parsed)) as days_since_last_order
                    FROM staging.stg_sales_cleaned 
                    WHERE data_quality_flag = 'VALID'
                    GROUP BY customer_id
                ),
                customer_enriched AS (
                    SELECT *,
                        CASE 
                            WHEN days_since_first_order <= 30 THEN '0-30 days'
                            WHEN days_since_first_order <= 90 THEN '31-90 days'
                            WHEN days_since_first_order <= 180 THEN '91-180 days'
                            WHEN days_since_first_order <= 365 THEN '181-365 days'
                            ELSE '365+ days'
                        END as customer_vintage_group,
                        CASE 
                            WHEN total_spent >= (SELECT total_spent FROM customer_metrics ORDER BY total_spent DESC LIMIT 1 OFFSET CAST((SELECT COUNT(*) FROM customer_metrics) * 0.8 AS INTEGER)) THEN
                                CASE WHEN days_since_last_order <= 30 THEN 'VIP Active' ELSE 'VIP At Risk' END
                            WHEN total_spent >= (SELECT AVG(total_spent) FROM customer_metrics) THEN
                                CASE WHEN days_since_last_order <= 60 THEN 'Regular Active' ELSE 'Regular At Risk' END
                            ELSE
                                CASE 
                                    WHEN total_orders = 1 THEN 'One-Time Buyer'
                                    WHEN days_since_last_order <= 90 THEN 'Low Value Active'
                                    ELSE 'Low Value Inactive'
                                END
                        END as customer_segment,
                        CASE 
                            WHEN days_since_last_order <= 30 THEN 'Active'
                            WHEN days_since_last_order <= 90 THEN 'At Risk'
                            ELSE 'Inactive'
                        END as customer_status
                    FROM customer_metrics
                )
                SELECT * FROM customer_enriched
            """)
            
            # Get row count
            cursor = self.warehouse_conn.execute("SELECT COUNT(*) FROM dim_customer")
            row_count = cursor.fetchone()[0]
            
            self.orchestrator.log_pipeline_run('WAREHOUSE', 'dim_customer', 'SUCCESS', row_count)
            
            # Run data quality checks
            self._run_customer_dimension_quality_checks(run_id)
            
            self.warehouse_conn.commit()
            self.logger.info(f"Customer dimension built successfully with {row_count} rows")
            
            return run_id
            
        except Exception as e:
            self.orchestrator.log_pipeline_run('WAREHOUSE', 'dim_customer', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to build customer dimension: {str(e)}")
            raise
            
    def build_order_dimension(self) -> str:
        """Build order dimension table"""
        
        run_id = self.orchestrator.log_pipeline_run('WAREHOUSE', 'dim_order', 'STARTED')
        
        try:
            # Build order dimension from staging data
            self.warehouse_conn.execute("""
                INSERT OR REPLACE INTO dim_order (
                    order_id, customer_id, order_date, date_id, order_amount,
                    order_year, order_month, order_quarter, order_day_of_week,
                    order_day_name, is_weekend_order, customer_order_sequence,
                    is_first_order, days_since_customer_first_order, 
                    days_since_previous_order, order_amount_quartile
                )
                WITH order_sequence AS (
                    SELECT 
                        order_id,
                        customer_id,
                        date_parsed as order_date,
                        CAST(REPLACE(strftime('%Y-%m-%d', date_parsed), '-', '') AS INTEGER) as date_id,
                        sales_amount as order_amount,
                        CAST(strftime('%Y', date_parsed) AS INTEGER) as order_year,
                        CAST(strftime('%m', date_parsed) AS INTEGER) as order_month,
                        CASE 
                            WHEN CAST(strftime('%m', date_parsed) AS INTEGER) BETWEEN 1 AND 3 THEN 1
                            WHEN CAST(strftime('%m', date_parsed) AS INTEGER) BETWEEN 4 AND 6 THEN 2
                            WHEN CAST(strftime('%m', date_parsed) AS INTEGER) BETWEEN 7 AND 9 THEN 3
                            ELSE 4
                        END as order_quarter,
                        CAST(strftime('%w', date_parsed) AS INTEGER) + 1 as order_day_of_week,
                        strftime('%A', date_parsed) as order_day_name,
                        CASE WHEN strftime('%w', date_parsed) IN ('0', '6') THEN 1 ELSE 0 END as is_weekend_order,
                        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY date_parsed, order_id) as customer_order_sequence,
                        LAG(date_parsed) OVER (PARTITION BY customer_id ORDER BY date_parsed, order_id) as prev_order_date
                    FROM staging.stg_sales_cleaned 
                    WHERE data_quality_flag = 'VALID'
                ),
                customer_first_orders AS (
                    SELECT customer_id, MIN(order_date) as first_order_date
                    FROM order_sequence
                    GROUP BY customer_id
                ),
                orders_enriched AS (
                    SELECT 
                        o.*,
                        CASE WHEN o.customer_order_sequence = 1 THEN 1 ELSE 0 END as is_first_order,
                        julianday(o.order_date) - julianday(cfo.first_order_date) as days_since_customer_first_order,
                        CASE 
                            WHEN o.prev_order_date IS NOT NULL 
                            THEN julianday(o.order_date) - julianday(o.prev_order_date)
                            ELSE NULL 
                        END as days_since_previous_order,
                        NTILE(4) OVER (ORDER BY o.order_amount) as amount_quartile
                    FROM order_sequence o
                    JOIN customer_first_orders cfo ON o.customer_id = cfo.customer_id
                ),
                final_orders AS (
                    SELECT *,
                        CASE amount_quartile
                            WHEN 1 THEN 'Low'
                            WHEN 2 THEN 'Medium-Low'
                            WHEN 3 THEN 'Medium-High'
                            WHEN 4 THEN 'High'
                        END as order_amount_quartile
                    FROM orders_enriched
                )
                SELECT 
                    order_id, customer_id, order_date, date_id, order_amount,
                    order_year, order_month, order_quarter, order_day_of_week,
                    order_day_name, is_weekend_order, customer_order_sequence,
                    is_first_order, days_since_customer_first_order,
                    days_since_previous_order, order_amount_quartile
                FROM final_orders
            """)
            
            # Get row count
            cursor = self.warehouse_conn.execute("SELECT COUNT(*) FROM dim_order")
            row_count = cursor.fetchone()[0]
            
            self.orchestrator.log_pipeline_run('WAREHOUSE', 'dim_order', 'SUCCESS', row_count)
            
            # Run data quality checks
            self._run_order_dimension_quality_checks(run_id)
            
            self.warehouse_conn.commit()
            self.logger.info(f"Order dimension built successfully with {row_count} rows")
            
            return run_id
            
        except Exception as e:
            self.orchestrator.log_pipeline_run('WAREHOUSE', 'dim_order', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to build order dimension: {str(e)}")
            raise
            
    def build_sales_fact(self) -> str:
        """Build sales fact table"""
        
        run_id = self.orchestrator.log_pipeline_run('WAREHOUSE', 'fact_sales', 'STARTED')
        
        try:
            # Build fact table from staging data
            self.warehouse_conn.execute("""
                INSERT OR REPLACE INTO fact_sales (
                    customer_id, order_id, date_id, sales_amount, transaction_count
                )
                SELECT 
                    customer_id,
                    order_id,
                    CAST(REPLACE(strftime('%Y-%m-%d', date_parsed), '-', '') AS INTEGER) as date_id,
                    sales_amount,
                    1 as transaction_count
                FROM staging.stg_sales_cleaned 
                WHERE data_quality_flag = 'VALID'
                ORDER BY date_parsed, customer_id, order_id
            """)
            
            # Get row count
            cursor = self.warehouse_conn.execute("SELECT COUNT(*) FROM fact_sales")
            row_count = cursor.fetchone()[0]
            
            self.orchestrator.log_pipeline_run('WAREHOUSE', 'fact_sales', 'SUCCESS', row_count)
            
            # Run data quality checks
            self._run_sales_fact_quality_checks(run_id)
            
            self.warehouse_conn.commit()
            self.logger.info(f"Sales fact table built successfully with {row_count} rows")
            
            return run_id
            
        except Exception as e:
            self.orchestrator.log_pipeline_run('WAREHOUSE', 'fact_sales', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to build sales fact table: {str(e)}")
            raise
            
    def _run_date_dimension_quality_checks(self, run_id: str):
        """Run data quality checks on date dimension"""
        
        # Check for no gaps in date sequence
        try:
            cursor = self.warehouse_conn.execute("""
                WITH date_gaps AS (
                    SELECT 
                        full_date,
                        LAG(full_date) OVER (ORDER BY full_date) as prev_date,
                        julianday(full_date) - julianday(LAG(full_date) OVER (ORDER BY full_date)) as day_diff
                    FROM dim_date
                )
                SELECT COUNT(*) FROM date_gaps WHERE day_diff > 1
            """)
            gap_count = cursor.fetchone()[0]
            
            status = "PASSED" if gap_count == 0 else "FAILED"
            
            self.orchestrator.log_data_quality_check(
                run_id, 'dim_date', 'COMPLETENESS', 'no_date_gaps_check',
                "0", str(gap_count), status
            )
            
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, 'dim_date', 'COMPLETENESS', 'no_date_gaps_check',
                "0", "ERROR", "FAILED", str(e)
            )
            
        # Check weekends are correctly identified
        self.dq_checker.check_null_percentage(
            self.warehouse_conn, 'dim_date', 'is_weekend', max_null_pct=0.0, run_id=run_id
        )
        
    def _run_customer_dimension_quality_checks(self, run_id: str):
        """Run data quality checks on customer dimension"""
        
        # Check no null customer IDs
        self.dq_checker.check_null_percentage(
            self.warehouse_conn, 'dim_customer', 'customer_id', max_null_pct=0.0, run_id=run_id
        )
        
        # Check all customers have first order date
        self.dq_checker.check_null_percentage(
            self.warehouse_conn, 'dim_customer', 'first_order_date', max_null_pct=0.0, run_id=run_id
        )
        
        # Check customer segments are assigned
        self.dq_checker.check_null_percentage(
            self.warehouse_conn, 'dim_customer', 'customer_segment', max_null_pct=0.0, run_id=run_id
        )
        
    def _run_order_dimension_quality_checks(self, run_id: str):
        """Run data quality checks on order dimension"""
        
        # Check no null order IDs
        self.dq_checker.check_null_percentage(
            self.warehouse_conn, 'dim_order', 'order_id', max_null_pct=0.0, run_id=run_id
        )
        
        # Check order sequence starts at 1 for each customer
        try:
            cursor = self.warehouse_conn.execute("""
                SELECT COUNT(*) 
                FROM dim_order 
                WHERE customer_order_sequence = 1 AND is_first_order = 0
            """)
            invalid_sequence = cursor.fetchone()[0]
            
            status = "PASSED" if invalid_sequence == 0 else "FAILED"
            
            self.orchestrator.log_data_quality_check(
                run_id, 'dim_order', 'BUSINESS_RULE', 'first_order_sequence_check',
                "0", str(invalid_sequence), status
            )
            
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, 'dim_order', 'BUSINESS_RULE', 'first_order_sequence_check',
                "0", "ERROR", "FAILED", str(e)
            )
            
    def _run_sales_fact_quality_checks(self, run_id: str):
        """Run data quality checks on sales fact table"""
        
        # Check no negative sales amounts
        try:
            cursor = self.warehouse_conn.execute("""
                SELECT COUNT(*) FROM fact_sales WHERE sales_amount <= 0
            """)
            negative_sales = cursor.fetchone()[0]
            
            status = "PASSED" if negative_sales == 0 else "FAILED"
            
            self.orchestrator.log_data_quality_check(
                run_id, 'fact_sales', 'BUSINESS_RULE', 'positive_sales_check',
                "0", str(negative_sales), status
            )
            
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, 'fact_sales', 'BUSINESS_RULE', 'positive_sales_check',
                "0", "ERROR", "FAILED", str(e)
            )
            
        # Check referential integrity with dimensions
        try:
            cursor = self.warehouse_conn.execute("""
                SELECT COUNT(*) 
                FROM fact_sales f
                LEFT JOIN dim_customer c ON f.customer_id = c.customer_id
                WHERE c.customer_id IS NULL
            """)
            orphaned_customers = cursor.fetchone()[0]
            
            status = "PASSED" if orphaned_customers == 0 else "FAILED"
            
            self.orchestrator.log_data_quality_check(
                run_id, 'fact_sales', 'REFERENTIAL_INTEGRITY', 'customer_fk_check',
                "0", str(orphaned_customers), status
            )
            
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, 'fact_sales', 'REFERENTIAL_INTEGRITY', 'customer_fk_check',
                "0", "ERROR", "FAILED", str(e)
            )
            
    def get_warehouse_summary(self) -> dict:
        """Get summary of warehouse layer data"""
        
        summary = {}
        
        # Table row counts
        tables = ['dim_date', 'dim_customer', 'dim_order', 'fact_sales']
        for table in tables:
            cursor = self.warehouse_conn.execute(f"SELECT COUNT(*) FROM {table}")
            summary[f'{table}_rows'] = cursor.fetchone()[0]
            
        # Business metrics
        cursor = self.warehouse_conn.execute("""
            SELECT 
                COUNT(DISTINCT customer_id) as unique_customers,
                COUNT(*) as total_transactions,
                SUM(sales_amount) as total_revenue,
                AVG(sales_amount) as avg_transaction_value
            FROM fact_sales
        """)
        metrics = cursor.fetchone()
        summary.update({
            'unique_customers': metrics[0],
            'total_transactions': metrics[1], 
            'total_revenue': round(metrics[2], 2),
            'avg_transaction_value': round(metrics[3], 2)
        })
        
        return summary


def run_warehouse_pipeline(orchestrator=None):
    """Main function to run the warehouse pipeline"""
    
    # Use provided orchestrator or initialize new one
    if orchestrator is None:
        orchestrator = DataPipelineOrchestrator()
    
    try:
        # Initialize warehouse layer
        warehouse = WarehouseLayer(orchestrator)
        
        # Create schema
        warehouse.create_warehouse_schema()
        
        # Build dimensions and facts
        warehouse.logger.info("Building date dimension...")
        warehouse.build_date_dimension()
        
        warehouse.logger.info("Building customer dimension...")
        warehouse.build_customer_dimension()
        
        warehouse.logger.info("Building order dimension...")
        warehouse.build_order_dimension()
        
        warehouse.logger.info("Building sales fact table...")
        warehouse.build_sales_fact()
        
        # Get summary
        summary = warehouse.get_warehouse_summary()
        warehouse.logger.info(f"Warehouse pipeline completed successfully: {summary}")
        
        print("WAREHOUSE LAYER SUMMARY:")
        print("=" * 50)
        print(f"Date dimension: {summary['dim_date_rows']:,} rows")
        print(f"Customer dimension: {summary['dim_customer_rows']:,} rows")
        print(f"Order dimension: {summary['dim_order_rows']:,} rows")
        print(f"Sales fact: {summary['fact_sales_rows']:,} rows")
        print(f"\nBusiness Metrics:")
        print(f"Unique customers: {summary['unique_customers']:,}")
        print(f"Total transactions: {summary['total_transactions']:,}")
        print(f"Total revenue: ${summary['total_revenue']:,.2f}")
        print(f"Avg transaction value: ${summary['avg_transaction_value']:.2f}")
        
        return orchestrator, summary
        
    except Exception as e:
        orchestrator.logger.error(f"Warehouse pipeline failed: {str(e)}")
        raise


# Example usage
if __name__ == "__main__":
    try:
        orchestrator, summary = run_warehouse_pipeline()
        print("\nWarehouse pipeline completed successfully!")
        
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