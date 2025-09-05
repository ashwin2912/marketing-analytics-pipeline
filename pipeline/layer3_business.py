# Layer 3: Business Analysis Pipeline
# Creates aggregated views and analysis for business consumption

import pandas as pd
import sqlite3
import logging
from datetime import datetime
import sys
import os

# Add the parent directory to sys.path to import our orchestrator
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from pipeline_orchestrator import DataPipelineOrchestrator, DataQualityChecker

class BusinessAnalysisLayer:
    """
    Layer 3: Business Analysis Layer
    - Creates aggregated views from warehouse tables
    - Builds business-focused analysis tables
    - Provides marketing insights and recommendations
    """
    
    def __init__(self, orchestrator: DataPipelineOrchestrator):
        self.orchestrator = orchestrator
        self.business_conn = orchestrator.databases['business']
        self.warehouse_conn = orchestrator.databases['warehouse']
        self.dq_checker = DataQualityChecker(orchestrator)
        self.logger = orchestrator.logger
        
        # Attach warehouse database to business connection for cross-database queries
        warehouse_db_path = f'{orchestrator.base_path}/warehouse.db'
        self.business_conn.execute(f"ATTACH DATABASE '{warehouse_db_path}' AS warehouse")
        
    def create_business_schema(self):
        """Create business analysis tables"""
        
        # Monthly aggregated metrics
        self.business_conn.execute("""
            CREATE TABLE IF NOT EXISTS monthly_metrics (
                period_month TEXT PRIMARY KEY,
                total_sales REAL,
                avg_order_value REAL,
                total_transactions INTEGER,
                total_orders INTEGER,
                unique_customers INTEGER,
                purchase_frequency REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Customer cohort analysis
        self.business_conn.execute("""
            CREATE TABLE IF NOT EXISTS cohort_analysis (
                cohort_month TEXT,
                activity_month TEXT, 
                months_since_acquisition INTEGER,
                cohort_size INTEGER,
                active_customers INTEGER,
                retention_rate_percent REAL,
                total_sales REAL,
                avg_order_value REAL,
                PRIMARY KEY (cohort_month, activity_month)
            )
        """)
        
        # Cumulative Retention Analysis (addresses missing requirement)
        self.business_conn.execute("""
            CREATE TABLE IF NOT EXISTS cumulative_retention_analysis (
                cohort_month TEXT,
                retention_window_months INTEGER,  -- 3, 12, 18
                cohort_size INTEGER,
                active_customers INTEGER,
                cumulative_retention_rate REAL,
                avg_purchase_frequency REAL,
                total_revenue REAL,
                avg_customer_value REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (cohort_month, retention_window_months)
            )
        """)
        
        # Customer lifetime value analysis
        self.business_conn.execute("""
            CREATE TABLE IF NOT EXISTS customer_ltv_analysis (
                customer_id INTEGER PRIMARY KEY,
                acquisition_cohort TEXT,
                customer_segment TEXT,
                total_orders INTEGER,
                total_spent REAL,
                avg_order_value REAL,
                days_active INTEGER,
                predicted_ltv_score INTEGER,
                churn_risk_score REAL
            )
        """)
        
        # Customer Segmentation (RFM Analysis)
        self.business_conn.execute("""
            CREATE TABLE IF NOT EXISTS customer_segmentation (
                customer_id INTEGER PRIMARY KEY,
                recency_score INTEGER,      -- 1-5 scale
                frequency_score INTEGER,    -- 1-5 scale  
                monetary_score INTEGER,     -- 1-5 scale
                rfm_segment TEXT,          -- e.g., "Champions", "At Risk", etc.
                segment_description TEXT,
                recommended_strategy TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Campaign targeting table
        self.business_conn.execute("""
            CREATE TABLE IF NOT EXISTS campaign_targets (
                customer_id INTEGER,
                campaign_type TEXT,
                priority_level INTEGER,
                estimated_value REAL,
                days_since_last_order INTEGER,
                recommended_action TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (customer_id, campaign_type)
            )
        """)
        
        # Business insights summary
        self.business_conn.execute("""
            CREATE TABLE IF NOT EXISTS business_insights (
                insight_id TEXT PRIMARY KEY,
                insight_type TEXT,
                insight_title TEXT,
                insight_description TEXT,
                metric_value REAL,
                recommendation TEXT,
                priority_level INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Customer lifecycle snapshot (daily headcount by stage)
        self.business_conn.execute("""
            CREATE TABLE IF NOT EXISTS customer_lifecycle_snapshot (
                snapshot_date DATE,
                lifecycle_stage TEXT,              -- New, Active, At Risk, Inactive
                customers INTEGER,
                share_of_base REAL,                -- customers / total_customers that day
                avg_days_since_last_order REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (snapshot_date, lifecycle_stage)
            )
        """)
        
        # Seasonal Trends Analysis
        self.business_conn.execute("""
            CREATE TABLE IF NOT EXISTS seasonal_trends (
                period_type TEXT,          -- 'monthly', 'quarterly'
                period_value TEXT,         -- '01' for Jan, 'Q1' for Q1
                avg_sales REAL,
                avg_orders INTEGER,
                avg_customers INTEGER,
                seasonal_index REAL,       -- compared to average
                trend_direction TEXT,      -- 'growing', 'declining', 'stable'
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (period_type, period_value)
            )
        """)
        
        self.business_conn.commit()
        self.logger.info("Business analysis schema created successfully")
        
    def build_monthly_metrics(self) -> str:
        """Build monthly aggregated metrics"""
        
        run_id = self.orchestrator.log_pipeline_run('BUSINESS', 'monthly_metrics', 'STARTED')
        
        try:
            # Clear existing data #Must update to handle new - incoming data
            self.business_conn.execute("DELETE FROM monthly_metrics")
            
            # Build monthly metrics from warehouse
            self.business_conn.execute("""
                INSERT INTO monthly_metrics (
                    period_month, total_sales, avg_order_value, total_transactions,
                    total_orders, unique_customers, purchase_frequency
                )
                SELECT 
                    d.year || '-' || printf('%02d', d.month) as period_month,
                    SUM(f.sales_amount) as total_sales,
                    AVG(f.sales_amount) as avg_order_value,
                    COUNT(*) as total_transactions,
                    COUNT(DISTINCT f.order_id) as total_orders,
                    COUNT(DISTINCT f.customer_id) as unique_customers,
                    ROUND(CAST(COUNT(DISTINCT f.order_id) AS FLOAT) / COUNT(DISTINCT f.customer_id), 2) as purchase_frequency
                FROM warehouse.fact_sales f
                JOIN warehouse.dim_date d ON f.date_id = d.date_id
                GROUP BY d.year, d.month
                ORDER BY d.year, d.month
            """)
            
            # Get row count
            cursor = self.business_conn.execute("SELECT COUNT(*) FROM monthly_metrics")
            row_count = cursor.fetchone()[0]
            
            self.orchestrator.log_pipeline_run('BUSINESS', 'monthly_metrics', 'SUCCESS', row_count)
            
            # Run data quality checks
            self._run_monthly_metrics_quality_checks(run_id)
            
            self.business_conn.commit()
            self.logger.info(f"Monthly metrics built successfully with {row_count} rows")
            
            return run_id
            
        except Exception as e:
            self.orchestrator.log_pipeline_run('BUSINESS', 'monthly_metrics', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to build monthly metrics: {str(e)}")
            raise
            
    def build_cohort_analysis(self) -> str:
        """Build customer cohort analysis"""
        
        run_id = self.orchestrator.log_pipeline_run('BUSINESS', 'cohort_analysis', 'STARTED')
        
        try:
            # Clear existing data
            self.business_conn.execute("DELETE FROM cohort_analysis")
            
            # Build cohort analysis from warehouse
            self.business_conn.execute("""
                INSERT INTO cohort_analysis (
                    cohort_month, activity_month, months_since_acquisition,
                    cohort_size, active_customers, retention_rate_percent,
                    total_sales, avg_order_value
                )
                WITH cohort_sizes AS (
                    SELECT 
                        first_order_cohort_month as cohort_month,
                        COUNT(DISTINCT customer_id) as cohort_size
                    FROM warehouse.dim_customer
                    GROUP BY first_order_cohort_month
                ),
                monthly_activity AS (
                    SELECT 
                        c.first_order_cohort_month as cohort_month,
                        d.year || '-' || printf('%02d', d.month) as activity_month,
                        (d.year - c.first_order_cohort_year) * 12 + 
                        (d.month - CAST(substr(c.first_order_cohort_month, 6, 2) AS INTEGER)) as months_since_acquisition,
                        COUNT(DISTINCT f.customer_id) as active_customers,
                        SUM(f.sales_amount) as total_sales,
                        AVG(f.sales_amount) as avg_order_value
                    FROM warehouse.fact_sales f
                    JOIN warehouse.dim_date d ON f.date_id = d.date_id
                    JOIN warehouse.dim_customer c ON f.customer_id = c.customer_id
                    GROUP BY c.first_order_cohort_month, d.year, d.month
                )
                SELECT 
                    ma.cohort_month,
                    ma.activity_month,
                    ma.months_since_acquisition,
                    cs.cohort_size,
                    ma.active_customers,
                    ROUND(CAST(ma.active_customers AS FLOAT) / cs.cohort_size * 100, 2) as retention_rate_percent,
                    ma.total_sales,
                    ma.avg_order_value
                FROM monthly_activity ma
                JOIN cohort_sizes cs ON ma.cohort_month = cs.cohort_month
                WHERE ma.months_since_acquisition >= 0
                ORDER BY ma.cohort_month, ma.months_since_acquisition
            """)
            
            # Get row count
            cursor = self.business_conn.execute("SELECT COUNT(*) FROM cohort_analysis")
            row_count = cursor.fetchone()[0]
            
            self.orchestrator.log_pipeline_run('BUSINESS', 'cohort_analysis', 'SUCCESS', row_count)
            
            # Run data quality checks
            self._run_cohort_analysis_quality_checks(run_id)
            
            self.business_conn.commit()
            self.logger.info(f"Cohort analysis built successfully with {row_count} rows")
            
            return run_id
            
        except Exception as e:
            self.orchestrator.log_pipeline_run('BUSINESS', 'cohort_analysis', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to build cohort analysis: {str(e)}")
            raise

    def build_cumulative_retention_analysis(self) -> str:
        """Build cumulative retention analysis for 3, 12, and 18 month windows"""
        
        run_id = self.orchestrator.log_pipeline_run('BUSINESS', 'cumulative_retention_analysis', 'STARTED')
        
        try:
            # Clear existing data
            self.business_conn.execute("DELETE FROM cumulative_retention_analysis")
            
            # Build cumulative retention for each window
            for window_months in [3, 12, 18]:
                self.business_conn.execute("""
                    INSERT INTO cumulative_retention_analysis (
                        cohort_month, retention_window_months, cohort_size, 
                        active_customers, cumulative_retention_rate, 
                        avg_purchase_frequency, total_revenue, avg_customer_value
                    )
                    WITH cohort_sizes AS (
                        SELECT 
                            first_order_cohort_month as cohort_month,
                            COUNT(DISTINCT customer_id) as cohort_size
                        FROM warehouse.dim_customer
                        WHERE first_order_cohort_month IS NOT NULL
                        GROUP BY first_order_cohort_month
                    ),
                    cohort_activity AS (
                        SELECT 
                            c.first_order_cohort_month as cohort_month,
                            COUNT(DISTINCT f.customer_id) as active_customers,
                            COUNT(DISTINCT f.order_id) as total_orders,
                            SUM(f.sales_amount) as total_revenue
                        FROM warehouse.fact_sales f
                        JOIN warehouse.dim_date d ON f.date_id = d.date_id
                        JOIN warehouse.dim_customer c ON f.customer_id = c.customer_id
                        WHERE (d.year - c.first_order_cohort_year) * 12 + 
                              (d.month - CAST(substr(c.first_order_cohort_month, 6, 2) AS INTEGER)) 
                              BETWEEN 0 AND ?
                        GROUP BY c.first_order_cohort_month
                    )
                    SELECT 
                        ca.cohort_month,
                        ? as retention_window_months,
                        cs.cohort_size,
                        ca.active_customers,
                        ROUND(CAST(ca.active_customers AS FLOAT) / cs.cohort_size * 100, 2) as cumulative_retention_rate,
                        ROUND(CAST(ca.total_orders AS FLOAT) / ca.active_customers, 2) as avg_purchase_frequency,
                        ca.total_revenue,
                        ROUND(ca.total_revenue / ca.active_customers, 2) as avg_customer_value
                    FROM cohort_activity ca
                    JOIN cohort_sizes cs ON ca.cohort_month = cs.cohort_month
                    WHERE cs.cohort_size >= 10 
                    ORDER BY ca.cohort_month
                """, (window_months, window_months))
            
            # Get row count
            cursor = self.business_conn.execute("SELECT COUNT(*) FROM cumulative_retention_analysis")
            row_count = cursor.fetchone()[0]
            
            self.business_conn.commit()
            
            # Run data quality checks
            self._run_cumulative_retention_quality_checks(run_id)
            
            self.orchestrator.log_pipeline_run('BUSINESS', 'cumulative_retention_analysis', 'SUCCESS', row_count)
            self.logger.info(f"Cumulative retention analysis built with {row_count} rows")
            
            return run_id
            
        except Exception as e:
            self.orchestrator.log_pipeline_run('BUSINESS', 'cumulative_retention_analysis', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to build cumulative retention analysis: {str(e)}")
            raise
            
    def build_customer_ltv_analysis(self) -> str:
        """Build customer lifetime value analysis"""
        
        run_id = self.orchestrator.log_pipeline_run('BUSINESS', 'customer_ltv_analysis', 'STARTED')
        
        try:
            # Clear existing data
            self.business_conn.execute("DELETE FROM customer_ltv_analysis")
            
            # Build LTV analysis from warehouse
            self.business_conn.execute("""
                INSERT INTO customer_ltv_analysis (
                    customer_id, acquisition_cohort, customer_segment, total_orders,
                    total_spent, avg_order_value, days_active, predicted_ltv_score, churn_risk_score
                )
                WITH customer_order_timing AS (
                    SELECT 
                        c.customer_id,
                        c.first_order_cohort_month as acquisition_cohort,
                        c.customer_segment,
                        c.total_orders,
                        c.total_spent,
                        c.avg_order_value,
                        c.days_since_first_order as days_active,
                        c.days_since_last_order,
                        -- Calculate second purchase timing for LTV prediction
                        CASE 
                            WHEN c.total_orders = 1 THEN NULL
                            ELSE (
                                SELECT MIN(o2.days_since_customer_first_order)
                                FROM warehouse.dim_order o2
                                WHERE o2.customer_id = c.customer_id 
                                AND o2.customer_order_sequence = 2
                            )
                        END as days_to_second_purchase
                    FROM warehouse.dim_customer c
                ),
                ltv_scored AS (
                    SELECT *,
                        -- LTV Prediction Score (1-5 scale)
                        CASE 
                            WHEN total_orders = 1 THEN 1
                            WHEN days_to_second_purchase IS NULL THEN 1
                            WHEN days_to_second_purchase <= 7 THEN 5
                            WHEN days_to_second_purchase <= 14 THEN 4
                            WHEN days_to_second_purchase <= 30 THEN 3
                            WHEN days_to_second_purchase <= 60 THEN 2
                            ELSE 1
                        END as predicted_ltv_score,
                        -- Churn Risk Score (0-1 scale, higher = more risk)
                        CASE 
                            WHEN days_since_last_order <= 30 THEN 0.1
                            WHEN days_since_last_order <= 60 THEN 0.3
                            WHEN days_since_last_order <= 90 THEN 0.5
                            WHEN days_since_last_order <= 180 THEN 0.7
                            ELSE 0.9
                        END as churn_risk_score
                    FROM customer_order_timing
                )
                SELECT 
                    customer_id, acquisition_cohort, customer_segment, total_orders,
                    total_spent, avg_order_value, days_active, predicted_ltv_score, churn_risk_score
                FROM ltv_scored
            """)
            
            # Get row count
            cursor = self.business_conn.execute("SELECT COUNT(*) FROM customer_ltv_analysis")
            row_count = cursor.fetchone()[0]
            
            self.orchestrator.log_pipeline_run('BUSINESS', 'customer_ltv_analysis', 'SUCCESS', row_count)
            
            # Run data quality checks
            self._run_ltv_analysis_quality_checks(run_id)
            
            self.business_conn.commit()
            self.logger.info(f"Customer LTV analysis built successfully with {row_count} rows")
            
            return run_id
            
        except Exception as e:
            self.orchestrator.log_pipeline_run('BUSINESS', 'customer_ltv_analysis', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to build customer LTV analysis: {str(e)}")
            raise

    def build_customer_segmentation(self) -> str:
        """Build RFM customer segmentation analysis"""
        
        run_id = self.orchestrator.log_pipeline_run('BUSINESS', 'customer_segmentation', 'STARTED')
        
        try:
            # Clear existing data
            self.business_conn.execute("DELETE FROM customer_segmentation")
            
            # Build RFM segmentation
            self.business_conn.execute("""
                INSERT INTO customer_segmentation (
                    customer_id, recency_score, frequency_score, monetary_score,
                    rfm_segment, segment_description, recommended_strategy
                )
                WITH dataset_dates AS (
                    SELECT MAX(full_date) as max_date FROM warehouse.dim_date
                ),
                customer_recency AS (
                    SELECT 
                        c.customer_id,
                        c.total_orders,
                        c.total_spent,
                        -- Calculate recency relative to dataset max date, not current date
                        julianday(dd.max_date) - julianday(c.last_order_date) as days_since_last_order_relative
                    FROM warehouse.dim_customer c
                    CROSS JOIN dataset_dates dd
                ),
                rfm_scores AS (
                    SELECT 
                        customer_id,
                        -- Recency Score (1-5, 5 = most recent) - relative to dataset
                        CASE 
                            WHEN days_since_last_order_relative <= 30 THEN 5
                            WHEN days_since_last_order_relative <= 60 THEN 4
                            WHEN days_since_last_order_relative <= 90 THEN 3
                            WHEN days_since_last_order_relative <= 180 THEN 2
                            ELSE 1
                        END as recency_score,
                        -- Frequency Score (1-5, 5 = highest frequency)
                        CASE 
                            WHEN total_orders >= 10 THEN 5
                            WHEN total_orders >= 5 THEN 4
                            WHEN total_orders >= 3 THEN 3
                            WHEN total_orders >= 2 THEN 2
                            ELSE 1
                        END as frequency_score,
                        -- Monetary Score (1-5, 5 = highest value)
                        NTILE(5) OVER (ORDER BY total_spent) as monetary_score
                    FROM customer_recency
                ),
                segmented AS (
                    SELECT *,
                    CASE 
                        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
                        WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
                        WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'New Customers'
                        WHEN recency_score <= 3 AND frequency_score >= 3 THEN 'At Risk'  -- Changed from <= 2
                        WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Cannot Lose Them'
                        WHEN recency_score <= 2 THEN 'Lost Customers'  -- Changed from <= 1
                        ELSE 'Others'
                    END as rfm_segment
                    FROM rfm_scores
                )
                SELECT 
                    customer_id, recency_score, frequency_score, monetary_score, rfm_segment,
                    CASE rfm_segment
                        WHEN 'Champions' THEN 'Best customers who bought recently and frequently'
                        WHEN 'Loyal Customers' THEN 'Regular customers with good value'
                        WHEN 'New Customers' THEN 'Recent customers with potential'
                        WHEN 'At Risk' THEN 'Good customers who haven''t purchased recently'
                        WHEN 'Cannot Lose Them' THEN 'High-value customers at risk of churning'
                        WHEN 'Lost Customers' THEN 'Customers who haven''t purchased in long time'
                        ELSE 'General customer segment'
                    END as segment_description,
                    CASE rfm_segment
                        WHEN 'Champions' THEN 'Upsell premium products, ask for reviews'
                        WHEN 'Loyal Customers' THEN 'Recommend related products, loyalty programs'
                        WHEN 'New Customers' THEN 'Onboarding campaigns, product education'
                        WHEN 'At Risk' THEN 'Reactivation campaigns with discounts'
                        WHEN 'Cannot Lose Them' THEN 'Immediate intervention, VIP treatment'
                        WHEN 'Lost Customers' THEN 'Win-back campaigns with strong incentives'
                        ELSE 'Standard marketing approach'
                    END as recommended_strategy
                FROM segmented
            """)
            
            # Get row count
            cursor = self.business_conn.execute("SELECT COUNT(*) FROM customer_segmentation")
            row_count = cursor.fetchone()[0]
            
            self.business_conn.commit()
            self.orchestrator.log_pipeline_run('BUSINESS', 'customer_segmentation', 'SUCCESS', row_count)
            self.logger.info(f"Customer segmentation built with {row_count} rows")
            
            return run_id
            
        except Exception as e:
            self.orchestrator.log_pipeline_run('BUSINESS', 'customer_segmentation', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to build customer segmentation: {str(e)}")
            raise

    def build_seasonal_trends(self) -> str:
        """Build seasonal trends analysis"""
        
        run_id = self.orchestrator.log_pipeline_run('BUSINESS', 'seasonal_trends', 'STARTED')
        
        try:
            # Clear existing data
            self.business_conn.execute("DELETE FROM seasonal_trends")
            
            # Monthly seasonal trends
            self.business_conn.execute("""
                INSERT INTO seasonal_trends (
                    period_type, period_value, avg_sales, avg_orders, 
                    avg_customers, seasonal_index, trend_direction
                )
                WITH monthly_avg AS (
                    SELECT AVG(total_sales) as overall_avg_sales FROM monthly_metrics
                ),
                monthly_trends AS (
                    SELECT 
                        'monthly' as period_type,
                        substr(period_month, 6, 2) as period_value,
                        AVG(total_sales) as avg_sales,
                        AVG(total_orders) as avg_orders,
                        AVG(unique_customers) as avg_customers
                    FROM monthly_metrics
                    GROUP BY substr(period_month, 6, 2)
                )
                SELECT 
                    mt.period_type,
                    mt.period_value,
                    ROUND(mt.avg_sales, 2) as avg_sales,
                    ROUND(mt.avg_orders, 0) as avg_orders,
                    ROUND(mt.avg_customers, 0) as avg_customers,
                    ROUND(mt.avg_sales / ma.overall_avg_sales, 3) as seasonal_index,
                    CASE 
                        WHEN mt.avg_sales / ma.overall_avg_sales > 1.1 THEN 'strong_positive'
                        WHEN mt.avg_sales / ma.overall_avg_sales > 1.05 THEN 'positive'
                        WHEN mt.avg_sales / ma.overall_avg_sales < 0.9 THEN 'strong_negative'
                        WHEN mt.avg_sales / ma.overall_avg_sales < 0.95 THEN 'negative'
                        ELSE 'stable'
                    END as trend_direction
                FROM monthly_trends mt
                CROSS JOIN monthly_avg ma
            """)
            
            # Get row count
            cursor = self.business_conn.execute("SELECT COUNT(*) FROM seasonal_trends")
            row_count = cursor.fetchone()[0]
            
            self.business_conn.commit()
            self.orchestrator.log_pipeline_run('BUSINESS', 'seasonal_trends', 'SUCCESS', row_count)
            self.logger.info(f"Seasonal trends built with {row_count} rows")
            
            return run_id
            
        except Exception as e:
            self.orchestrator.log_pipeline_run('BUSINESS', 'seasonal_trends', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to build seasonal trends: {str(e)}")
            raise
            
    def build_campaign_targets(self) -> str:
        """Build campaign targeting recommendations"""
        
        run_id = self.orchestrator.log_pipeline_run('BUSINESS', 'campaign_targets', 'STARTED')
        
        try:
            # Clear existing data
            self.business_conn.execute("DELETE FROM campaign_targets")
            
            # Build campaign targets from customer analysis
            self.business_conn.execute("""
                INSERT INTO campaign_targets (
                    customer_id, campaign_type, priority_level, estimated_value,
                    days_since_last_order, recommended_action
                )
                WITH customer_campaign_data AS (
                    SELECT 
                        c.customer_id,
                        c.days_since_last_order,
                        c.total_spent,
                        c.customer_segment,
                        ltv.churn_risk_score,
                        ltv.predicted_ltv_score
                    FROM warehouse.dim_customer c
                    JOIN customer_ltv_analysis ltv ON c.customer_id = ltv.customer_id
                    WHERE c.total_orders = 1  -- Focus on one-time buyers
                ),
                campaign_logic AS (
                    SELECT 
                        customer_id,
                        days_since_last_order,
                        total_spent as estimated_value,
                        churn_risk_score,
                        predicted_ltv_score,
                        CASE 
                            WHEN days_since_last_order BETWEEN 14 AND 30 THEN 'Early Engagement'
                            WHEN days_since_last_order BETWEEN 31 AND 60 THEN 'Re-activation'
                            WHEN days_since_last_order BETWEEN 61 AND 90 THEN 'Win-back'
                            WHEN days_since_last_order BETWEEN 91 AND 180 THEN 'Final Push'
                            WHEN days_since_last_order > 180 THEN 'Long-term Win-back'
                            ELSE NULL
                        END as campaign_type,
                        CASE 
                            WHEN days_since_last_order BETWEEN 14 AND 30 THEN 1
                            WHEN days_since_last_order BETWEEN 31 AND 60 THEN 2
                            WHEN days_since_last_order BETWEEN 61 AND 90 THEN 3
                            WHEN days_since_last_order BETWEEN 91 AND 180 THEN 4
                            WHEN days_since_last_order > 180 THEN 5
                            ELSE 99
                        END as priority_level,
                        CASE 
                            WHEN days_since_last_order BETWEEN 14 AND 30 THEN 'Send personalized product recommendations'
                            WHEN days_since_last_order BETWEEN 31 AND 60 THEN 'Offer 15% discount + free shipping'
                            WHEN days_since_last_order BETWEEN 61 AND 90 THEN 'Limited-time 20% discount offer'
                            WHEN days_since_last_order BETWEEN 91 AND 180 THEN 'Win-back campaign with survey'
                            WHEN days_since_last_order > 180 THEN 'Final 25% discount attempt'
                            ELSE 'Monitor for future campaigns'
                        END as recommended_action
                    FROM customer_campaign_data
                )
                SELECT 
                    customer_id, campaign_type, priority_level, estimated_value,
                    days_since_last_order, recommended_action
                FROM campaign_logic
                WHERE campaign_type IS NOT NULL
                ORDER BY priority_level, estimated_value DESC
            """)
            
            # Get row count
            cursor = self.business_conn.execute("SELECT COUNT(*) FROM campaign_targets")
            row_count = cursor.fetchone()[0]
            
            self.orchestrator.log_pipeline_run('BUSINESS', 'campaign_targets', 'SUCCESS', row_count)
            
            # Run data quality checks
            self._run_campaign_targets_quality_checks(run_id)
            
            self.business_conn.commit()
            self.logger.info(f"Campaign targets built successfully with {row_count} rows")
            
            return run_id
            
        except Exception as e:
            self.orchestrator.log_pipeline_run('BUSINESS', 'campaign_targets', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to build campaign targets: {str(e)}")
            raise

    def build_customer_lifecycle_snapshot(self) -> str:
        """
        Build a daily snapshot of customers by lifecycle stage.

        Stages:
        - 'New'      : First-time buyers still within the early window (total_orders = 1 AND days_since_first_order <= 30)
        - 'Active'   : days_since_last_order <= 30
        - 'At Risk'  : 31 <= days_since_last_order <= 90
        - 'Inactive' : days_since_last_order > 90

        Notes:
        - 'New' overrides status for first 30 days after first purchase to give marketing a clear onboarding cohort.
        - Snapshot date is taken as the max known calendar date in warehouse.dim_date (so it aligns with the latest loaded data).
        """

        run_id = self.orchestrator.log_pipeline_run('BUSINESS', 'customer_lifecycle_snapshot', 'STARTED')

        try:
            # Determine the snapshot date from warehouse calendar (latest loaded date)
            cursor = self.business_conn.execute("""
                SELECT MAX(full_date) FROM warehouse.dim_date
            """)
            snapshot_date = cursor.fetchone()[0]

            if snapshot_date is None:
                # Nothing to snapshot yet
                self.orchestrator.log_pipeline_run('BUSINESS', 'customer_lifecycle_snapshot', 'SUCCESS', 0)
                self.logger.info("No dates available in warehouse.dim_date; lifecycle snapshot skipped.")
                return run_id

            # We rebuild the snapshot for the latest date (idempotent upsert)
            self.business_conn.execute("""
                DELETE FROM customer_lifecycle_snapshot
                WHERE snapshot_date = ?
            """, (snapshot_date,))

            # Materialize lifecycle stage per rules. We compute a stage label per customer,
            # then roll up counts and shares.
            self.business_conn.execute("""
                INSERT INTO customer_lifecycle_snapshot (
                    snapshot_date, lifecycle_stage, customers, share_of_base, avg_days_since_last_order
                )
                WITH base AS (
                    SELECT
                        c.customer_id,
                        c.total_orders,
                        c.days_since_first_order,
                        c.days_since_last_order,
                        CASE
                            WHEN c.total_orders = 1 AND c.days_since_first_order IS NOT NULL AND c.days_since_first_order <= 30
                                THEN 'New'
                            WHEN c.days_since_last_order IS NOT NULL AND c.days_since_last_order <= 30
                                THEN 'Active'
                            WHEN c.days_since_last_order BETWEEN 31 AND 90
                                THEN 'At Risk'
                            WHEN c.days_since_last_order > 90
                                THEN 'Inactive'
                            ELSE 'Inactive'  -- default if days_since_* is missing
                        END AS lifecycle_stage
                    FROM warehouse.dim_customer c
                ),
                totals AS (
                    SELECT COUNT(*) AS total_customers FROM base
                ),
                rolled AS (
                    SELECT
                        ? AS snapshot_date,
                        lifecycle_stage,
                        COUNT(*) AS customers,
                        AVG(CAST(days_since_last_order AS REAL)) AS avg_days_since_last_order
                    FROM base
                    GROUP BY lifecycle_stage
                )
                SELECT
                    r.snapshot_date,
                    r.lifecycle_stage,
                    r.customers,
                    ROUND(r.customers * 1.0 / t.total_customers, 4) AS share_of_base,
                    ROUND(COALESCE(r.avg_days_since_last_order, 0), 2) AS avg_days_since_last_order
                FROM rolled r
                CROSS JOIN totals t
                ORDER BY
                    CASE r.lifecycle_stage
                        WHEN 'New' THEN 1
                        WHEN 'Active' THEN 2
                        WHEN 'At Risk' THEN 3
                        WHEN 'Inactive' THEN 4
                        ELSE 99
                    END
            """, (snapshot_date,))

            # Row count inserted for this snapshot date
            row_count = self.business_conn.execute("""
                SELECT COUNT(*) FROM customer_lifecycle_snapshot
                WHERE snapshot_date = ?
            """, (snapshot_date,)).fetchone()[0]

            # Optional DQ checks
            self._run_lifecycle_snapshot_quality_checks(run_id, snapshot_date)

            self.business_conn.commit()
            self.orchestrator.log_pipeline_run('BUSINESS', 'customer_lifecycle_snapshot', 'SUCCESS', row_count)
            self.logger.info(f"Customer lifecycle snapshot built for {snapshot_date} with {row_count} rows.")
            return run_id

        except Exception as e:
            self.orchestrator.log_pipeline_run('BUSINESS', 'customer_lifecycle_snapshot', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to build customer lifecycle snapshot: {str(e)}")
            raise

    def generate_business_insights(self) -> str:
        """Generate business insights and recommendations - ENHANCED"""
        
        run_id = self.orchestrator.log_pipeline_run('BUSINESS', 'business_insights', 'STARTED')
        
        try:
            # Clear existing insights
            self.business_conn.execute("DELETE FROM business_insights")
            
            insights = []
            
            # Insight 1: Overall conversion rate
            cursor = self.business_conn.execute("""
                SELECT 
                    COUNT(*) as total_customers,
                    SUM(CASE WHEN total_orders = 1 THEN 1 ELSE 0 END) as one_time_buyers,
                    ROUND(SUM(CASE WHEN total_orders = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as one_time_rate
                FROM customer_ltv_analysis
            """)
            result = cursor.fetchone()
            
            insights.append({
                'insight_id': 'CONV_001',
                'insight_type': 'CONVERSION',
                'insight_title': 'Customer Conversion Rate',
                'insight_description': f'Out of {result[0]:,} customers, {result[1]:,} ({result[2]}%) are one-time buyers',
                'metric_value': result[2],
                'recommendation': 'Implement automated email sequences to convert one-time buyers',
                'priority_level': 1
            })
            
            # Insight 2: Best converting cohort
            cursor = self.business_conn.execute("""
                SELECT 
                    cohort_month,
                    AVG(retention_rate_percent) as avg_retention
                FROM cohort_analysis
                WHERE months_since_acquisition = 1
                GROUP BY cohort_month
                ORDER BY avg_retention DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
            
            if result:
                insights.append({
                    'insight_id': 'COH_001',
                    'insight_type': 'COHORT',
                    'insight_title': 'Best Performing Cohort',
                    'insight_description': f'Cohort {result[0]} has the highest month-1 retention at {result[1]:.1f}%',
                    'metric_value': result[1],
                    'recommendation': 'Analyze and replicate the acquisition strategies used for this cohort',
                    'priority_level': 2
                })
            
            # NEW: Cumulative retention insights
            cursor = self.business_conn.execute("""
                SELECT 
                    cohort_month,
                    AVG(CASE WHEN retention_window_months = 3 THEN cumulative_retention_rate END) as retention_3m,
                    AVG(CASE WHEN retention_window_months = 12 THEN cumulative_retention_rate END) as retention_12m,
                    AVG(CASE WHEN retention_window_months = 18 THEN cumulative_retention_rate END) as retention_18m
                FROM cumulative_retention_analysis
                GROUP BY cohort_month
                ORDER BY retention_12m DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
            
            if result and result[2]:  # Check if 12m retention exists
                insights.append({
                    'insight_id': 'RET_001',
                    'insight_type': 'RETENTION',
                    'insight_title': 'Best Retention Cohort Performance',
                    'insight_description': f'Cohort {result[0]} shows strongest retention: 3m={result[1]:.1f}%, 12m={result[2]:.1f}%, 18m={result[3]:.1f}%',
                    'metric_value': result[2],
                    'recommendation': 'Analyze acquisition channels and onboarding for this cohort to replicate success',
                    'priority_level': 1
                })
            
            # Insight 3: High-value at-risk customers
            cursor = self.business_conn.execute("""
                SELECT 
                    COUNT(*) as high_value_at_risk,
                    SUM(total_spent) as revenue_at_risk
                FROM customer_ltv_analysis
                WHERE predicted_ltv_score >= 4 AND churn_risk_score >= 0.5
            """)
            result = cursor.fetchone()
            
            insights.append({
                'insight_id': 'RISK_001',
                'insight_type': 'CHURN_RISK',
                'insight_title': 'High-Value Customers at Risk',
                'insight_description': f'{result[0]} high-LTV customers are at risk, representing ${result[1]:,.2f} in potential lost revenue',
                'metric_value': result[0],
                'recommendation': 'Immediate intervention with personalized offers for high-LTV at-risk customers',
                'priority_level': 1
            })
            
            # NEW: Segmentation insights
            cursor = self.business_conn.execute("""
                SELECT 
                    rfm_segment,
                    COUNT(*) as segment_size,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customer_segmentation), 1) as segment_percent
                FROM customer_segmentation
                WHERE rfm_segment IN ('Champions', 'At Risk', 'Cannot Lose Them')
                ORDER BY segment_size DESC
            """)
            
            for row in cursor.fetchall():
                segment, size, percent = row
                insights.append({
                    'insight_id': f'SEG_{segment[:3].upper()}',
                    'insight_type': 'SEGMENTATION',
                    'insight_title': f'{segment} Segment Analysis',
                    'insight_description': f'{size:,} customers ({percent}%) in {segment} segment',
                    'metric_value': percent,
                    'recommendation': f'Focus on {segment.lower()} with targeted campaigns',
                    'priority_level': 2 if segment == 'Champions' else 1
                })
            
            # Insight 4: Campaign opportunity
            cursor = self.business_conn.execute("""
                SELECT 
                    campaign_type,
                    COUNT(*) as target_count,
                    SUM(estimated_value) as total_opportunity
                FROM campaign_targets
                WHERE priority_level <= 2
                GROUP BY campaign_type
                ORDER BY total_opportunity DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
            
            if result:
                insights.append({
                    'insight_id': 'CAMP_001',
                    'insight_type': 'CAMPAIGN',
                    'insight_title': 'Top Campaign Opportunity',
                    'insight_description': f'{result[1]} customers ready for {result[0]} campaigns, ${result[2]:,.2f} potential value',
                    'metric_value': result[1],
                    'recommendation': f'Launch {result[0]} campaign immediately',
                    'priority_level': 1
                })
                
            # NEW: Seasonal insights
            cursor = self.business_conn.execute("""
                SELECT 
                    period_value as month,
                    seasonal_index,
                    trend_direction
                FROM seasonal_trends
                WHERE period_type = 'monthly'
                ORDER BY seasonal_index DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
            
            if result:
                month_names = {
                    '01': 'January', '02': 'February', '03': 'March', '04': 'April',
                    '05': 'May', '06': 'June', '07': 'July', '08': 'August',
                    '09': 'September', '10': 'October', '11': 'November', '12': 'December'
                }
                month_name = month_names.get(result[0], result[0])
                
                insights.append({
                    'insight_id': 'SEAS_001',
                    'insight_type': 'SEASONAL',
                    'insight_title': 'Peak Seasonal Performance',
                    'insight_description': f'{month_name} is peak month with {result[1]:.2f}x average performance ({result[2]} trend)',
                    'metric_value': result[1],
                    'recommendation': f'Increase marketing spend and inventory for {month_name}',
                    'priority_level': 2
                })
            
            # Insert insights into table
            for insight in insights:
                self.business_conn.execute("""
                    INSERT INTO business_insights 
                    (insight_id, insight_type, insight_title, insight_description, 
                     metric_value, recommendation, priority_level)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    insight['insight_id'], insight['insight_type'], insight['insight_title'],
                    insight['insight_description'], insight['metric_value'], 
                    insight['recommendation'], insight['priority_level']
                ))
            
            self.orchestrator.log_pipeline_run('BUSINESS', 'business_insights', 'SUCCESS', len(insights))
            
            self.business_conn.commit()
            self.logger.info(f"Generated {len(insights)} business insights")
            
            return run_id
            
        except Exception as e:
            self.orchestrator.log_pipeline_run('BUSINESS', 'business_insights', 'FAILED', 0, str(e))
            self.logger.error(f"Failed to generate business insights: {str(e)}")
            raise
            
    def _run_monthly_metrics_quality_checks(self, run_id: str):
        """Run data quality checks on monthly metrics"""
        
        # Check no negative sales
        try:
            cursor = self.business_conn.execute("SELECT COUNT(*) FROM monthly_metrics WHERE total_sales < 0")
            negative_sales = cursor.fetchone()[0]
            
            status = "PASSED" if negative_sales == 0 else "FAILED"
            
            self.orchestrator.log_data_quality_check(
                run_id, 'monthly_metrics', 'BUSINESS_RULE', 'positive_sales_check',
                "0", str(negative_sales), status
            )
            
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, 'monthly_metrics', 'BUSINESS_RULE', 'positive_sales_check',
                "0", "ERROR", "FAILED", str(e)
            )
            
    def _run_cohort_analysis_quality_checks(self, run_id: str):
        """Run data quality checks on cohort analysis"""
        
        # Check retention rates are <= 100%
        try:
            cursor = self.business_conn.execute("SELECT COUNT(*) FROM cohort_analysis WHERE retention_rate_percent > 100")
            invalid_retention = cursor.fetchone()[0]
            
            status = "PASSED" if invalid_retention == 0 else "FAILED"
            
            self.orchestrator.log_data_quality_check(
                run_id, 'cohort_analysis', 'BUSINESS_RULE', 'valid_retention_rate_check',
                "0", str(invalid_retention), status
            )
            
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, 'cohort_analysis', 'BUSINESS_RULE', 'valid_retention_rate_check',
                "0", "ERROR", "FAILED", str(e)
            )

    def _run_cumulative_retention_quality_checks(self, run_id: str):
        """Advanced DQ checks for retention analysis"""
        
        # Check retention rates don't increase over time (logical impossibility)
        try:
            cursor = self.business_conn.execute("""
                SELECT COUNT(*) FROM (
                    SELECT cohort_month,
                        MAX(CASE WHEN retention_window_months = 3 THEN cumulative_retention_rate END) as ret_3m,
                        MAX(CASE WHEN retention_window_months = 12 THEN cumulative_retention_rate END) as ret_12m,
                        MAX(CASE WHEN retention_window_months = 18 THEN cumulative_retention_rate END) as ret_18m
                    FROM cumulative_retention_analysis 
                    GROUP BY cohort_month
                    HAVING ret_12m > ret_3m OR ret_18m > ret_12m
                )
            """)
            invalid_trends = cursor.fetchone()[0]
            
            status = "PASSED" if invalid_trends == 0 else "FAILED"
            
            self.orchestrator.log_data_quality_check(
                run_id, 'cumulative_retention_analysis', 'BUSINESS_RULE', 'retention_trend_logic_check',
                "0", str(invalid_trends), status
            )
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, 'cumulative_retention_analysis', 'BUSINESS_RULE', 'retention_trend_logic_check',
                "0", "ERROR", "FAILED", str(e)
            )
            
    def _run_ltv_analysis_quality_checks(self, run_id: str):
        """Run data quality checks on LTV analysis"""
        
        # Check LTV scores are in valid range (1-5)
        try:
            cursor = self.business_conn.execute("""
                SELECT COUNT(*) FROM customer_ltv_analysis 
                WHERE predicted_ltv_score NOT BETWEEN 1 AND 5
            """)
            invalid_scores = cursor.fetchone()[0]
            
            status = "PASSED" if invalid_scores == 0 else "FAILED"
            
            self.orchestrator.log_data_quality_check(
                run_id, 'customer_ltv_analysis', 'BUSINESS_RULE', 'valid_ltv_score_check',
                "0", str(invalid_scores), status
            )
            
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, 'customer_ltv_analysis', 'BUSINESS_RULE', 'valid_ltv_score_check',
                "0", "ERROR", "FAILED", str(e)
            )
    
    def _run_lifecycle_snapshot_quality_checks(self, run_id: str, snapshot_date: str):
        """Simple DQ checks: share sums to ~1.0 and no negative counts."""

        try:
            # shares should approximately sum to 1 (allow small floating error)
            row = self.business_conn.execute("""
                SELECT ROUND(SUM(share_of_base), 4) FROM customer_lifecycle_snapshot
                WHERE snapshot_date = ?
            """, (snapshot_date,)).fetchone()
            share_sum = row[0] if row and row[0] is not None else 0.0
            status = "PASSED" if 0.99 <= share_sum <= 1.01 else "FAILED"

            self.orchestrator.log_data_quality_check(
                run_id, 'customer_lifecycle_snapshot', 'AGGREGATE', 'share_of_base_sums_to_1',
                "1.0 ± 0.01", str(share_sum), status
            )
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, 'customer_lifecycle_snapshot', 'AGGREGATE', 'share_of_base_sums_to_1',
                "1.0 ± 0.01", "ERROR", "FAILED", str(e)
            )

        try:
            # no negative customers
            row = self.business_conn.execute("""
                SELECT COUNT(*) FROM customer_lifecycle_snapshot
                WHERE snapshot_date = ? AND customers < 0
            """, (snapshot_date,)).fetchone()
            negatives = row[0] if row else 0
            status = "PASSED" if negatives == 0 else "FAILED"

            self.orchestrator.log_data_quality_check(
                run_id, 'customer_lifecycle_snapshot', 'BUSINESS_RULE', 'non_negative_counts',
                "0", str(negatives), status
            )
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, 'customer_lifecycle_snapshot', 'BUSINESS_RULE', 'non_negative_counts',
                "0", "ERROR", "FAILED", str(e)
            )

            
    def _run_campaign_targets_quality_checks(self, run_id: str):
        """Run data quality checks on campaign targets"""
        
        # Check all targets have recommendations
        self.dq_checker.check_null_percentage(
            self.business_conn, 'campaign_targets', 'recommended_action', max_null_pct=0.0, run_id=run_id
        )
        
    def get_business_summary(self) -> dict:
        """Get summary of business analysis layer"""
        
        summary = {}
        
        # Table row counts
        tables = [
            'monthly_metrics', 'cohort_analysis', 'cumulative_retention_analysis',
            'customer_ltv_analysis', 'customer_segmentation', 'seasonal_trends',
            'campaign_targets', 'business_insights', 'customer_lifecycle_snapshot'
        ]
        for table in tables:
            cursor = self.business_conn.execute(f"SELECT COUNT(*) FROM {table}")
            summary[f'{table}_rows'] = cursor.fetchone()[0]
            
        # Key business metrics
        cursor = self.business_conn.execute("""
            SELECT 
                COUNT(*) as active_campaigns,
                SUM(estimated_value) as total_campaign_value
            FROM campaign_targets
            WHERE priority_level <= 3
        """)
        result = cursor.fetchone()
        summary.update({
            'active_campaigns': result[0],
            'total_campaign_value': round(result[1] if result[1] else 0, 2)
        })
        
        # Get latest insights
        cursor = self.business_conn.execute("""
            SELECT COUNT(*) as total_insights,
                   SUM(CASE WHEN priority_level = 1 THEN 1 ELSE 0 END) as high_priority_insights
            FROM business_insights
        """)
        result = cursor.fetchone()
        summary.update({
            'total_insights': result[0],
            'high_priority_insights': result[1]
        })
        
        # Segmentation summary
        cursor = self.business_conn.execute("""
            SELECT 
                COUNT(CASE WHEN rfm_segment = 'Champions' THEN 1 END) as champions,
                COUNT(CASE WHEN rfm_segment IN ('At Risk', 'Cannot Lose Them') THEN 1 END) as at_risk
            FROM customer_segmentation
        """)
        result = cursor.fetchone()
        summary.update({
            'champions_customers': result[0] if result[0] else 0,
            'at_risk_customers': result[1] if result[1] else 0
        })
        
        return summary


def run_business_analysis_pipeline(orchestrator=None):
    """Main function to run the business analysis pipeline - ENHANCED"""
    
    # Use provided orchestrator or initialize new one
    if orchestrator is None:
        orchestrator = DataPipelineOrchestrator()
    
    try:
        # Initialize business analysis layer
        business = BusinessAnalysisLayer(orchestrator)
        
        # Create schema
        business.create_business_schema()
        
        # Core required tables
        business.logger.info("Building monthly metrics...")
        business.build_monthly_metrics()
        
        business.logger.info("Building cohort analysis...")
        business.build_cohort_analysis()
        
        # NEW: Critical missing requirement
        business.logger.info("Building cumulative retention analysis...")
        business.build_cumulative_retention_analysis()
        
        business.logger.info("Building customer LTV analysis...")
        business.build_customer_ltv_analysis()
        
        # Advanced analytics for differentiation
        business.logger.info("Building customer segmentation...")
        business.build_customer_segmentation()
        
        business.logger.info("Building seasonal trends...")
        business.build_seasonal_trends()
        
        business.logger.info("Building customer lifecycle snapshot...")
        business.build_customer_lifecycle_snapshot()
        
        business.logger.info("Building campaign targets...")
        business.build_campaign_targets()
        
        business.logger.info("Generating business insights...")
        business.generate_business_insights()

        
        # Get summary
        summary = business.get_business_summary()
        business.logger.info(f"Business analysis pipeline completed successfully: {summary}")
        
        print("BUSINESS ANALYSIS LAYER SUMMARY:")
        print("=" * 50)
        print(f"Monthly metrics: {summary['monthly_metrics_rows']:,} rows")
        print(f"Cohort analysis: {summary['cohort_analysis_rows']:,} rows")
        print(f"Cumulative retention analysis: {summary['cumulative_retention_analysis_rows']:,} rows")
        print(f"Customer LTV analysis: {summary['customer_ltv_analysis_rows']:,} rows")
        print(f"Customer segmentation: {summary['customer_segmentation_rows']:,} rows")
        print(f"Seasonal trends: {summary['seasonal_trends_rows']:,} rows")
        print(f"Campaign targets: {summary['campaign_targets_rows']:,} rows")
        print(f"Customer lifecycle snapshot: {summary['customer_lifecycle_snapshot_rows']:,} rows")
        print(f"Business insights: {summary['business_insights_rows']:,} rows")
        print(f"\nActionable Intelligence:")
        print(f"Active campaigns ready: {summary['active_campaigns']:,}")
        print(f"Total campaign value: ${summary['total_campaign_value']:,.2f}")
        print(f"High priority insights: {summary['high_priority_insights']:,}")
        print(f"Champions customers: {summary['champions_customers']:,}")
        print(f"At-risk customers: {summary['at_risk_customers']:,}")
        
        # Show top insights
        insights_df = pd.read_sql_query("""
            SELECT insight_title, insight_description, recommendation, priority_level
            FROM business_insights
            ORDER BY priority_level, metric_value DESC
        """, business.business_conn)
        
        if len(insights_df) > 0:
            print(f"\nTop Business Insights:")
            print("-" * 30)
            for _, insight in insights_df.iterrows():
                print(f"• {insight['insight_title']}")
                print(f"  {insight['insight_description']}")
                print(f"  Recommendation: {insight['recommendation']}")
                print()
        
        return orchestrator, summary
        
    except Exception as e:
        orchestrator.logger.error(f"Business analysis pipeline failed: {str(e)}")
        raise


# Example usage
if __name__ == "__main__":
    try:
        orchestrator, summary = run_business_analysis_pipeline()
        print("\nBusiness analysis pipeline completed successfully!")
        
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