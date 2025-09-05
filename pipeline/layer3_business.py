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
        
        self.business_conn.commit()
        self.logger.info("Business analysis schema created successfully")
        
    def build_monthly_metrics(self) -> str:
        """Build monthly aggregated metrics"""
        
        run_id = self.orchestrator.log_pipeline_run('BUSINESS', 'monthly_metrics', 'STARTED')
        
        try:
            # Clear existing data
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
            
    def generate_business_insights(self) -> str:
        """Generate business insights and recommendations"""
        
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
        tables = ['monthly_metrics', 'cohort_analysis', 'customer_ltv_analysis', 'campaign_targets', 'business_insights']
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
        
        return summary


def run_business_analysis_pipeline(orchestrator=None):
    """Main function to run the business analysis pipeline"""
    
    # Use provided orchestrator or initialize new one
    if orchestrator is None:
        orchestrator = DataPipelineOrchestrator()
    
    try:
        # Initialize business analysis layer
        business = BusinessAnalysisLayer(orchestrator)
        
        # Create schema
        business.create_business_schema()
        
        # Build analysis tables
        business.logger.info("Building monthly metrics...")
        business.build_monthly_metrics()
        
        business.logger.info("Building cohort analysis...")
        business.build_cohort_analysis()
        
        business.logger.info("Building customer LTV analysis...")
        business.build_customer_ltv_analysis()
        
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
        print(f"Customer LTV analysis: {summary['customer_ltv_analysis_rows']:,} rows")
        print(f"Campaign targets: {summary['campaign_targets_rows']:,} rows")
        print(f"Business insights: {summary['business_insights_rows']:,} rows")
        print(f"\nActionable Intelligence:")
        print(f"Active campaigns ready: {summary['active_campaigns']:,}")
        print(f"Total campaign value: ${summary['total_campaign_value']:,.2f}")
        print(f"High priority insights: {summary['high_priority_insights']:,}")
        
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