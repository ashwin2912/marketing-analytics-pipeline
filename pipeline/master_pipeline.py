# Master Pipeline Runner
# Orchestrates the complete 3-layer data engineering pipeline

import pandas as pd
import sqlite3
import logging
from datetime import datetime
import sys
import os
import argparse

# Import all pipeline components
from .pipeline_orchestrator import DataPipelineOrchestrator, DataQualityChecker
from .layer1_staging import run_staging_pipeline
from .layer2_warehouse import run_warehouse_pipeline
from .layer3_business import run_business_analysis_pipeline

class MasterPipelineRunner:
    """
    Master pipeline runner that orchestrates all three layers:
    1. Staging (Layer 1): Raw data ingestion and cleaning
    2. Warehouse (Layer 2): Dimensional modeling
    3. Business (Layer 3): Analysis and insights
    """
    
    def __init__(self, config: dict = None):
        self.config = config or {}
        self.orchestrator = None
        self.results = {}
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('./data/master_pipeline.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def run_full_pipeline(self, csv_file_path: str, skip_layers: list = None):
        """
        Run the complete 3-layer pipeline
        
        Args:
            csv_file_path: Path to the input CSV file
            skip_layers: List of layers to skip (e.g., ['staging'] to skip layer 1)
        """
        
        skip_layers = skip_layers or []
        pipeline_start_time = datetime.now()
        
        try:
            self.logger.info("=" * 60)
            self.logger.info("STARTING MASTER DATA PIPELINE")
            self.logger.info("=" * 60)
            
            # Initialize orchestrator
            self.orchestrator = DataPipelineOrchestrator()
            
            # Layer 1: Staging
            if 'staging' not in skip_layers:
                self.logger.info("Starting Layer 1: Staging Pipeline...")
                layer1_start = datetime.now()
                
                try:
                    self.orchestrator, staging_summary = run_staging_pipeline(csv_file_path)
                    self.results['layer1'] = {
                        'status': 'SUCCESS',
                        'duration': (datetime.now() - layer1_start).total_seconds(),
                        'summary': staging_summary
                    }
                    self.logger.info("Layer 1 completed successfully")
                    
                except Exception as e:
                    self.results['layer1'] = {
                        'status': 'FAILED',
                        'duration': (datetime.now() - layer1_start).total_seconds(),
                        'error': str(e)
                    }
                    self.logger.error(f"Layer 1 failed: {str(e)}")
                    raise
            else:
                self.logger.info("Skipping Layer 1: Staging Pipeline")
                # Initialize orchestrator if staging was skipped
                if not self.orchestrator:
                    self.orchestrator = DataPipelineOrchestrator()
                
            # Layer 2: Data Warehouse
            if 'warehouse' not in skip_layers:
                self.logger.info("Starting Layer 2: Data Warehouse Pipeline...")
                layer2_start = datetime.now()
                
                try:
                    # Pass the same orchestrator to maintain connections
                    warehouse_orchestrator, warehouse_summary = run_warehouse_pipeline(self.orchestrator)
                    self.results['layer2'] = {
                        'status': 'SUCCESS',
                        'duration': (datetime.now() - layer2_start).total_seconds(),
                        'summary': warehouse_summary
                    }
                    self.logger.info("Layer 2 completed successfully")
                    
                except Exception as e:
                    self.results['layer2'] = {
                        'status': 'FAILED',
                        'duration': (datetime.now() - layer2_start).total_seconds(),
                        'error': str(e)
                    }
                    self.logger.error(f"Layer 2 failed: {str(e)}")
                    raise
            else:
                self.logger.info("Skipping Layer 2: Data Warehouse Pipeline")
                
            # Layer 3: Business Analysis
            if 'business' not in skip_layers:
                self.logger.info("Starting Layer 3: Business Analysis Pipeline...")
                layer3_start = datetime.now()
                
                try:
                    # Pass the same orchestrator to maintain connections
                    business_orchestrator, business_summary = run_business_analysis_pipeline(self.orchestrator)
                    self.results['layer3'] = {
                        'status': 'SUCCESS',
                        'duration': (datetime.now() - layer3_start).total_seconds(),
                        'summary': business_summary
                    }
                    self.logger.info("Layer 3 completed successfully")
                    
                except Exception as e:
                    self.results['layer3'] = {
                        'status': 'FAILED',
                        'duration': (datetime.now() - layer3_start).total_seconds(),
                        'error': str(e)
                    }
                    self.logger.error(f"Layer 3 failed: {str(e)}")
                    raise
            else:
                self.logger.info("Skipping Layer 3: Business Analysis Pipeline")
                
            # Calculate total duration
            total_duration = (datetime.now() - pipeline_start_time).total_seconds()
            
            self.logger.info("=" * 60)
            self.logger.info("MASTER PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info(f"Total duration: {total_duration:.2f} seconds")
            self.logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            total_duration = (datetime.now() - pipeline_start_time).total_seconds()
            self.logger.error("=" * 60)
            self.logger.error("MASTER PIPELINE FAILED")
            self.logger.error(f"Error: {str(e)}")
            self.logger.error(f"Duration until failure: {total_duration:.2f} seconds")
            self.logger.error("=" * 60)
            return False
            
        finally:
            if self.orchestrator:
                self.orchestrator.close_connections()
                
    def get_pipeline_report(self) -> dict:
        """Generate comprehensive pipeline execution report"""
        
        if not self.orchestrator:
            return {"error": "Pipeline has not been run yet"}
            
        report = {
            'execution_summary': self.results,
            'pipeline_status': None,
            'data_quality_summary': None,
            'business_metrics': None
        }
        
        try:
            # Get pipeline execution status
            status_df = self.orchestrator.get_pipeline_status()
            report['pipeline_status'] = status_df.to_dict('records')
            
            # Get data quality summary
            dq_summary = self.orchestrator.get_data_quality_summary()
            report['data_quality_summary'] = dq_summary.to_dict('records')
            
            # Get business insights if Layer 3 completed
            if 'layer3' in self.results and self.results['layer3']['status'] == 'SUCCESS':
                business_conn = self.orchestrator.databases['business']
                
                # Get key business metrics
                insights_df = pd.read_sql_query("""
                    SELECT insight_type, insight_title, insight_description, 
                           metric_value, recommendation, priority_level
                    FROM business_insights
                    ORDER BY priority_level, metric_value DESC
                """, business_conn)
                
                campaign_summary = pd.read_sql_query("""
                    SELECT campaign_type, COUNT(*) as target_count, 
                           SUM(estimated_value) as total_value
                    FROM campaign_targets
                    GROUP BY campaign_type
                    ORDER BY total_value DESC
                """, business_conn)
                
                report['business_metrics'] = {
                    'insights': insights_df.to_dict('records'),
                    'campaigns': campaign_summary.to_dict('records')
                }
                
        except Exception as e:
            report['report_error'] = f"Error generating report: {str(e)}"
            
        return report
        
    def print_execution_summary(self):
        """Print a formatted execution summary"""
        
        print("\n" + "=" * 70)
        print("DATA PIPELINE EXECUTION SUMMARY")
        print("=" * 70)
        
        # Layer execution summary
        for layer_name, layer_result in self.results.items():
            status = layer_result['status']
            duration = layer_result['duration']
            
            print(f"\n{layer_name.upper()}:")
            print(f"  Status: {status}")
            print(f"  Duration: {duration:.2f} seconds")
            
            if status == 'SUCCESS' and 'summary' in layer_result:
                summary = layer_result['summary']
                
                if layer_name == 'layer1':
                    print(f"  Raw records: {summary.get('raw_records', 0):,}")
                    print(f"  Cleaned records: {summary.get('cleaned_records', 0):,}")
                    
                elif layer_name == 'layer2':
                    print(f"  Unique customers: {summary.get('unique_customers', 0):,}")
                    print(f"  Total transactions: {summary.get('total_transactions', 0):,}")
                    print(f"  Total revenue: ${summary.get('total_revenue', 0):,.2f}")
                    
                elif layer_name == 'layer3':
                    print(f"  Campaign targets: {summary.get('campaign_targets_rows', 0):,}")
                    print(f"  Business insights: {summary.get('business_insights_rows', 0):,}")
                    print(f"  Campaign value: ${summary.get('total_campaign_value', 0):,.2f}")
                    
            elif status == 'FAILED':
                print(f"  Error: {layer_result.get('error', 'Unknown error')}")
                
        # Data quality summary
        if self.orchestrator:
            print(f"\nDATA QUALITY SUMMARY:")
            try:
                dq_summary = self.orchestrator.get_data_quality_summary()
                
                if len(dq_summary) > 0:
                    total_checks = dq_summary['total_checks'].sum()
                    total_passed = dq_summary['passed'].sum()
                    total_failed = dq_summary['failed'].sum()
                    success_rate = (total_passed / total_checks * 100) if total_checks > 0 else 0
                    
                    print(f"  Total checks: {total_checks}")
                    print(f"  Passed: {total_passed}")
                    print(f"  Failed: {total_failed}")
                    print(f"  Success rate: {success_rate:.1f}%")
                    
                    if total_failed > 0:
                        print(f"\n  Failed checks by table:")
                        failed_by_table = dq_summary[dq_summary['failed'] > 0]
                        for _, row in failed_by_table.iterrows():
                            print(f"    {row['table_name']}: {row['failed']} failed")
                else:
                    print("  No data quality checks recorded")
            except Exception as e:
                print(f"  Error retrieving data quality summary: {str(e)}")
                self.logger.error(f"Data quality summary error: {str(e)}")
                
        print("\n" + "=" * 70)


def main():
    """Command line interface for the master pipeline"""
    
    parser = argparse.ArgumentParser(description='Run the 3-layer data engineering pipeline')
    parser.add_argument('csv_file', help='Path to the input CSV file')
    parser.add_argument('--skip-layers', nargs='+', 
                       choices=['staging', 'warehouse', 'business'],
                       help='Layers to skip (e.g., --skip-layers staging)')
    parser.add_argument('--report-only', action='store_true',
                       help='Only generate a report from existing data')
    
    args = parser.parse_args()
    
    # Initialize master pipeline runner
    runner = MasterPipelineRunner()
    
    if args.report_only:
        # Just generate report from existing data
        runner.orchestrator = DataPipelineOrchestrator()
        report = runner.get_pipeline_report()
        print("PIPELINE REPORT:")
        print("=" * 50)
        
        if 'pipeline_status' in report and report['pipeline_status']:
            print("\nRecent Pipeline Runs:")
            for run in report['pipeline_status'][:10]:
                print(f"  {run['layer']}.{run['table_name']}: {run['status']} ({run.get('row_count', 0)} rows)")
                
        if 'business_metrics' in report and report['business_metrics']:
            print(f"\nBusiness Insights:")
            for insight in report['business_metrics']['insights'][:5]:
                print(f"  • {insight['insight_title']}")
                print(f"    {insight['insight_description']}")
                
        runner.orchestrator.close_connections()
        
    else:
        # Run the full pipeline
        success = runner.run_full_pipeline(args.csv_file, args.skip_layers)
        
        # Print execution summary
        runner.print_execution_summary()
        
        if success:
            print("\n✅ Pipeline completed successfully!")
            
            # Show quick business insights
            if 'layer3' in runner.results and runner.results['layer3']['status'] == 'SUCCESS':
                business_conn = runner.orchestrator.databases['business']
                insights_df = pd.read_sql_query("""
                    SELECT insight_title, recommendation 
                    FROM business_insights 
                    WHERE priority_level = 1
                    ORDER BY metric_value DESC
                    LIMIT 3
                """, business_conn)
                
                if len(insights_df) > 0:
                    print(f"\n🎯 KEY BUSINESS INSIGHTS:")
                    for _, insight in insights_df.iterrows():
                        print(f"  • {insight['insight_title']}")
                        print(f"    💡 {insight['recommendation']}")
                        
        else:
            print("\n❌ Pipeline failed. Check logs for details.")
            return 1
            
    return 0


if __name__ == "__main__":
    exit(main())


# Example usage functions for Jupyter notebook environment
def run_example_pipeline():
    """Example function to run the pipeline in a Jupyter notebook"""
    
    # Example CSV file path - update this to your actual data file
    csv_file = "path/to/your/sales_data.csv"
    
    if not os.path.exists(csv_file):
        print("❌ CSV file not found. Please update the csv_file path.")
        print("Creating sample data for demonstration...")
        
        # Create sample data for demonstration
        import numpy as np
        
        # Generate sample sales data
        np.random.seed(42)
        n_records = 1000
        
        sample_data = {
            'Unnamed: 0': range(n_records),
            'Date': pd.date_range('2021-01-01', periods=n_records, freq='D'),
            'Customer ID': np.random.randint(100000, 999999, n_records),
            'Order ID': range(1, n_records + 1),
            'Sales': np.random.uniform(50, 500, n_records)
        }
        
        df = pd.DataFrame(sample_data)
        csv_file = "./data/sample_sales_data.csv"
        os.makedirs("./data", exist_ok=True)
        df.to_csv(csv_file, index=False)
        print(f"✅ Created sample data at {csv_file}")
    
    # Run the pipeline
    runner = MasterPipelineRunner()
    success = runner.run_full_pipeline(csv_file)
    
    if success:
        runner.print_execution_summary()
        return runner
    else:
        print("Pipeline failed. Check the logs for details.")
        return None


def quick_data_quality_check():
    """Quick function to check data quality status"""
    
    orchestrator = DataPipelineOrchestrator()
    
    try:
        print("DATA QUALITY STATUS:")
        print("=" * 40)
        
        # Get data quality summary
        dq_summary = orchestrator.get_data_quality_summary()
        
        if len(dq_summary) > 0:
            print(dq_summary.to_string(index=False))
            
            # Calculate overall success rate
            total_checks = dq_summary['total_checks'].sum()
            total_passed = dq_summary['passed'].sum()
            success_rate = (total_passed / total_checks * 100) if total_checks > 0 else 0
            
            print(f"\nOVERALL SUCCESS RATE: {success_rate:.1f}%")
            
        else:
            print("No data quality checks found. Run the pipeline first.")
            
    finally:
        orchestrator.close_connections()


# Jupyter notebook helpers
def get_business_insights():
    """Get business insights for analysis in Jupyter"""
    
    orchestrator = DataPipelineOrchestrator()
    
    try:
        business_conn = orchestrator.databases['business']
        
        # Get insights
        insights_df = pd.read_sql_query("""
            SELECT * FROM business_insights 
            ORDER BY priority_level, metric_value DESC
        """, business_conn)
        
        # Get campaign targets summary
        campaigns_df = pd.read_sql_query("""
            SELECT campaign_type, COUNT(*) as target_count,
                   SUM(estimated_value) as total_value,
                   AVG(estimated_value) as avg_value
            FROM campaign_targets
            GROUP BY campaign_type
            ORDER BY total_value DESC
        """, business_conn)
        
        return {
            'insights': insights_df,
            'campaigns': campaigns_df
        }
        
    finally:
        orchestrator.close_connections()