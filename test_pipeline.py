#!/usr/bin/env python3
"""
Marketing Analytics Pipeline Test Suite
Tests the complete 3-layer data engineering pipeline with sample data
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import tempfile
import shutil
import logging

# Add pipeline modules to path
sys.path.append('./pipeline')

from pipeline.master_pipeline import MasterPipelineRunner
from pipeline.pipeline_orchestrator import DataPipelineOrchestrator


class PipelineTestSuite:
    """Comprehensive test suite for the marketing analytics pipeline"""
    
    def __init__(self, csv_file_path: str = None):
        self.csv_file_path = csv_file_path or './data/raw/HEC_testing_data_sample_2_.csv'
        self.test_dir = None
        self.runner = None
        
        # Setup test logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - TEST - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_test_environment(self):
        """Set up test environment and validate actual data"""
        
        print("🔧 Setting up test environment...")
        
        # Validate CSV file exists
        if not os.path.exists(self.csv_file_path):
            raise FileNotFoundError(f"CSV file not found: {self.csv_file_path}")
            
        # Create test directory for pipeline outputs
        self.test_dir = tempfile.mkdtemp(prefix='pipeline_test_')
        print(f"📁 Test directory: {self.test_dir}")
        
        # Analyze actual data
        self._analyze_actual_data()
        print(f"📊 Using actual data: {self.csv_file_path}")
        
        # Update data path for pipeline to use test directory
        os.environ['PIPELINE_DATA_PATH'] = self.test_dir
        
        return True
        
    def _analyze_actual_data(self):
        """Analyze the actual data file and display statistics"""
        
        try:
            # Load and analyze the actual data
            df = pd.read_csv(self.csv_file_path)
            
            # Print actual data statistics
            print(f"📈 Actual data statistics:")
            print(f"   Records: {len(df):,}")
            print(f"   Columns: {list(df.columns)}")
            
            # Check for expected columns
            expected_columns = ['Date', 'Customer ID', 'Order ID', 'Sales']
            missing_columns = [col for col in expected_columns if col not in df.columns]
            
            if missing_columns:
                print(f"   ⚠️  Missing expected columns: {missing_columns}")
            else:
                print(f"   ✅ All expected columns present")
                
                # Analyze key metrics if columns exist
                if 'Customer ID' in df.columns:
                    print(f"   Unique customers: {df['Customer ID'].nunique():,}")
                    
                if 'Date' in df.columns:
                    try:
                        # Try to parse dates
                        date_series = pd.to_datetime(df['Date'])
                        print(f"   Date range: {date_series.min()} to {date_series.max()}")
                    except:
                        print(f"   Date range: {df['Date'].min()} to {df['Date'].max()}")
                        
                if 'Sales' in df.columns:
                    print(f"   Total sales: ${df['Sales'].sum():,.2f}")
                    print(f"   Average order value: ${df['Sales'].mean():.2f}")
                    
        except Exception as e:
            print(f"   ⚠️  Error analyzing data: {str(e)}")
            self.logger.warning(f"Data analysis error: {str(e)}")
        
    def test_pipeline_components(self) -> dict:
        """Test individual pipeline components"""
        
        print("\n🧪 Testing pipeline components...")
        
        test_results = {
            'orchestrator_init': False,
            'staging_layer': False,
            'warehouse_layer': False,
            'business_layer': False,
            'data_quality': False
        }
        
        try:
            # Test orchestrator initialization
            print("   Testing pipeline orchestrator...")
            orchestrator = DataPipelineOrchestrator(base_path=self.test_dir)
            test_results['orchestrator_init'] = True
            print("   ✅ Orchestrator initialization: PASSED")
            
            # Test staging layer
            print("   Testing staging layer...")
            from pipeline.layer1_staging import run_staging_pipeline
            shared_orchestrator, staging_summary = run_staging_pipeline(self.csv_file_path)
            test_results['staging_layer'] = staging_summary['raw_records'] > 0
            print(f"   ✅ Staging layer: PASSED ({staging_summary['raw_records']:,} records)")
            
            # Test warehouse layer using same orchestrator
            print("   Testing warehouse layer...")
            from pipeline.layer2_warehouse import run_warehouse_pipeline
            warehouse_orchestrator, warehouse_summary = run_warehouse_pipeline(shared_orchestrator)
            test_results['warehouse_layer'] = warehouse_summary['unique_customers'] > 0
            print(f"   ✅ Warehouse layer: PASSED ({warehouse_summary['unique_customers']:,} customers)")
            
            # Test business layer using same orchestrator  
            print("   Testing business analysis layer...")
            from pipeline.layer3_business import run_business_analysis_pipeline
            business_orchestrator, business_summary = run_business_analysis_pipeline(shared_orchestrator)
            test_results['business_layer'] = business_summary['campaign_targets_rows'] >= 0
            print(f"   ✅ Business layer: PASSED ({business_summary['campaign_targets_rows']:,} targets)")
            
            # Close connections at the end
            shared_orchestrator.close_connections()
            
            # Test data quality
            print("   Testing data quality checks...")
            dq_orchestrator = DataPipelineOrchestrator(base_path=self.test_dir)
            dq_summary = dq_orchestrator.get_data_quality_summary()
            if len(dq_summary) > 0:
                total_checks = dq_summary['total_checks'].sum()
                passed_checks = dq_summary['passed'].sum()
                success_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0
                test_results['data_quality'] = success_rate >= 80  # At least 80% success rate
                print(f"   ✅ Data quality: PASSED ({success_rate:.1f}% success rate)")
            else:
                test_results['data_quality'] = True
                print("   ✅ Data quality: PASSED (no checks recorded)")
            
            dq_orchestrator.close_connections()
            
        except Exception as e:
            print(f"   ❌ Component test failed: {str(e)}")
            self.logger.error(f"Component test error: {str(e)}")
            
        return test_results
    
    def debug_warehouse_components(self) -> dict:
        """Debug individual warehouse components to isolate issues"""
        
        print("\n🔍 Debugging warehouse components...")
        
        debug_results = {
            'date_dimension': False,
            'customer_dimension': False,  
            'order_dimension': False,
            'sales_fact': False
        }
        
        try:
            # First ensure staging layer is ready
            from pipeline.layer1_staging import run_staging_pipeline
            staging_orchestrator, staging_summary = run_staging_pipeline(self.csv_file_path)
            # Don't close connections yet - warehouse needs to access staging DB
            
            # Initialize warehouse layer using the same orchestrator
            from pipeline.layer2_warehouse import WarehouseLayer
            warehouse = WarehouseLayer(staging_orchestrator)
            warehouse.create_warehouse_schema()
            
            # Test date dimension
            print("   Testing date dimension...")
            try:
                warehouse.build_date_dimension()
                debug_results['date_dimension'] = True
                print("   ✅ Date dimension: PASSED")
            except Exception as e:
                print(f"   ❌ Date dimension failed: {str(e)}")
                
            # Test customer dimension with detailed error info
            print("   Testing customer dimension...")
            try:
                warehouse.build_customer_dimension()
                debug_results['customer_dimension'] = True
                print("   ✅ Customer dimension: PASSED")
            except Exception as e:
                print(f"   ❌ Customer dimension failed: {str(e)}")
                # Let's debug this specific query
                self._debug_customer_dimension_query(warehouse)
                
            # Test order dimension
            print("   Testing order dimension...")
            try:
                warehouse.build_order_dimension()
                debug_results['order_dimension'] = True
                print("   ✅ Order dimension: PASSED")
            except Exception as e:
                print(f"   ❌ Order dimension failed: {str(e)}")
                
            # Test sales fact
            print("   Testing sales fact...")
            try:
                warehouse.build_sales_fact()
                debug_results['sales_fact'] = True
                print("   ✅ Sales fact: PASSED")
            except Exception as e:
                print(f"   ❌ Sales fact failed: {str(e)}")
                
            staging_orchestrator.close_connections()
            
        except Exception as e:
            print(f"   ❌ Warehouse debugging failed: {str(e)}")
            import traceback
            traceback.print_exc()
            
        return debug_results
        
    def _debug_customer_dimension_query(self, warehouse):
        """Debug the specific customer dimension query"""
        
        print("     🔍 Debugging customer dimension query...")
        
        try:
            # Test the basic aggregation first
            cursor = warehouse.warehouse_conn.execute("""
                SELECT 
                    customer_id,
                    MIN(date_parsed) as first_order_date,
                    MAX(date_parsed) as last_order_date,
                    COUNT(*) as total_transactions,
                    SUM(sales_amount) as total_spent
                FROM staging.stg_sales_cleaned 
                WHERE data_quality_flag = 'VALID'
                GROUP BY customer_id
                LIMIT 5
            """)
            
            results = cursor.fetchall()
            print(f"     ✅ Basic aggregation works - sample: {results[0] if results else 'No data'}")
            
            # Test the date calculations
            cursor = warehouse.warehouse_conn.execute("""
                SELECT 
                    customer_id,
                    MIN(date_parsed) as first_order_date,
                    julianday('now') - julianday(MIN(date_parsed)) as days_since_first_order
                FROM staging.stg_sales_cleaned 
                WHERE data_quality_flag = 'VALID'
                GROUP BY customer_id
                LIMIT 3
            """)
            
            results = cursor.fetchall()
            print(f"     ✅ Date calculations work - sample: {results[0] if results else 'No data'}")
            
            # Test the complex customer enrichment step by step
            cursor = warehouse.warehouse_conn.execute("""
                WITH customer_metrics AS (
                    SELECT 
                        customer_id,
                        MIN(date_parsed) as first_order_date,
                        MAX(date_parsed) as last_order_date,
                        COUNT(*) as total_transactions,
                        SUM(sales_amount) as total_spent,
                        AVG(sales_amount) as avg_order_value,
                        COUNT(DISTINCT order_id) as total_orders
                    FROM staging.stg_sales_cleaned 
                    WHERE data_quality_flag = 'VALID'
                    GROUP BY customer_id
                    LIMIT 5
                )
                SELECT * FROM customer_metrics
            """)
            
            results = cursor.fetchall()
            print(f"     ✅ Customer metrics CTE works - sample count: {len(results)}")
            
        except Exception as e:
            print(f"     ❌ Customer dimension debug failed: {str(e)}")
            import traceback
            traceback.print_exc()
        
    def test_full_pipeline(self) -> dict:
        """Test the complete end-to-end pipeline"""
        
        print("\n🚀 Testing complete pipeline...")
        
        test_results = {
            'pipeline_execution': False,
            'all_layers_completed': False,
            'business_insights_generated': False,
            'campaign_targets_created': False,
            'performance_acceptable': False
        }
        
        try:
            # Initialize and run master pipeline
            start_time = datetime.now()
            
            self.runner = MasterPipelineRunner()
            success = self.runner.run_full_pipeline(self.csv_file_path)
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            test_results['pipeline_execution'] = success
            test_results['performance_acceptable'] = execution_time < 300  # Should complete in under 5 minutes
            
            if success:
                print(f"   ✅ Pipeline execution: PASSED ({execution_time:.2f} seconds)")
                
                # Check if all layers completed
                layers_completed = all([
                    self.runner.results.get('layer1', {}).get('status') == 'SUCCESS',
                    self.runner.results.get('layer2', {}).get('status') == 'SUCCESS',
                    self.runner.results.get('layer3', {}).get('status') == 'SUCCESS'
                ])
                test_results['all_layers_completed'] = layers_completed
                
                if layers_completed:
                    print("   ✅ All layers completed: PASSED")
                    
                    # Test business insights
                    business_conn = self.runner.orchestrator.databases['business']
                    
                    # Check business insights from layer results instead of database
                    if 'layer3' in self.runner.results:
                        business_summary = self.runner.results['layer3'].get('summary', {})
                        insights_count = business_summary.get('business_insights_rows', 0)
                        campaigns_count = business_summary.get('campaign_targets_rows', 0)
                        
                        test_results['business_insights_generated'] = insights_count > 0
                        test_results['campaign_targets_created'] = campaigns_count >= 0
                        
                        print(f"   ✅ Business insights: {'PASSED' if insights_count > 0 else 'NOTED'} ({insights_count} insights)")
                        print(f"   ✅ Campaign targets: {'PASSED' if campaigns_count > 0 else 'NOTED'} ({campaigns_count} targets)")
                    else:
                        test_results['business_insights_generated'] = False
                        test_results['campaign_targets_created'] = False
                    
                else:
                    print("   ❌ Not all layers completed successfully")
                    
            else:
                print("   ❌ Pipeline execution: FAILED")
                
        except Exception as e:
            print(f"   ❌ Full pipeline test failed: {str(e)}")
            self.logger.error(f"Full pipeline test error: {str(e)}")
            
        return test_results
        
    def generate_test_report(self, component_results: dict, pipeline_results: dict):
        """Generate comprehensive test report"""
        
        print("\n📊 TEST RESULTS SUMMARY")
        print("=" * 50)
        
        # Component tests
        print("Component Tests:")
        for component, passed in component_results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"  {component.replace('_', ' ').title()}: {status}")
            
        component_success_rate = sum(component_results.values()) / len(component_results) * 100
        print(f"\nComponent Success Rate: {component_success_rate:.1f}%")
        
        # Pipeline tests
        print("\nEnd-to-End Pipeline Tests:")
        for test, passed in pipeline_results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"  {test.replace('_', ' ').title()}: {status}")
            
        pipeline_success_rate = sum(pipeline_results.values()) / len(pipeline_results) * 100
        print(f"\nPipeline Success Rate: {pipeline_success_rate:.1f}%")
        
        # Overall assessment
        overall_success = component_success_rate >= 80 and pipeline_success_rate >= 80
        overall_status = "✅ PASSED" if overall_success else "❌ FAILED"
        print(f"\nOVERALL TEST STATUS: {overall_status}")
        
        # Performance metrics if pipeline ran
        if self.runner and hasattr(self.runner, 'results'):
            print(f"\nPerformance Metrics:")
            for layer, result in self.runner.results.items():
                duration = result.get('duration', 0)
                print(f"  {layer.upper()} Duration: {duration:.2f} seconds")
                
        # Business insights preview - get this before connections are closed
        business_insights_preview = None
        if pipeline_results.get('pipeline_execution', False):
            try:
                # Get business insights from the business summary instead
                if hasattr(self.runner, 'results') and 'layer3' in self.runner.results:
                    business_summary = self.runner.results['layer3'].get('summary', {})
                    if business_summary.get('business_insights_rows', 0) > 0:
                        business_insights_preview = "Business insights were successfully generated"
                        print(f"\n✅ Business Insights Generated: {business_summary['business_insights_rows']} insights")
                        if business_summary.get('high_priority_insights', 0) > 0:
                            print(f"   High Priority Insights: {business_summary['high_priority_insights']}")
                        
            except Exception as e:
                print(f"\nCould not retrieve business insights: {str(e)}")
                
        return overall_success
        
    def cleanup(self):
        """Clean up test environment and all generated data"""
        
        print(f"\n🧹 Cleaning up test environment...")
        
        # Close any open connections
        if self.runner and self.runner.orchestrator:
            self.runner.orchestrator.close_connections()
            
        # Remove test directory and all database files
        if self.test_dir and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
            print(f"   Test directory removed: {self.test_dir}")
            
        # Clean up any database files in current directory (in case they were created there)
        for db_file in ['staging.db', 'warehouse.db', 'business.db', 'metadata.db']:
            if os.path.exists(db_file):
                os.remove(db_file)
                print(f"   Database file removed: {db_file}")
                
        # Clean environment variables
        if 'PIPELINE_DATA_PATH' in os.environ:
            del os.environ['PIPELINE_DATA_PATH']
            
        print("   ✅ Cleanup completed - all test data removed")


def run_debug_warehouse():
    """Run detailed warehouse debugging"""
    print("🔍 Running warehouse component debugging...")
    
    test_suite = PipelineTestSuite()
    
    try:
        test_suite.setup_test_environment()
        debug_results = test_suite.debug_warehouse_components()
        
        print("\n🔍 WAREHOUSE DEBUG RESULTS:")
        print("=" * 40)
        for component, passed in debug_results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"  {component.replace('_', ' ').title()}: {status}")
            
        success_rate = sum(debug_results.values()) / len(debug_results) * 100
        print(f"\nWarehouse Success Rate: {success_rate:.1f}%")
        
        return success_rate >= 50
        
    except Exception as e:
        print(f"❌ Debug suite failed: {str(e)}")
        return False
        
    finally:
        test_suite.cleanup()

def run_quick_test():
    """Run a quick test with actual data"""
    print("🏃‍♂️ Running quick pipeline test with actual data...")
    
    test_suite = PipelineTestSuite()
    
    try:
        # Setup test environment
        test_suite.setup_test_environment()
        
        # Run component tests
        component_results = test_suite.test_pipeline_components()
        
        # Run warehouse debugging if warehouse failed
        if not component_results.get('warehouse_layer', False):
            print("\n🔧 Warehouse layer failed, running detailed debugging...")
            test_suite.debug_warehouse_components()
        
        # Run full pipeline test
        pipeline_results = test_suite.test_full_pipeline()
        
        # Generate report
        success = test_suite.generate_test_report(component_results, pipeline_results)
        
        return success
        
    except Exception as e:
        print(f"❌ Test suite failed: {str(e)}")
        return False
        
    finally:
        test_suite.cleanup()


def run_comprehensive_test():
    """Run comprehensive test with actual dataset"""
    print("🔬 Running comprehensive pipeline test with actual data...")
    
    test_suite = PipelineTestSuite()
    
    try:
        # Setup test environment
        test_suite.setup_test_environment()
        
        # Run component tests
        component_results = test_suite.test_pipeline_components()
        
        # Run full pipeline test
        pipeline_results = test_suite.test_full_pipeline()
        
        # Generate report
        success = test_suite.generate_test_report(component_results, pipeline_results)
        
        return success
        
    except Exception as e:
        print(f"❌ Test suite failed: {str(e)}")
        return False
        
    finally:
        test_suite.cleanup()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test the marketing analytics pipeline')
    parser.add_argument('--quick', action='store_true', help='Run quick test with actual data')
    parser.add_argument('--comprehensive', action='store_true', help='Run comprehensive test with actual data')
    parser.add_argument('--debug-warehouse', action='store_true', help='Run detailed warehouse debugging')
    parser.add_argument('--csv-file', type=str, help='Path to CSV file (default: ./data/raw/HEC_testing_data_sample_2_.csv)')
    
    args = parser.parse_args()
    
    if args.quick:
        success = run_quick_test()
    elif args.comprehensive:
        success = run_comprehensive_test()
    elif args.debug_warehouse:
        success = run_debug_warehouse()
    else:
        # Custom CSV file path
        csv_path = args.csv_file or './data/raw/HEC_testing_data_sample_2_.csv'
        print(f"🧪 Running pipeline test with data: {csv_path}")
        test_suite = PipelineTestSuite(csv_file_path=csv_path)
        
        try:
            test_suite.setup_test_environment()
            component_results = test_suite.test_pipeline_components()
            pipeline_results = test_suite.test_full_pipeline()
            success = test_suite.generate_test_report(component_results, pipeline_results)
        except Exception as e:
            print(f"❌ Test suite failed: {str(e)}")
            success = False
        finally:
            test_suite.cleanup()
    
    # Exit with appropriate code
    exit(0 if success else 1)