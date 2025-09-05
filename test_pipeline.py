#!/usr/bin/env python3
"""
Marketing Analytics Pipeline Test Suite - Enhanced Version
Tests the complete 3-layer data engineering pipeline with enhanced business analytics
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


class EnhancedPipelineTestSuite:
    """Comprehensive test suite for the enhanced marketing analytics pipeline"""
    
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
        
        print("🔧 Setting up enhanced test environment...")
        
        # Validate CSV file exists
        if not os.path.exists(self.csv_file_path):
            raise FileNotFoundError(f"CSV file not found: {self.csv_file_path}")
            
        # Create test directory for pipeline outputs
        self.test_dir = tempfile.mkdtemp(prefix='enhanced_pipeline_test_')
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
                        # Calculate time span for retention analysis expectations
                        time_span_months = (date_series.max() - date_series.min()).days / 30.44
                        print(f"   Time span: {time_span_months:.1f} months")
                        if time_span_months < 18:
                            print(f"   ⚠️  Note: Time span < 18 months may limit retention analysis")
                    except:
                        print(f"   Date range: {df['Date'].min()} to {df['Date'].max()}")
                        
                if 'Sales' in df.columns:
                    print(f"   Total sales: ${df['Sales'].sum():,.2f}")
                    print(f"   Average order value: ${df['Sales'].mean():.2f}")
                    
        except Exception as e:
            print(f"   ⚠️  Error analyzing data: {str(e)}")
            self.logger.warning(f"Data analysis error: {str(e)}")
        
    def test_enhanced_business_layer_components(self) -> dict:
        """Test the new enhanced business layer components"""
        
        print("\n🧪 Testing enhanced business layer components...")
        
        test_results = {
            'cumulative_retention_analysis': False,
            'customer_segmentation': False,
            'seasonal_trends': False,
            'executive_summary': False,
            'enhanced_insights': False,
            'retention_windows_validation': False,
            'rfm_segments_validation': False,
            'seasonal_patterns_validation': False
        }
        
        try:
            # First run the complete pipeline to get all data
            print("   Setting up pipeline for enhanced testing...")
            from pipeline.layer1_staging import run_staging_pipeline
            from pipeline.layer2_warehouse import run_warehouse_pipeline
            from pipeline.layer3_business import run_business_analysis_pipeline
            
            # Run through all layers
            staging_orchestrator, staging_summary = run_staging_pipeline(self.csv_file_path)
            warehouse_orchestrator, warehouse_summary = run_warehouse_pipeline(staging_orchestrator)
            business_orchestrator, business_summary = run_business_analysis_pipeline(staging_orchestrator)
            
            business_conn = business_orchestrator.databases['business']
            
            # Test 1: Cumulative Retention Analysis
            print("   Testing cumulative retention analysis...")
            try:
                retention_df = pd.read_sql_query("""
                    SELECT 
                        retention_window_months,
                        COUNT(DISTINCT cohort_month) as cohort_count,
                        AVG(cumulative_retention_rate) as avg_retention,
                        MIN(cumulative_retention_rate) as min_retention,
                        MAX(cumulative_retention_rate) as max_retention
                    FROM cumulative_retention_analysis
                    GROUP BY retention_window_months
                    ORDER BY retention_window_months
                """, business_conn)
                
                # Validate we have 3, 12, 18 month windows
                expected_windows = {3, 12, 18}
                actual_windows = set(retention_df['retention_window_months'].tolist())
                has_all_windows = expected_windows.issubset(actual_windows)
                
                # Validate retention logic (later windows should have <= retention than earlier)
                logical_retention = True
                if len(retention_df) >= 2:
                    for i in range(1, len(retention_df)):
                        if retention_df.iloc[i]['avg_retention'] > retention_df.iloc[i-1]['avg_retention']:
                            logical_retention = False
                            break
                
                test_results['cumulative_retention_analysis'] = len(retention_df) > 0
                test_results['retention_windows_validation'] = has_all_windows and logical_retention
                
                print(f"   ✅ Cumulative retention: {'PASSED' if test_results['cumulative_retention_analysis'] else 'FAILED'}")
                print(f"      Windows: {sorted(actual_windows)} (Expected: {sorted(expected_windows)})")
                print(f"      Retention logic: {'VALID' if logical_retention else 'INVALID'}")
                
            except Exception as e:
                print(f"   ❌ Cumulative retention analysis failed: {str(e)}")
            
            # Test 2: Customer Segmentation (RFM)
            print("   Testing customer segmentation...")
            try:
                segmentation_df = pd.read_sql_query("""
                    SELECT 
                        rfm_segment,
                        COUNT(*) as customer_count,
                        AVG(recency_score) as avg_recency,
                        AVG(frequency_score) as avg_frequency,
                        AVG(monetary_score) as avg_monetary
                    FROM customer_segmentation
                    GROUP BY rfm_segment
                    ORDER BY customer_count DESC
                """, business_conn)
                
                # Validate expected segments exist
                expected_segments = {'Champions', 'Loyal Customers', 'New Customers', 'At Risk', 'Cannot Lose Them', 'Lost Customers', 'Others'}
                actual_segments = set(segmentation_df['rfm_segment'].tolist())
                has_key_segments = len(actual_segments.intersection(expected_segments)) >= 4
                
                # Validate score ranges (1-5)
                valid_scores = True
                for col in ['avg_recency', 'avg_frequency', 'avg_monetary']:
                    if not segmentation_df[col].between(1, 5).all():
                        valid_scores = False
                        break
                
                test_results['customer_segmentation'] = len(segmentation_df) > 0
                test_results['rfm_segments_validation'] = has_key_segments and valid_scores
                
                print(f"   ✅ Customer segmentation: {'PASSED' if test_results['customer_segmentation'] else 'FAILED'}")
                print(f"      Segments found: {len(actual_segments)} (Key segments: {has_key_segments})")
                print(f"      Score validity: {'VALID' if valid_scores else 'INVALID'}")
                
            except Exception as e:
                print(f"   ❌ Customer segmentation failed: {str(e)}")
            
            # Test 3: Seasonal Trends
            print("   Testing seasonal trends...")
            try:
                seasonal_df = pd.read_sql_query("""
                    SELECT 
                        period_type,
                        COUNT(*) as period_count,
                        AVG(seasonal_index) as avg_seasonal_index,
                        MIN(seasonal_index) as min_index,
                        MAX(seasonal_index) as max_index
                    FROM seasonal_trends
                    GROUP BY period_type
                """, business_conn)
                
                # Validate monthly trends exist
                has_monthly = 'monthly' in seasonal_df['period_type'].tolist()
                
                # Validate seasonal indices are reasonable (should average around 1.0)
                valid_indices = True
                if has_monthly:
                    monthly_avg = seasonal_df[seasonal_df['period_type'] == 'monthly']['avg_seasonal_index'].iloc[0]
                    valid_indices = 0.8 <= monthly_avg <= 1.2  # Should be close to 1.0
                
                test_results['seasonal_trends'] = len(seasonal_df) > 0
                test_results['seasonal_patterns_validation'] = has_monthly and valid_indices
                
                print(f"   ✅ Seasonal trends: {'PASSED' if test_results['seasonal_trends'] else 'FAILED'}")
                print(f"      Monthly trends: {'FOUND' if has_monthly else 'MISSING'}")
                print(f"      Index validity: {'VALID' if valid_indices else 'INVALID'}")
                
            except Exception as e:
                print(f"   ❌ Seasonal trends failed: {str(e)}")
            
            # Test 4: Executive Summary View
            print("   Testing executive summary...")
            try:
                exec_df = pd.read_sql_query("""
                    SELECT 
                        metric_category,
                        COUNT(*) as metric_count
                    FROM executive_summary
                    GROUP BY metric_category
                """, business_conn)
                
                # Validate expected categories
                expected_categories = {'Current Month Performance', 'Retention Performance', 'Customer Health'}
                actual_categories = set(exec_df['metric_category'].tolist())
                has_all_categories = expected_categories.issubset(actual_categories)
                
                test_results['executive_summary'] = len(exec_df) > 0 and has_all_categories
                
                print(f"   ✅ Executive summary: {'PASSED' if test_results['executive_summary'] else 'FAILED'}")
                print(f"      Categories: {len(actual_categories)} (Complete: {has_all_categories})")
                
            except Exception as e:
                print(f"   ❌ Executive summary failed: {str(e)}")
            
            # Test 5: Enhanced Business Insights
            print("   Testing enhanced business insights...")
            try:
                insights_df = pd.read_sql_query("""
                    SELECT 
                        insight_type,
                        COUNT(*) as insight_count,
                        AVG(priority_level) as avg_priority
                    FROM business_insights
                    GROUP BY insight_type
                    ORDER BY insight_count DESC
                """, business_conn)
                
                # Validate new insight types exist
                expected_types = {'RETENTION', 'SEGMENTATION', 'SEASONAL'}
                actual_types = set(insights_df['insight_type'].tolist())
                has_new_insights = len(actual_types.intersection(expected_types)) >= 2
                
                test_results['enhanced_insights'] = len(insights_df) > 0 and has_new_insights
                
                print(f"   ✅ Enhanced insights: {'PASSED' if test_results['enhanced_insights'] else 'FAILED'}")
                print(f"      Insight types: {len(actual_types)} (Enhanced: {has_new_insights})")
                
            except Exception as e:
                print(f"   ❌ Enhanced insights failed: {str(e)}")
            
            # Close connections
            business_orchestrator.close_connections()
            
        except Exception as e:
            print(f"   ❌ Enhanced business layer test failed: {str(e)}")
            import traceback
            traceback.print_exc()
            
        return test_results
    
    def test_data_quality_enhancements(self) -> dict:
        """Test enhanced data quality checks with direct validation"""
        
        print("\n🔍 Testing enhanced data quality checks...")
        
        dq_results = {
            'retention_logic_checks': False,
            'rfm_score_validation': False,
            'seasonal_index_validation': False,
            'business_rule_compliance': False
        }
        
        try:
            # Run the business pipeline first to generate data
            from pipeline.layer3_business import run_business_analysis_pipeline
            orchestrator, summary = run_business_analysis_pipeline()
            business_conn = orchestrator.databases['business']
            
            # Test 1: Retention Logic Checks
            print("   Testing retention logic...")
            try:
                retention_df = pd.read_sql_query("""
                    SELECT cohort_month,
                        MAX(CASE WHEN retention_window_months = 3 THEN cumulative_retention_rate END) as ret_3m,
                        MAX(CASE WHEN retention_window_months = 12 THEN cumulative_retention_rate END) as ret_12m,
                        MAX(CASE WHEN retention_window_months = 18 THEN cumulative_retention_rate END) as ret_18m
                    FROM cumulative_retention_analysis 
                    GROUP BY cohort_month
                    HAVING ret_3m IS NOT NULL OR ret_12m IS NOT NULL OR ret_18m IS NOT NULL
                """, business_conn)
                
                # Check logical retention (rates shouldn't increase over time)
                logical_retention = True
                for _, row in retention_df.iterrows():
                    if row['ret_12m'] and row['ret_3m'] and row['ret_12m'] > row['ret_3m']:
                        logical_retention = False
                        break
                    if row['ret_18m'] and row['ret_12m'] and row['ret_18m'] > row['ret_12m']:
                        logical_retention = False  
                        break
                
                # Check rates are within 0-100%
                valid_ranges = True
                for col in ['ret_3m', 'ret_12m', 'ret_18m']:
                    if retention_df[col].notna().any():
                        if retention_df[col].min() < 0 or retention_df[col].max() > 100:
                            valid_ranges = False
                            break
                
                dq_results['retention_logic_checks'] = logical_retention and valid_ranges and len(retention_df) > 0
                print(f"      Retention logic: {'✅ VALID' if dq_results['retention_logic_checks'] else '❌ INVALID'}")
                
            except Exception as e:
                print(f"      ❌ Retention checks failed: {str(e)}")
            
            # Test 2: RFM Score Validation  
            print("   Testing RFM score validation...")
            try:
                rfm_df = pd.read_sql_query("""
                    SELECT rfm_segment, 
                           MIN(recency_score) as min_r, MAX(recency_score) as max_r,
                           MIN(frequency_score) as min_f, MAX(frequency_score) as max_f,
                           MIN(monetary_score) as min_m, MAX(monetary_score) as max_m,
                           COUNT(*) as segment_count
                    FROM customer_segmentation
                    GROUP BY rfm_segment
                """, business_conn)
                
                # Check score ranges (all should be 1-5)
                score_ranges_valid = True
                for col in ['min_r', 'max_r', 'min_f', 'max_f', 'min_m', 'max_m']:
                    if rfm_df[col].min() < 1 or rfm_df[col].max() > 5:
                        score_ranges_valid = False
                        break
                
                # Check we have key segments
                expected_segments = {'Champions', 'Loyal Customers', 'New Customers', 'At Risk', 'Cannot Lose Them', 'Lost Customers'}
                actual_segments = set(rfm_df['rfm_segment'].tolist())
                has_key_segments = len(expected_segments.intersection(actual_segments)) >= 4
                
                dq_results['rfm_score_validation'] = score_ranges_valid and has_key_segments and len(rfm_df) > 0
                print(f"      RFM scores: {'✅ VALID' if dq_results['rfm_score_validation'] else '❌ INVALID'}")
                print(f"      Segments found: {len(actual_segments)} (Key segments: {has_key_segments})")
                
            except Exception as e:
                print(f"      ❌ RFM validation failed: {str(e)}")
            
            # Test 3: Seasonal Index Validation
            print("   Testing seasonal index validation...")
            try:
                seasonal_df = pd.read_sql_query("""
                    SELECT period_type, period_value, seasonal_index, trend_direction
                    FROM seasonal_trends
                    WHERE period_type = 'monthly'
                """, business_conn)
                
                # Check seasonal indices are reasonable (0.1 - 3.0)
                indices_valid = True
                if len(seasonal_df) > 0:
                    indices_valid = (seasonal_df['seasonal_index'].min() >= 0.1 and 
                                   seasonal_df['seasonal_index'].max() <= 3.0)
                
                # Check we have 12 months
                has_all_months = len(seasonal_df) == 12
                
                # Check trend directions are valid
                valid_trends = {'strong_positive', 'positive', 'stable', 'negative', 'strong_negative'}
                trends_valid = seasonal_df['trend_direction'].isin(valid_trends).all()
                
                dq_results['seasonal_index_validation'] = indices_valid and has_all_months and trends_valid
                print(f"      Seasonal indices: {'✅ VALID' if dq_results['seasonal_index_validation'] else '❌ INVALID'}")
                print(f"      Months found: {len(seasonal_df)}/12")
                
            except Exception as e:
                print(f"      ❌ Seasonal validation failed: {str(e)}")
            
            # Test 4: Business Rule Compliance
            print("   Testing business rule compliance...")
            try:
                # Check monthly metrics have positive values
                monthly_valid = pd.read_sql_query("""
                    SELECT COUNT(*) as negative_count 
                    FROM monthly_metrics 
                    WHERE total_sales < 0 OR avg_order_value < 0 OR unique_customers < 0
                """, business_conn).iloc[0]['negative_count'] == 0
                
                # Check customer LTV scores are in valid range
                ltv_valid = pd.read_sql_query("""
                    SELECT COUNT(*) as invalid_count
                    FROM customer_ltv_analysis 
                    WHERE predicted_ltv_score NOT BETWEEN 1 AND 5 
                       OR churn_risk_score NOT BETWEEN 0 AND 1
                """, business_conn).iloc[0]['invalid_count'] == 0
                
                # Check campaign targets have valid recommendations
                campaign_valid = pd.read_sql_query("""
                    SELECT COUNT(*) as missing_recs
                    FROM campaign_targets 
                    WHERE recommended_action IS NULL OR recommended_action = ''
                """, business_conn).iloc[0]['missing_recs'] == 0
                
                dq_results['business_rule_compliance'] = monthly_valid and ltv_valid and campaign_valid
                print(f"      Business rules: {'✅ COMPLIANT' if dq_results['business_rule_compliance'] else '❌ VIOLATIONS'}")
                
            except Exception as e:
                print(f"      ❌ Business rule checks failed: {str(e)}")
            
            orchestrator.close_connections()
            
            passed_checks = sum(dq_results.values())
            total_checks = len(dq_results)
            print(f"   📊 Data quality summary: {passed_checks}/{total_checks} checks passed")
            
        except Exception as e:
            print(f"   ❌ Data quality testing failed: {str(e)}")
            
        return dq_results
    
    def test_business_insights_quality(self) -> dict:
        """Test the quality and completeness of business insights"""
        
        print("\n📊 Testing business insights quality...")
        
        insight_results = {
            'insight_completeness': False,
            'actionable_recommendations': False,
            'priority_distribution': False,
            'metric_validity': False
        }
        
        try:
            # Run pipeline to get insights
            self.runner = MasterPipelineRunner()
            success = self.runner.run_full_pipeline(self.csv_file_path)
            
            if success and 'layer3' in self.runner.results:
                business_conn = self.runner.orchestrator.databases['business']
                
                # Test insight completeness
                insights_df = pd.read_sql_query("""
                    SELECT 
                        insight_id,
                        insight_type,
                        insight_title,
                        insight_description,
                        metric_value,
                        recommendation,
                        priority_level
                    FROM business_insights
                    ORDER BY priority_level, metric_value DESC
                """, business_conn)
                
                if not insights_df.empty:
                    # Check completeness (no null values in key fields)
                    required_fields = ['insight_title', 'insight_description', 'recommendation']
                    completeness = all(
                        insights_df[field].notna().all() for field in required_fields
                    )
                    insight_results['insight_completeness'] = completeness
                    
                    # Check actionable recommendations (should contain action words)
                    action_words = ['implement', 'launch', 'analyze', 'focus', 'increase', 'target']
                    actionable_count = insights_df['recommendation'].str.lower().str.contains(
                        '|'.join(action_words), na=False
                    ).sum()
                    insight_results['actionable_recommendations'] = (
                        actionable_count / len(insights_df) >= 0.7
                    )
                    
                    # Check priority distribution
                    priority_counts = insights_df['priority_level'].value_counts()
                    has_priority_1 = 1 in priority_counts.index
                    insight_results['priority_distribution'] = has_priority_1
                    
                    # Check metric validity (no negative values where inappropriate)
                    valid_metrics = insights_df['metric_value'].notna().all()
                    insight_results['metric_validity'] = valid_metrics
                    
                    print(f"   ✅ Insights generated: {len(insights_df)}")
                    print(f"   ✅ Completeness: {'PASSED' if completeness else 'FAILED'}")
                    print(f"   ✅ Actionable: {actionable_count}/{len(insights_df)} insights")
                    print(f"   ✅ Priority distribution: {'VALID' if has_priority_1 else 'INVALID'}")
                    
                else:
                    print("   ⚠️  No business insights found")
                    
        except Exception as e:
            print(f"   ❌ Business insights quality test failed: {str(e)}")
            
        return insight_results
    
    def test_full_enhanced_pipeline(self) -> dict:
        """Test the complete enhanced pipeline end-to-end"""
        
        print("\n🚀 Testing complete enhanced pipeline...")
        
        test_results = {
            'pipeline_execution': False,
            'all_enhanced_components': False,
            'retention_analysis_complete': False,
            'segmentation_complete': False,
            'seasonal_analysis_complete': False,
            'executive_dashboard_ready': False,
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
            test_results['performance_acceptable'] = execution_time < 600  # Should complete in under 10 minutes
            
            if success:
                print(f"   ✅ Pipeline execution: PASSED ({execution_time:.2f} seconds)")
                
                # Check enhanced components
                if 'layer3' in self.runner.results:
                    business_summary = self.runner.results['layer3'].get('summary', {})
                    
                    # Check all enhanced tables have data
                    enhanced_tables = [
                        'cumulative_retention_analysis_rows',
                        'customer_segmentation_rows', 
                        'seasonal_trends_rows'
                    ]
                    
                    enhanced_complete = all(
                        business_summary.get(table, 0) > 0 for table in enhanced_tables
                    )
                    test_results['all_enhanced_components'] = enhanced_complete
                    
                    # Specific component checks
                    test_results['retention_analysis_complete'] = business_summary.get('cumulative_retention_analysis_rows', 0) > 0
                    test_results['segmentation_complete'] = business_summary.get('customer_segmentation_rows', 0) > 0
                    test_results['seasonal_analysis_complete'] = business_summary.get('seasonal_trends_rows', 0) > 0
                    
                    # Check executive dashboard readiness
                    business_conn = self.runner.orchestrator.databases['business']
                    try:
                        exec_count = pd.read_sql_query(
                            "SELECT COUNT(*) as count FROM executive_summary", 
                            business_conn
                        )['count'].iloc[0]
                        test_results['executive_dashboard_ready'] = exec_count > 0
                    except:
                        test_results['executive_dashboard_ready'] = False
                    
                    print(f"   ✅ Enhanced components: {'COMPLETE' if enhanced_complete else 'INCOMPLETE'}")
                    print(f"   ✅ Retention analysis: {business_summary.get('cumulative_retention_analysis_rows', 0)} rows")
                    print(f"   ✅ Customer segmentation: {business_summary.get('customer_segmentation_rows', 0)} rows")
                    print(f"   ✅ Seasonal trends: {business_summary.get('seasonal_trends_rows', 0)} rows")
                    print(f"   ✅ Executive dashboard: {'READY' if test_results['executive_dashboard_ready'] else 'NOT READY'}")
                    
                else:
                    print("   ❌ Business layer results not found")
                    
            else:
                print("   ❌ Pipeline execution: FAILED")
                
        except Exception as e:
            print(f"   ❌ Full enhanced pipeline test failed: {str(e)}")
            self.logger.error(f"Full enhanced pipeline test error: {str(e)}")
            
        return test_results
    
    def generate_enhanced_test_report(self, component_results: dict, enhanced_results: dict, 
                                     dq_results: dict, insight_results: dict, pipeline_results: dict):
        """Generate comprehensive test report for enhanced pipeline"""
        
        print("\n📊 ENHANCED PIPELINE TEST RESULTS")
        print("=" * 60)
        
        # Enhanced Business Layer Tests
        print("Enhanced Business Layer Components:")
        for component, passed in enhanced_results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"  {component.replace('_', ' ').title()}: {status}")
            
        enhanced_success_rate = sum(enhanced_results.values()) / len(enhanced_results) * 100
        print(f"\nEnhanced Components Success Rate: {enhanced_success_rate:.1f}%")
        
        # Data Quality Tests
        print("\nEnhanced Data Quality Tests:")
        for check, passed in dq_results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"  {check.replace('_', ' ').title()}: {status}")
            
        dq_success_rate = sum(dq_results.values()) / len(dq_results) * 100
        print(f"\nData Quality Success Rate: {dq_success_rate:.1f}%")
        
        # Business Insights Quality
        print("\nBusiness Insights Quality:")
        for insight, passed in insight_results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"  {insight.replace('_', ' ').title()}: {status}")
            
        insight_success_rate = sum(insight_results.values()) / len(insight_results) * 100
        print(f"\nInsights Quality Success Rate: {insight_success_rate:.1f}%")
        
        # Pipeline tests
        print("\nEnd-to-End Enhanced Pipeline Tests:")
        for test, passed in pipeline_results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"  {test.replace('_', ' ').title()}: {status}")
            
        pipeline_success_rate = sum(pipeline_results.values()) / len(pipeline_results) * 100
        print(f"\nPipeline Success Rate: {pipeline_success_rate:.1f}%")
        
        # Overall assessment
        overall_rates = [enhanced_success_rate, dq_success_rate, insight_success_rate, pipeline_success_rate]
        overall_success_rate = sum(overall_rates) / len(overall_rates)
        overall_success = overall_success_rate >= 75
        overall_status = "✅ PASSED" if overall_success else "❌ FAILED"
        
        print(f"\nOVERALL ENHANCED PIPELINE STATUS: {overall_status}")
        print(f"Overall Success Rate: {overall_success_rate:.1f}%")
        
        # Performance metrics if pipeline ran
        if self.runner and hasattr(self.runner, 'results'):
            print(f"\nPerformance Metrics:")
            for layer, result in self.runner.results.items():
                duration = result.get('duration', 0)
                print(f"  {layer.upper()} Duration: {duration:.2f} seconds")
                
        # Enhanced capabilities summary
        if pipeline_results.get('pipeline_execution', False):
            try:
                if hasattr(self.runner, 'results') and 'layer3' in self.runner.results:
                    business_summary = self.runner.results['layer3'].get('summary', {})
                    
                    print(f"\n🚀 ENHANCED ANALYTICS CAPABILITIES:")
                    print(f"   Retention Windows: 3, 12, 18 months")
                    print(f"   Customer Segments: {business_summary.get('customer_segmentation_rows', 0):,} customers segmented")
                    print(f"   Seasonal Patterns: {business_summary.get('seasonal_trends_rows', 0)} trend periods analyzed")
                    print(f"   Business Insights: {business_summary.get('business_insights_rows', 0)} insights generated")
                    print(f"   Campaign Targets: {business_summary.get('campaign_targets_rows', 0)} customers targeted")
                    
                    if business_summary.get('champions_customers', 0) > 0:
                        print(f"   Champion Customers: {business_summary['champions_customers']:,}")
                    if business_summary.get('at_risk_customers', 0) > 0:
                        print(f"   At-Risk Customers: {business_summary['at_risk_customers']:,}")
                        
            except Exception as e:
                print(f"\nCould not retrieve enhanced analytics summary: {str(e)}")
                
        return overall_success
        
    def cleanup(self):
        """Clean up test environment and all generated data"""
        
        print(f"\n🧹 Cleaning up enhanced test environment...")
        
        # Close any open connections
        if self.runner and self.runner.orchestrator:
            self.runner.orchestrator.close_connections()
            
        # Remove test directory and all database files
        if self.test_dir and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
            print(f"   Test directory removed: {self.test_dir}")
            
        # Clean up any database files in current directory
        for db_file in ['staging.db', 'warehouse.db', 'business.db', 'metadata.db']:
            if os.path.exists(db_file):
                os.remove(db_file)
                print(f"   Database file removed: {db_file}")
                
        # Clean environment variables
        if 'PIPELINE_DATA_PATH' in os.environ:
            del os.environ['PIPELINE_DATA_PATH']
            
        print("   ✅ Cleanup completed - all test data removed")


def run_enhanced_pipeline_test():
    """Run comprehensive test of the enhanced pipeline"""
    print("🔬 Running enhanced marketing analytics pipeline test...")
    
    test_suite = EnhancedPipelineTestSuite()
    
    try:
        # Setup test environment
        test_suite.setup_test_environment()
        
        # Test enhanced business layer components
        enhanced_results = test_suite.test_enhanced_business_layer_components()
        
        # Test data quality enhancements
        dq_results = test_suite.test_data_quality_enhancements()
        
        # Test full enhanced pipeline
        pipeline_results = test_suite.test_full_enhanced_pipeline()

        # Test business insights quality
        insight_results = test_suite.test_business_insights_quality()
        
        # Generate comprehensive report
        success = test_suite.generate_enhanced_test_report(
            {}, enhanced_results, dq_results, insight_results, pipeline_results
        )
        
        return success
        
    except Exception as e:
        print(f"❌ Enhanced test suite failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        test_suite.cleanup()


def run_retention_analysis_test():
    """Focused test on retention analysis components"""
    print("📈 Testing retention analysis components...")
    
    test_suite = EnhancedPipelineTestSuite()
    
    try:
        test_suite.setup_test_environment()
        
        # Run just the retention analysis part
        from pipeline.layer1_staging import run_staging_pipeline
        from pipeline.layer2_warehouse import run_warehouse_pipeline
        from pipeline.layer3_business import BusinessAnalysisLayer
        
        staging_orchestrator, _ = run_staging_pipeline(test_suite.csv_file_path)
        warehouse_orchestrator, _ = run_warehouse_pipeline(staging_orchestrator)
        
        business = BusinessAnalysisLayer(staging_orchestrator)
        business.create_business_schema()
        
        # Test just cumulative retention
        print("   Building cumulative retention analysis...")
        business.build_cumulative_retention_analysis()
        
        # Validate results
        retention_df = pd.read_sql_query("""
            SELECT 
                cohort_month,
                retention_window_months,
                cohort_size,
                active_customers,
                cumulative_retention_rate
            FROM cumulative_retention_analysis
            ORDER BY cohort_month, retention_window_months
        """, business.business_conn)
        
        print(f"   ✅ Retention analysis completed: {len(retention_df)} rows")
        print(f"   Windows tested: {sorted(retention_df['retention_window_months'].unique())}")
        print(f"   Cohorts analyzed: {retention_df['cohort_month'].nunique()}")
        
        staging_orchestrator.close_connections()
        
        return len(retention_df) > 0
        
    except Exception as e:
        print(f"   ❌ Retention analysis test failed: {str(e)}")
        return False
        
    finally:
        test_suite.cleanup()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test the enhanced marketing analytics pipeline')
    parser.add_argument('--enhanced', action='store_true', help='Run comprehensive enhanced pipeline test')
    parser.add_argument('--retention', action='store_true', help='Run focused retention analysis test')
    parser.add_argument('--csv-file', type=str, help='Path to CSV file (default: ./data/raw/HEC_testing_data_sample_2_.csv)')
    
    args = parser.parse_args()
    
    if args.enhanced:
        success = run_enhanced_pipeline_test()
    elif args.retention:
        success = run_retention_analysis_test()
    else:
        # Default: run enhanced test
        csv_path = args.csv_file or './data/raw/HEC_testing_data_sample_2_.csv'
        print(f"🧪 Running enhanced pipeline test with data: {csv_path}")
        test_suite = EnhancedPipelineTestSuite(csv_file_path=csv_path)
        
        try:
            test_suite.setup_test_environment()
            enhanced_results = test_suite.test_enhanced_business_layer_components()
            dq_results = test_suite.test_data_quality_enhancements()
            insight_results = test_suite.test_business_insights_quality()
            pipeline_results = test_suite.test_full_enhanced_pipeline()
            success = test_suite.generate_enhanced_test_report(
                {}, enhanced_results, dq_results, insight_results, pipeline_results
            )
        except Exception as e:
            print(f"❌ Test suite failed: {str(e)}")
            success = False
        finally:
            test_suite.cleanup()
    
    # Exit with appropriate code
    exit(0 if success else 1)