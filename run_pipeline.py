#!/usr/bin/env python3
"""
Marketing Analytics Pipeline Runner

Simple script to run the complete marketing analytics pipeline.
Usage: python run_pipeline.py [csv_file_path]

If no CSV file is provided, it will use the default HEC test data.
"""

import sys
import os
from pipeline.master_pipeline import main

def run_pipeline():
    """Run the marketing analytics pipeline"""
    
    # Default data file
    default_csv = "./data/raw/HEC_testing_data_sample_2_.csv"
    
    # Get CSV file from command line argument or use default
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = default_csv
        
    # Check if file exists
    if not os.path.exists(csv_file):
        print(f"❌ Error: CSV file not found: {csv_file}")
        print(f"💡 Make sure the file exists or provide a valid path")
        print(f"📋 Usage: python run_pipeline.py [csv_file_path]")
        return 1
        
    print(f"🚀 Starting Marketing Analytics Pipeline...")
    print(f"📊 Data source: {csv_file}")
    print(f"📁 Database output: data/ directory")
    print("=" * 60)
    
    # Set the CSV file path in sys.argv for the main function
    sys.argv = ["master_pipeline.py", csv_file]
    
    try:
        # Run the pipeline
        result = main()
        
        if result == 0:
            print("\n" + "=" * 60)
            print("🎉 Pipeline completed successfully!")
            print("\n📊 Generated databases:")
            print("   • data/staging.db - Cleaned raw data")  
            print("   • data/warehouse.db - Dimensional model")
            print("   • data/business.db - Business analysis tables")
            print("   • data/metadata.db - Pipeline execution logs")
            print("\n📓 Next steps:")
            print("   • Open notebooks/business_analysis_dashboard.ipynb")
            print("   • Run Jupyter notebook for analysis and visualization")
            print("   • Query business.db directly for custom analysis")
            
        return result
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {str(e)}")
        print("💡 Check the logs above for more details")
        return 1

if __name__ == "__main__":
    exit_code = run_pipeline()
    sys.exit(exit_code)