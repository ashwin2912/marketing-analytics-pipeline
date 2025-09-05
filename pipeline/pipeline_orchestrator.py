import sqlite3
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import json
import os

class DataPipelineOrchestrator:
    """
    Main orchestrator for the 3-layer data engineering pipeline
    Layer 1: Staging (raw data ingestion)
    Layer 2: Facts & Dimensions (data modeling)
    Layer 3: Business Analysis (aggregated views)
    """
    
    def __init__(self, base_path: str = "./data"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
        
        # Database connections
        self.databases = {
            'staging': sqlite3.connect(f'{base_path}/staging.db'),
            'warehouse': sqlite3.connect(f'{base_path}/warehouse.db'),
            'business': sqlite3.connect(f'{base_path}/business.db'),
            'metadata': sqlite3.connect(f'{base_path}/metadata.db')
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'{base_path}/pipeline.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize metadata tables
        self._setup_metadata_tables()
        
    def _setup_metadata_tables(self):
        """Create metadata tracking tables"""
        metadata_conn = self.databases['metadata']
        
        # Pipeline run tracking
        metadata_conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                layer TEXT,
                table_name TEXT,
                status TEXT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                row_count INTEGER,
                error_message TEXT
            )
        """)
        
        # Data quality check results
        metadata_conn.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_checks (
                check_id TEXT PRIMARY KEY,
                run_id TEXT,
                table_name TEXT,
                check_type TEXT,
                check_name TEXT,
                expected_value TEXT,
                actual_value TEXT,
                status TEXT,
                error_details TEXT,
                check_time TIMESTAMP,
                FOREIGN KEY (run_id) REFERENCES pipeline_runs (run_id)
            )
        """)
        
        # Table lineage tracking
        metadata_conn.execute("""
            CREATE TABLE IF NOT EXISTS table_lineage (
                lineage_id TEXT PRIMARY KEY,
                source_table TEXT,
                target_table TEXT,
                transformation_type TEXT,
                created_at TIMESTAMP
            )
        """)
        
        metadata_conn.commit()
        
    def log_pipeline_run(self, layer: str, table_name: str, status: str, 
                        row_count: int = None, error_message: str = None) -> str:
        """Log pipeline run information"""
        run_id = f"{layer}_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        
        metadata_conn = self.databases['metadata']
        
        if status == 'STARTED':
            metadata_conn.execute("""
                INSERT INTO pipeline_runs 
                (run_id, layer, table_name, status, start_time)
                VALUES (?, ?, ?, ?, ?)
            """, (run_id, layer, table_name, status, datetime.now()))
        else:
            metadata_conn.execute("""
                UPDATE pipeline_runs 
                SET status = ?, end_time = ?, row_count = ?, error_message = ?
                WHERE run_id = ?
            """, (status, datetime.now(), row_count, error_message, run_id))
        
        metadata_conn.commit()
        return run_id
        
    def log_data_quality_check(self, run_id: str, table_name: str, check_type: str,
                              check_name: str, expected: str, actual: str, 
                              status: str, error_details: str = None):
        """Log data quality check results"""
        check_id = f"{run_id}_{check_name}_{datetime.now().strftime('%H%M%S_%f')}"
        
        metadata_conn = self.databases['metadata']
        metadata_conn.execute("""
            INSERT INTO data_quality_checks 
            (check_id, run_id, table_name, check_type, check_name, 
             expected_value, actual_value, status, error_details, check_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (check_id, run_id, table_name, check_type, check_name, 
              expected, actual, status, error_details, datetime.now()))
        
        metadata_conn.commit()
        
    def get_pipeline_status(self) -> pd.DataFrame:
        """Get current pipeline status"""
        metadata_conn = self.databases['metadata']
        return pd.read_sql_query("""
            SELECT layer, table_name, status, start_time, end_time, row_count, error_message
            FROM pipeline_runs 
            ORDER BY start_time DESC 
            LIMIT 20
        """, metadata_conn)
        
    def get_data_quality_summary(self) -> pd.DataFrame:
        """Get data quality check summary"""
        metadata_conn = self.databases['metadata']
        return pd.read_sql_query("""
            SELECT table_name, check_type, 
                   COUNT(*) as total_checks,
                   SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END) as passed,
                   SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed
            FROM data_quality_checks 
            GROUP BY table_name, check_type
            ORDER BY table_name
        """, metadata_conn)
        
    def close_connections(self):
        """Close all database connections"""
        for db_name, conn in self.databases.items():
            conn.close()
            self.logger.info(f"Closed {db_name} database connection")


class DataQualityChecker:
    """Data quality validation framework"""
    
    def __init__(self, orchestrator: DataPipelineOrchestrator):
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        
    def check_row_count(self, conn: sqlite3.Connection, table_name: str, 
                       min_rows: int, run_id: str) -> bool:
        """Check minimum row count"""
        try:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            actual_count = cursor.fetchone()[0]
            
            status = "PASSED" if actual_count >= min_rows else "FAILED"
            
            self.orchestrator.log_data_quality_check(
                run_id, table_name, "ROW_COUNT", "min_rows_check",
                str(min_rows), str(actual_count), status
            )
            
            if status == "FAILED":
                self.logger.warning(f"Row count check failed for {table_name}: {actual_count} < {min_rows}")
                
            return status == "PASSED"
            
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, table_name, "ROW_COUNT", "min_rows_check",
                str(min_rows), "ERROR", "FAILED", str(e)
            )
            return False
            
    def check_null_percentage(self, conn: sqlite3.Connection, table_name: str,
                             column_name: str, max_null_pct: float, run_id: str) -> bool:
        """Check null percentage in a column"""
        try:
            cursor = conn.execute(f"""
                SELECT 
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN {column_name} IS NULL THEN 1 ELSE 0 END) as null_rows
                FROM {table_name}
            """)
            result = cursor.fetchone()
            total_rows, null_rows = result
            
            actual_null_pct = (null_rows / total_rows * 100) if total_rows > 0 else 0
            status = "PASSED" if actual_null_pct <= max_null_pct else "FAILED"
            
            self.orchestrator.log_data_quality_check(
                run_id, table_name, "NULL_CHECK", f"{column_name}_null_check",
                f"<={max_null_pct}%", f"{actual_null_pct:.2f}%", status
            )
            
            return status == "PASSED"
            
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, table_name, "NULL_CHECK", f"{column_name}_null_check",
                f"<={max_null_pct}%", "ERROR", "FAILED", str(e)
            )
            return False
            
    def check_unique_count(self, conn: sqlite3.Connection, table_name: str,
                          column_name: str, min_unique: int, run_id: str) -> bool:
        """Check minimum unique values in a column"""
        try:
            cursor = conn.execute(f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name}")
            actual_unique = cursor.fetchone()[0]
            
            status = "PASSED" if actual_unique >= min_unique else "FAILED"
            
            self.orchestrator.log_data_quality_check(
                run_id, table_name, "UNIQUENESS", f"{column_name}_unique_check",
                f">={min_unique}", str(actual_unique), status
            )
            
            return status == "PASSED"
            
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, table_name, "UNIQUENESS", f"{column_name}_unique_check",
                f">={min_unique}", "ERROR", "FAILED", str(e)
            )
            return False
            
    def check_date_range(self, conn: sqlite3.Connection, table_name: str,
                        date_column: str, min_date: str, max_date: str, run_id: str) -> bool:
        """Check date range validity"""
        try:
            cursor = conn.execute(f"""
                SELECT MIN({date_column}), MAX({date_column}) 
                FROM {table_name}
                WHERE {date_column} IS NOT NULL
            """)
            result = cursor.fetchone()
            actual_min, actual_max = result
            
            # Check if dates are within expected range
            date_valid = True
            if actual_min < min_date or actual_max > max_date:
                date_valid = False
                
            status = "PASSED" if date_valid else "FAILED"
            
            self.orchestrator.log_data_quality_check(
                run_id, table_name, "DATE_RANGE", f"{date_column}_range_check",
                f"{min_date} to {max_date}", f"{actual_min} to {actual_max}", status
            )
            
            return status == "PASSED"
            
        except Exception as e:
            self.orchestrator.log_data_quality_check(
                run_id, table_name, "DATE_RANGE", f"{date_column}_range_check",
                f"{min_date} to {max_date}", "ERROR", "FAILED", str(e)
            )
            return False


# Example usage and configuration
if __name__ == "__main__":
    # Initialize the orchestrator
    orchestrator = DataPipelineOrchestrator()
    
    # Example of how to use the orchestrator
    print("Pipeline orchestrator initialized successfully!")
    print("Databases created:")
    for db_name in orchestrator.databases.keys():
        print(f"- {db_name}.db")
    
    # Check pipeline status
    status_df = orchestrator.get_pipeline_status()
    print(f"\nPipeline status table created with {len(status_df)} rows")
    
    # Close connections
    orchestrator.close_connections()