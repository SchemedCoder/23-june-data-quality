import os
import shutil
import time
import sys

# Clean sys.path of space-containing elements to prevent JVM launch issues on Windows
saved_sys_path = list(sys.path)
sys.path = [p for p in sys.path if " " not in p]

# Import SparkSession after cleaning path
from pyspark.sql import SparkSession

# Restore sys.path
sys.path = saved_sys_path
workspace_dir = os.getcwd()
if workspace_dir not in sys.path:
    sys.path.insert(0, workspace_dir)

from scripts import quality_engine, anomaly_detector, alert_manager

def clear_outputs():
    """
    Cleans up previous outputs to guarantee a clean data quality pipeline run.
    """
    paths_to_clean = [
        "data/delta", 
        "data/spark-warehouse", 
        "data/derby", 
        "data_quality_report.csv", 
        "anomaly_report.csv", 
        "incident_log.csv"
    ]
    print("[*] Cleaning up previous data quality runs...")
    for path in paths_to_clean:
        if os.path.exists(path):
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
                print(f"    - Cleaned: {path}")
            except Exception as e:
                print(f"    - [Warning] Could not clean {path}: {e}")

def main():
    print("====================================================================")
    print("       DATA QUALITY OBSERVABILITY & SLA ALERTING PLATFORM")
    print("====================================================================")
    
    # 1. Clean previous runs
    clear_outputs()
    
    # Ensure source mock files are present in 'data/'
    if not os.path.exists("data/orders.csv"):
        print("[!] Warning: Missing source CSV files in data/ directory.")
        return
        
    # 2. Run Data Quality Engine (Bronze CSVs -> Silver Clean -> Gold Metrics)
    print("\n----------------------------------------------------")
    print("   STAGE 1: Executing Data Quality Rules Engine")
    print("----------------------------------------------------")
    engine = quality_engine.QualityEngine()
    engine.run_all()
    
    # 3. Run Anomaly Detector (Analyzing Gold Metrics Delta Table)
    print("\n----------------------------------------------------")
    print("   STAGE 2: Analyzing Quality Metrics for Anomalies")
    print("----------------------------------------------------")
    anomaly_detector.detect_anomalies()
    
    # 4. Run Alert Manager (Dispatched notifications & Incident logging)
    print("\n----------------------------------------------------")
    print("   STAGE 3: Triggering Alerts and Incident Auditing")
    print("----------------------------------------------------")
    alert_manager.run_alerting()
    
    # 5. Delta Log audit checks (Demonstrating Delta Lake time travel history query)
    print("\n----------------------------------------------------")
    print("   STAGE 4: Verifying Delta Lake Transaction History")
    print("----------------------------------------------------")
    spark = quality_engine.get_spark_session()
    
    gold_metrics_path = "data/delta/gold_quality_metrics"
    if os.path.exists(gold_metrics_path):
        print("[+] Displaying history of commit actions from Delta Lake logs:")
        from delta.tables import DeltaTable
        dt = DeltaTable.forPath(spark, gold_metrics_path)
        dt.history().select(
            "version", "timestamp", "userId", "userName", "operation", "operationParameters"
        ).show(truncate=False)
    else:
        print("[!] Delta Gold Metrics table not found.")
        
    print("\n====================================================================")
    print("                     DEMO RUN COMPLETED SUCCESSFULLY")
    print("====================================================================")

if __name__ == "__main__":
    main()
