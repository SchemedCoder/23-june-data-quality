import os
import sys
import pandas as pd
from dotenv import load_dotenv

# Clean sys.path for PySpark compatibility
saved_sys_path = list(sys.path)
sys.path = [p for p in sys.path if " " not in p]

# Restore sys.path
sys.path = saved_sys_path
workspace_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if workspace_dir not in sys.path:
    sys.path.insert(0, workspace_dir)

from scripts.quality_engine import get_spark_session
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
import logging
logger = logging.getLogger("AnomalyDetector")

# Load environment configs
load_dotenv()

def detect_anomalies(gold_metrics_path="data/delta/gold_quality_metrics", anomaly_output_path="data/delta/gold_anomaly_report"):
    """
    Analyzes quality metrics to identify severe validation drops and record spikes in failed records.
    Saves anomaly metrics to Delta tables for observability.
    """
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Detecting Data Quality anomalies...")
    
    if not os.path.exists(gold_metrics_path):
        logger.error(f"Gold metrics not found at {gold_metrics_path}. Cannot detect anomalies.")
        return None
        
    # Read metrics from Delta Gold Table
    metrics_df = spark.read.format("delta").load(gold_metrics_path)
    
    # Load thresholds from env variables (default to 95% quality and max 5 failures)
    threshold_score = float(os.getenv("QUALITY_THRESHOLD", "95.0"))
    threshold_failures = int(os.getenv("FAILED_ROWS_THRESHOLD", "5"))
    
    # Filter anomalies using Spark SQL expressions
    anomalies_df = metrics_df.filter(
        (col("quality_score") < threshold_score) | 
        (col("failed_rows") > threshold_failures)
    )
    
    # Select columns and add issue tags
    from pyspark.sql.functions import when, current_timestamp
    final_anomalies = anomalies_df.select(
        col("dataset_name"),
        when(col("quality_score") < threshold_score, "Low Quality Score").otherwise("High Failed Rows").alias("issue"),
        when(col("quality_score") < threshold_score, col("quality_score")).otherwise(col("failed_rows").cast(DoubleType())).alias("metric_value"),
        col("severity")
    ).withColumn("detected_at", current_timestamp())
    
    anomaly_count = final_anomalies.count()
    logger.info(f"Anomaly detection complete. Identified {anomaly_count} data quality anomalies.")
    
    # Write to Gold Delta Table for alerting history
    final_anomalies.write.format("delta").mode("overwrite").save(anomaly_output_path)
    logger.info(f"Anomaly report table written to Delta at {anomaly_output_path}")
    
    # Write to local CSV for local reporting compatibility
    final_anomalies.toPandas().to_csv("anomaly_report.csv", index=False)
    logger.info("Local anomaly_report.csv output written.")
    
    return final_anomalies

if __name__ == "__main__":
    detect_anomalies()
