import os
import pytest
from pyspark.sql import SparkSession
from scripts.quality_engine import QualityEngine
from scripts.anomaly_detector import detect_anomalies
from scripts.alert_manager import run_alerting

def test_full_observability_pipeline(spark: SparkSession):
    """
    Integration test validating that running the QualityEngine, AnomalyDetector, 
    and AlertManager generates allconformed Silver and Gold Delta tables.
    """
    # 1. Run Engine
    engine = QualityEngine(rules_dir="rules", data_dir="data")
    engine.run_all()
    
    # 2. Run Anomaly Detector
    detect_anomalies()
    
    # 3. Run Alert Manager
    run_alerting()
    
    # 4. Verifications
    silver_customers_path = "data/delta/silver_customers"
    gold_metrics_path = "data/delta/gold_quality_metrics"
    gold_anomalies_path = "data/delta/gold_anomaly_report"
    gold_incidents_path = "data/delta/gold_incident_logs"
    
    assert os.path.exists(silver_customers_path)
    assert os.path.exists(gold_metrics_path)
    assert os.path.exists(gold_anomalies_path)
    assert os.path.exists(gold_incidents_path)
    
    # Load and verify counts
    metrics_df = spark.read.format("delta").load(gold_metrics_path)
    assert metrics_df.count() > 0
