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
import logging
logger = logging.getLogger("AlertManager")

# Load environment configs
load_dotenv()

def run_alerting(anomaly_path="data/delta/gold_anomaly_report", incident_path="data/delta/gold_incident_logs"):
    """
    Reads detected anomalies from Delta storage, sends structured alerts,
    and logs active incidents to a Delta Lake registry.
    """
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Executing alert triggers...")
    
    if not os.path.exists(anomaly_path):
        logger.warning(f"No anomaly data found at {anomaly_path}. Skipping alerts.")
        return
        
    anomalies_df = spark.read.format("delta").load(anomaly_path)
    
    if anomalies_df.count() == 0:
        logger.info("No anomalies detected. Alerts will not be sent.")
        return
        
    # Convert to Pandas to iterate and simulate sending email/Slack notifications
    anomalies_pd = anomalies_df.toPandas()
    
    alert_email = os.getenv("ALERT_EMAIL", "data-team@company.com")
    slack_webhook = os.getenv("SLACK_WEBHOOK", "https://dummy-slack-webhook")
    
    logger.info(f"Configured Alerts Recipients: Email={alert_email}, Slack={slack_webhook}")
    
    incidents = []
    
    for _, row in anomalies_pd.iterrows():
        alert_msg = f"""
==================================================
!!! [DATA QUALITY ALERT] !!!
==================================================
Dataset Name: {row['dataset_name']}
Check Issue : {row['issue']}
Metric Value: {row['metric_value']}
Severity    : {row['severity']}
Notification: Dispatched to {alert_email}
==================================================
"""
        print(alert_msg)
        
        incidents.append({
            "dataset_name": row["dataset_name"],
            "issue": row["issue"],
            "severity": row["severity"]
        })
        
    # Create incident log dataframe
    incident_spark_df = spark.createDataFrame(incidents)
    
    # Add logged_at timestamp
    from pyspark.sql.functions import current_timestamp
    final_incidents = incident_spark_df.withColumn("logged_at", current_timestamp())
    
    # Write to Gold Delta Table for incident management audits
    final_incidents.write.format("delta").mode("overwrite").save(incident_path)
    logger.info(f"Incident audits saved to Delta Table at {incident_path}")
    
    # Save local CSV log for dashboard compatibility
    final_incidents.toPandas().to_csv("incident_log.csv", index=False)
    logger.info("Local incident_log.csv written.")

if __name__ == "__main__":
    run_alerting()
