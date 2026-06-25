import pandas as pd
from datetime import datetime


# ==========================================
# LOAD ANOMALY REPORT
# ==========================================

anomalies = pd.read_csv("anomaly_report.csv")


# ==========================================
# ALERT FUNCTION
# ==========================================

def send_alert(alert_message):
    print("\n================ ALERT ================")
    print(alert_message)
    print("=======================================\n")


incident_logs = []


# ==========================================
# PROCESS ALERTS
# ==========================================

for _, row in anomalies.iterrows():

    message = f"""
ALERT TRIGGERED

Dataset  : {row['dataset_name']}
Issue    : {row['issue']}
Value    : {row['metric_value']}
Severity : {row['severity']}
Timestamp: {datetime.now()}
"""

    send_alert(message)

    incident_logs.append({
        "dataset_name": row["dataset_name"],
        "issue": row["issue"],
        "severity": row["severity"],
        "logged_at": datetime.now()
    })


# ==========================================
# SAVE INCIDENT LOG
# ==========================================

incident_df = pd.DataFrame(incident_logs)

incident_df.to_csv(
    "incident_log.csv",
    index=False
)

print("Incident log generated successfully.")
