import pandas as pd


# ==========================================
# LOAD QUALITY REPORT
# ==========================================

df = pd.read_csv("data_quality_report.csv")


# ==========================================
# DETECT QUALITY SCORE ANOMALIES
# ==========================================

THRESHOLD_SCORE = 95
THRESHOLD_FAILURES = 5

anomalies = []


for _, row in df.iterrows():

    if row["quality_score"] < THRESHOLD_SCORE:
        anomalies.append({
            "dataset_name": row["dataset_name"],
            "issue": "Low Quality Score",
            "metric_value": row["quality_score"],
            "severity": "HIGH"
        })

    if row["failed_rows"] > THRESHOLD_FAILURES:
        anomalies.append({
            "dataset_name": row["dataset_name"],
            "issue": "High Failed Rows",
            "metric_value": row["failed_rows"],
            "severity": "HIGH"
        })


# ==========================================
# SAVE ANOMALIES
# ==========================================

anomaly_df = pd.DataFrame(anomalies)

anomaly_df.to_csv(
    "anomaly_report.csv",
    index=False
)

print(anomaly_df)
print("\nAnomaly detection completed.")
