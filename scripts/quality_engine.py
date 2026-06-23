import pandas as pd
import json
from datetime import datetime


# ==========================================
# LOAD RULE FILES
# ==========================================

with open("rules/schema_rules.json") as f:
    schema_rules = json.load(f)

with open("rules/null_rules.json") as f:
    null_rules = json.load(f)

with open("rules/business_rules.json") as f:
    business_rules = json.load(f)


# ==========================================
# LOAD DATASETS
# ==========================================

datasets = {
    "customers": pd.read_csv("data/customers.csv"),
    "orders": pd.read_csv("data/orders.csv"),
    "payments": pd.read_csv("data/payments.csv"),
    "products": pd.read_csv("data/products.csv")
}


quality_results = []


# ==========================================
# QUALITY SCORE FUNCTION
# ==========================================

def calculate_quality_score(total_rows, failed_rows):
    valid_rows = total_rows - failed_rows

    if total_rows == 0:
        return 0

    return round((valid_rows / total_rows) * 100, 2)


# ==========================================
# SCHEMA VALIDATION
# ==========================================

def validate_schema(name, df):
    expected = schema_rules[name]["required_columns"]
    actual = list(df.columns)

    missing = set(expected) - set(actual)
    extra = set(actual) - set(expected)

    failed_rows = len(missing) + len(extra)

    quality_results.append({
        "dataset_name": name,
        "check_type": "schema_check",
        "failed_rows": failed_rows,
        "total_rows": len(df),
        "quality_score": calculate_quality_score(len(df), failed_rows)
    })


# ==========================================
# NULL VALIDATION
# ==========================================

def validate_nulls(name, df):
    failed_rows = 0

    for column in null_rules[name]:
        failed_rows += df[column].isnull().sum()

    quality_results.append({
        "dataset_name": name,
        "check_type": "null_check",
        "failed_rows": int(failed_rows),
        "total_rows": len(df),
        "quality_score": calculate_quality_score(len(df), failed_rows)
    })


# ==========================================
# DUPLICATE VALIDATION
# ==========================================

def validate_duplicates(name, df):
    key_map = {
        "customers": "customer_id",
        "orders": "order_id",
        "payments": "payment_id",
        "products": "product_id"
    }

    key = key_map[name]

    duplicates = df.duplicated(subset=[key]).sum()

    quality_results.append({
        "dataset_name": name,
        "check_type": "duplicate_check",
        "failed_rows": int(duplicates),
        "total_rows": len(df),
        "quality_score": calculate_quality_score(len(df), duplicates)
    })


# ==========================================
# BUSINESS RULE VALIDATION
# ==========================================

def validate_business_rules(name, df):
    if name not in business_rules:
        return

    total_failed = 0

    rules = business_rules[name]

    for column, rule_data in rules.items():

        rule = rule_data["rule"]
        value = rule_data["value"]

        if rule == "greater_than":
            failed = (df[column] <= value).sum()
            total_failed += failed

    quality_results.append({
        "dataset_name": name,
        "check_type": "business_rule_check",
        "failed_rows": int(total_failed),
        "total_rows": len(df),
        "quality_score": calculate_quality_score(len(df), total_failed)
    })


# ==========================================
# LATE ARRIVAL CHECK
# ==========================================

def validate_late_arrivals(df):
    allowed_days = 30

    today = datetime(2024, 9, 1)

    df["payment_date"] = pd.to_datetime(df["payment_date"])

    days_diff = (today - df["payment_date"]).dt.days

    late_records = (days_diff > allowed_days).sum()

    quality_results.append({
        "dataset_name": "payments",
        "check_type": "late_arrival_check",
        "failed_rows": int(late_records),
        "total_rows": len(df),
        "quality_score": calculate_quality_score(len(df), late_records)
    })


# ==========================================
# RUN VALIDATIONS
# ==========================================

for dataset_name, df in datasets.items():
    validate_schema(dataset_name, df)
    validate_nulls(dataset_name, df)
    validate_duplicates(dataset_name, df)
    validate_business_rules(dataset_name, df)

validate_late_arrivals(datasets["payments"])


# ==========================================
# GENERATE REPORT
# ==========================================

quality_report = pd.DataFrame(quality_results)

quality_report["severity"] = quality_report["failed_rows"].apply(
    lambda x: "HIGH" if x > 3 else "LOW"
)

quality_report["run_timestamp"] = datetime.now()

quality_report.to_csv(
    "data_quality_report.csv",
    index=False
)

print(quality_report)
print("\nQuality report generated successfully.")
