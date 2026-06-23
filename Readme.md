

## Overview

This project demonstrates an end-to-end Data Quality Monitoring Platform designed to validate, profile, monitor, and alert on data quality issues across multiple enterprise datasets.

The platform detects common data problems including:

* Null values
* Duplicate records
* Invalid business rules
* Schema drift
* Late arriving records
* Data anomalies

The solution helps improve data reliability before datasets move into analytics or machine learning systems.

---

## Problem Statement

Modern organizations ingest data from multiple sources:

* CRM systems
* Order management systems
* Payment gateways
* Product catalog databases
* Web applications

Poor quality data can cause:

* Incorrect dashboards
* Broken ETL jobs
* Revenue miscalculations
* Failed machine learning models
* Bad business decisions

This platform identifies and reports such issues automatically.

---

## Architecture

Source Systems

↓

Bronze Raw Layer

↓

Data Validation Engine

↓

Quality Rules Engine

↓

Silver Clean Layer

↓

Quality Metrics Table

↓

Dashboard / Alerts

---

## Technology Stack

* Python
* Pandas
* PySpark
* SQL
* Delta Lake / Fabric
* Airflow
* GitHub Actions
* Pytest

---

## Project Structure

data-quality-monitoring-platform/

├── README.md
├── requirements.txt
├── .env.example

├── data/
├── rules/
├── scripts/
├── sql/
├── tests/
├── docs/
└── .github/

---

## Data Sources

### Customers

Master customer records.

### Orders

Customer transaction records.

### Payments

Payment settlement records.

### Products

Product catalog records.

---

## Quality Rules

### Null Validation

Primary keys cannot be null.

### Duplicate Validation

No duplicate business keys.

### Schema Validation

Columns and data types must match expected schema.

### Business Rule Validation

Examples:

* order_amount > 0
* payment_amount > 0
* discount <= product_price

### Late Arrival Detection

Flag records arriving after SLA threshold.

---

## Quality Metrics

Generated metrics include:

* Total rows
* Valid rows
* Failed rows
* Null counts
* Duplicate counts
* Quality score
* Severity level

---

## Quality Score Formula

Quality Score =

(valid_rows / total_rows) * 100

---

## Gold Layer Output

Final quality metrics table includes:

* dataset_name
* check_type
* failed_rows
* total_rows
* quality_score
* severity
* run_timestamp

---

## Example Use Cases

* Data observability
* ETL validation
* Production monitoring
* Root cause analysis
* Data governance

---

## How To Run

1. Load source datasets
2. Apply validation rules
3. Generate quality metrics
4. Save clean datasets
5. Trigger alerts for failures
6. Review dashboard

---

## Future Enhancements

* Slack alerts
* Email notifications
* Great Expectations integration
* ML anomaly detection
* Real-time monitoring
