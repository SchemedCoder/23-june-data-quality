# Data Quality Monitoring Platform

An end-to-end **Data Quality & Observability Platform** built using **Python, SQL, Pandas, and CI/CD automation** to detect data quality issues before bad data reaches analytics systems.

This project simulates a real enterprise environment where multiple datasets (customers, orders, payments, products) are ingested and automatically validated using configurable quality rules.

---

## Why This Project?

Modern data platforms often fail because of bad data rather than bad pipelines.

Common production issues include:

- Missing primary keys
- Duplicate records
- Negative transaction values
- Schema drift
- Late-arriving data
- Broken downstream dashboards

This platform helps detect such failures automatically and generate alerts for data teams.

---

## Key Features

- Schema validation
- Null checks
- Duplicate detection
- Business rule validation
- Late-arriving record detection
- Data quality scoring
- Anomaly detection
- Automated incident logging
- CI/CD testing with GitHub Actions

---

## Architecture

```text
Source Files (CSV / APIs / DB)
            ↓
      Bronze Raw Layer
            ↓
    Validation Engine
            ↓
     Quality Rule Checks
            ↓
      Silver Clean Layer
            ↓
     Quality Metrics Table
            ↓
 Anomaly Detection & Alerts
            ↓
   Dashboard / Monitoring
```

---

## Tech Stack

| Category | Tools |
|----------|-------|
| Language | Python |
| Data Processing | Pandas |
| Storage | CSV / SQL Warehouse |
| Validation | Custom Rules Engine |
| Analytics | SQL |
| Testing | Pytest |
| CI/CD | GitHub Actions |

---

## Repository Structure

```text
data-quality-monitoring-platform/

├── README.md
├── requirements.txt
├── .env.example

├── data/
│   ├── customers.csv
│   ├── orders.csv
│   ├── payments.csv
│   └── products.csv

├── outputs/
│   ├── data_quality_report.csv
│   ├── anomaly_report.csv
│   └── incident_log.csv

├── rules/
│   ├── schema_rules.json
│   ├── null_rules.json
│   └── business_rules.json

├── scripts/
│   ├── quality_engine.py
│   ├── anomaly_detector.py
│   └── alert_manager.py

├── sql/
│   ├── bronze.sql
│   ├── silver.sql
│   ├── quality_metrics.sql
│   └── dashboard_queries.sql

├── tests/
├── docs/
└── .github/
```

---

## Datasets

The platform validates four enterprise datasets:

### customers.csv
Customer master records.

### orders.csv
Transaction/order data.

### payments.csv
Payment settlement records.

### products.csv
Product catalog data.

---

## Quality Rules

### Schema Validation
Detects missing or extra columns.

Example:
```text
Expected: customer_id, email
Received: customer_id, email, phone
```

---

### Null Validation
Critical fields cannot be null.

Examples:
- customer_id
- order_id
- payment_id

---

### Duplicate Validation
Detects repeated business keys.

Examples:
- Duplicate customer_id
- Duplicate order_id

---

### Business Rule Validation

Examples:

```python
order_amount > 0
payment_amount > 0
price > 0
```

---

### Late Arrival Detection

Records breaching SLA thresholds are flagged.

Example:

```text
Allowed SLA = 30 days
```

---

## Data Quality Score

Quality score is calculated as:

```text
(valid_rows / total_rows) × 100
```

Example:

| Dataset | Score |
|---------|------:|
| Customers | 91.67 |
| Orders | 92.31 |
| Payments | 0.00 |
| Products | 90.91 |

---

## Sample Output

### Quality Report

| Dataset | Check Type | Failed Rows | Score |
|---------|------------|------------:|------:|
| customers | null_check | 1 | 91.67 |
| orders | duplicate_check | 1 | 92.31 |
| payments | late_arrival_check | 12 | 0.00 |

---

### Anomaly Report

Detected anomalies include:

- Low quality score
- High failed row count
- Severity classification

---

### Incident Log

Generated alert logs for production monitoring.

Example:

```text
ALERT TRIGGERED
Dataset : payments
Issue   : Low Quality Score
Severity: HIGH
```

---

## How to Run

Install dependencies:

```bash
pip install -r requirements.txt
```

Run validation engine:

```bash
python scripts/quality_engine.py
```

Run anomaly detection:

```bash
python scripts/anomaly_detector.py
```

Run alert manager:

```bash
python scripts/alert_manager.py
```

Run tests:

```bash
pytest tests/
```

---

## Business Value

This platform improves:

- Data reliability
- Pipeline observability
- Root cause analysis
- Incident response
- Production monitoring

---

## Resume Impact

This project demonstrates strong skills in:

- Data Engineering
- Data Observability
- Production Monitoring
- Data Reliability Engineering
- Testing & CI/CD

---

## Future Enhancements

- Airflow orchestration
- Slack/email alerts
- Great Expectations integration
- Real-time monitoring with Kafka
- ML-based anomaly detection



