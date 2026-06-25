import os


def test_quality_report_exists():
    assert os.path.exists("outputs/data_quality_report.csv")


def test_anomaly_report_exists():
    assert os.path.exists("outputs/anomaly_report.csv")


def test_incident_log_exists():
    assert os.path.exists("outputs/incident_log.csv")
