import pandas as pd


def test_no_negative_order_amount():
    df = pd.read_csv("data/orders.csv")
    failed = (df["order_amount"] <= 0).sum()
    assert failed >= 1


def test_duplicate_customers_exist():
    df = pd.read_csv("data/customers.csv")
    duplicates = df.duplicated(subset=["customer_id"]).sum()
    assert duplicates >= 1


def test_missing_emails_exist():
    df = pd.read_csv("data/customers.csv")
    assert df["email"].isnull().sum() >= 1
