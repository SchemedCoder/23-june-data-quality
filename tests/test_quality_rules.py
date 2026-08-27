import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from scripts.quality_engine import QualityEngine

def test_calculate_quality_score():
    engine = QualityEngine(rules_dir="rules", data_dir="data")
    
    # 0 failures -> 100 score
    assert engine.calculate_quality_score(100, 0) == 100.0
    
    # 10 failures in 100 rows -> 90 score
    assert engine.calculate_quality_score(100, 10) == 90.0
    
    # Empty DataFrame -> 100 score (or fallback)
    assert engine.calculate_quality_score(0, 0) == 100.0

def test_null_validation_logic(spark: SparkSession):
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_amount", DoubleType(), True)
    ])
    
    # 3 total rows, 1 row has null order_id (primary key), 1 has null order_amount
    data = [
        (1, 101, 150.00),
        (None, 102, 50.00),
        (3, 103, None)
    ]
    
    df = spark.createDataFrame(data, schema)
    engine = QualityEngine(rules_dir="rules", data_dir="data")
    
    # Null rules for orders require: "order_id", "customer_id", "order_amount"
    engine.validate_nulls("orders", df)
    
    # We should have appended 1 null check result
    assert len(engine.results) == 1
    assert engine.results[0]["check_type"] == "null_check"
    assert engine.results[0]["failed_rows"] == 2
    assert engine.results[0]["quality_score"] == 33.33  # 1 valid row out of 3 = 33.33%

def test_duplicate_validation_logic(spark: SparkSession):
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True)
    ])
    
    # 4 total rows, customer_id 101 appears twice (1 duplicate failure)
    data = [
        (101, "Alice", "alice@test.com"),
        (102, "Bob", "bob@test.com"),
        (101, "Alice Dup", "alice2@test.com"),
        (103, "Charlie", "charlie@test.com")
    ]
    
    df = spark.createDataFrame(data, schema)
    engine = QualityEngine(rules_dir="rules", data_dir="data")
    
    engine.validate_duplicates("customers", df)
    
    assert len(engine.results) == 1
    assert engine.results[0]["check_type"] == "duplicate_check"
    assert engine.results[0]["failed_rows"] == 1
    assert engine.results[0]["quality_score"] == 75.00  # 3 unique rows out of 4 = 75%
