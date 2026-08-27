import os
import sys
import json
import urllib.request
from datetime import datetime

# 1. Clean sys.path of space-containing elements to prevent JVM launch issues on Windows
saved_sys_path = list(sys.path)
sys.path = [p for p in sys.path if " " not in p]

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date, count, sum as spark_sum, countDistinct
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType

sys.path = saved_sys_path
workspace_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if workspace_dir not in sys.path:
    sys.path.insert(0, workspace_dir)

import logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] - %(message)s')
logger = logging.getLogger("QualityEngine")

def get_spark_session(app_name="DataQualityEngine"):
    """
    Creates a local Spark Session pre-configured for Delta Lake.
    Downloads certified Delta JARs from Maven Central directly to bypass Windows space-in-path problems.
    """
    version = pyspark.__version__
    if version.startswith("3.5"):
        delta_ver = "3.1.0"
    elif version.startswith("3.4"):
        delta_ver = "2.4.0"
    else:
        delta_ver = "3.1.0"
        
    jars_dir = "jars"
    os.makedirs(jars_dir, exist_ok=True)
    
    jars = {
        f"delta-spark_2.12-{delta_ver}.jar": f"https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/{delta_ver}/delta-spark_2.12-{delta_ver}.jar",
        f"delta-storage-{delta_ver}.jar": f"https://repo1.maven.org/maven2/io/delta/delta-storage/{delta_ver}/delta-storage-{delta_ver}.jar"
    }
    
    local_jar_paths = []
    for jar_name, url in jars.items():
        dest_path = os.path.join(jars_dir, jar_name)
        local_jar_paths.append(dest_path)
        if not os.path.exists(dest_path):
            logger.info(f"Downloading Delta Jar {jar_name}...")
            urllib.request.urlretrieve(url, dest_path)
            
    jar_config = ",".join(local_jar_paths)
    warehouse_dir = "data/spark-warehouse"
    derby_dir = "data/derby"
    
    sys.path = [p for p in sys.path if " " not in p]
    for k in list(os.environ.keys()):
        if k.startswith("ANTIGRAVITY_"):
            del os.environ[k]
            
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", jar_config) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_dir}") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
        
    return spark

class QualityEngine:
    """
    Modular Data Quality validation engine implementing null, duplicate, schema,
    business rule and SLA late-arrival checks using Delta Lake audit logs.
    """
    def __init__(self, rules_dir="rules", data_dir="data"):
        self.rules_dir = rules_dir
        self.data_dir = data_dir
        self.spark = get_spark_session()
        self.results = []
        
        # Load rule configurations
        with open(os.path.join(rules_dir, "schema_rules.json")) as f:
            self.schema_rules = json.load(f)
        with open(os.path.join(rules_dir, "null_rules.json")) as f:
            self.null_rules = json.load(f)
        with open(os.path.join(rules_dir, "business_rules.json")) as f:
            self.business_rules = json.load(f)
            
    def calculate_quality_score(self, total_rows, failed_rows):
        if total_rows == 0:
            return 100.0
        valid_rows = max(0, total_rows - failed_rows)
        return round((valid_rows / total_rows) * 100, 2)
        
    def validate_schema(self, name, df):
        expected_cols = self.schema_rules[name]["required_columns"]
        actual_cols = df.columns
        
        missing = set(expected_cols) - set(actual_cols)
        extra = set(actual_cols) - set(expected_cols)
        failures = len(missing) + len(extra)
        total_rows = df.count()
        
        self.results.append({
            "dataset_name": name,
            "check_type": "schema_check",
            "failed_rows": int(failures),
            "total_rows": int(total_rows),
            "quality_score": float(self.calculate_quality_score(total_rows, failures))
        })
        logger.info(f"Schema Check: {name} (Failures: {failures})")
        
    def validate_nulls(self, name, df):
        total_rows = df.count()
        failures = 0
        
        for col_name in self.null_rules[name]:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                failures += null_count
                
        self.results.append({
            "dataset_name": name,
            "check_type": "null_check",
            "failed_rows": int(failures),
            "total_rows": int(total_rows),
            "quality_score": float(self.calculate_quality_score(total_rows, failures))
        })
        logger.info(f"Null Check: {name} (Failures: {failures})")
        
    def validate_duplicates(self, name, df):
        total_rows = df.count()
        key_map = {
            "customers": "customer_id",
            "orders": "order_id",
            "payments": "payment_id",
            "products": "product_id"
        }
        key = key_map[name]
        
        if key in df.columns:
            unique_count = df.select(key).distinct().count()
            failures = total_rows - unique_count
        else:
            failures = 0
            
        self.results.append({
            "dataset_name": name,
            "check_type": "duplicate_check",
            "failed_rows": int(failures),
            "total_rows": int(total_rows),
            "quality_score": float(self.calculate_quality_score(total_rows, failures))
        })
        logger.info(f"Duplicate Check: {name} (Failures: {failures})")
        
    def validate_business_rules(self, name, df):
        total_rows = df.count()
        failures = 0
        
        if name in self.business_rules:
            rules = self.business_rules[name]
            for col_name, rule_data in rules.items():
                if col_name in df.columns:
                    rule = rule_data["rule"]
                    val = rule_data["value"]
                    
                    if rule == "greater_than":
                        fail_count = df.filter(col(col_name) <= val).count()
                        failures += fail_count
                        
        self.results.append({
            "dataset_name": name,
            "check_type": "business_rule_check",
            "failed_rows": int(failures),
            "total_rows": int(total_rows),
            "quality_score": float(self.calculate_quality_score(total_rows, failures))
        })
        logger.info(f"Business Rule Check: {name} (Failures: {failures})")
        
    def validate_late_arrivals(self, df):
        total_rows = df.count()
        benchmark_date = datetime(2024, 9, 1)
        allowed_days = 30
        
        if "payment_date" in df.columns:
            # Calculate datediff between benchmark and payment date
            from pyspark.sql.functions import datediff
            # Convert string to date first
            df_date = df.withColumn("parsed_payment_date", to_date(col("payment_date"), "yyyy-MM-dd"))
            late_df = df_date.withColumn("days_diff", datediff(lit(benchmark_date), col("parsed_payment_date")))
            failures = late_df.filter(col("days_diff") > allowed_days).count()
        else:
            failures = 0
            
        self.results.append({
            "dataset_name": "payments",
            "check_type": "late_arrival_check",
            "failed_rows": int(failures),
            "total_rows": int(total_rows),
            "quality_score": float(self.calculate_quality_score(total_rows, failures))
        })
        logger.info(f"Late Arrival Check: payments (Failures: {failures})")
        
    def run_all(self):
        logger.info("Executing Data Quality Validation Engine...")
        
        # Load input datasets as spark dataframes (Bronze Layer representation)
        datasets = {
            "customers": self.spark.read.option("header", "true").csv(os.path.join(self.data_dir, "customers.csv")),
            "orders": self.spark.read.option("header", "true").csv(os.path.join(self.data_dir, "orders.csv")),
            "payments": self.spark.read.option("header", "true").csv(os.path.join(self.data_dir, "payments.csv")),
            "products": self.spark.read.option("header", "true").csv(os.path.join(self.data_dir, "products.csv"))
        }
        
        # Run validations on each
        for name, df in datasets.items():
            self.validate_schema(name, df)
            self.validate_nulls(name, df)
            self.validate_duplicates(name, df)
            self.validate_business_rules(name, df)
            
            # Write conformed clean dataset to Silver Delta Lake Table (removing null key records)
            # This is where the interviewer can check out how conformed Delta Tables are maintained!
            key_map = {"customers": "customer_id", "orders": "order_id", "payments": "payment_id", "products": "product_id"}
            key = key_map[name]
            clean_df = df.filter(col(key).isNotNull())
            
            silver_path = f"data/delta/silver_{name}"
            clean_df.write.format("delta").mode("overwrite").save(silver_path)
            logger.info(f"Conformed {name} written to Silver Delta table at {silver_path}")
            
        self.validate_late_arrivals(datasets["payments"])
        
        # Build quality report dataframe
        report_df = self.spark.createDataFrame(self.results)
        
        # Add severity and timestamp columns
        from pyspark.sql.functions import when
        final_report = report_df \
            .withColumn("severity", when(col("failed_rows") > 3, "HIGH").otherwise("LOW")) \
            .withColumn("run_timestamp", current_timestamp())
            
        # Write Quality Metrics Table (Gold Layer Output) to Delta Lake
        gold_metrics_path = "data/delta/gold_quality_metrics"
        final_report.write.format("delta").mode("overwrite").save(gold_metrics_path)
        logger.info(f"Gold Quality Metrics Table written to Delta Lake at {gold_metrics_path}")
        
        # Also write locally to CSV for reports/dashboards compatibility as defined in original script
        final_report.toPandas().to_csv("data_quality_report.csv", index=False)
        logger.info("Local data_quality_report.csv output written.")
        
        return final_report

if __name__ == "__main__":
    engine = QualityEngine()
    engine.run_all()
