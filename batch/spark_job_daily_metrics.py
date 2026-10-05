# pyright: reportMissingImports=false
"""
OmniFlow Apache Spark Batch Processing Job
Performs large-scale distributed aggregation over Data Lake Parquet files:
- RFM (Recency, Frequency, Monetary) Customer Value Segmentation
- Category & Daily Performance Aggregation
- Fraud Anomaly Batch Analytics
"""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# PySpark imports (optional in local standalone mode, containerized in Spark cluster)
try:
    from pyspark.sql import SparkSession  # type: ignore
    from pyspark.sql import functions as F  # type: ignore
    from pyspark.sql.window import Window  # type: ignore
    SPARK_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    SPARK_AVAILABLE = False


def create_spark_session(app_name="OmniFlow-Batch-Aggregator"):
    """Initializes Spark Session with S3/MinIO and local configuration."""
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is not installed in current environment.")

    spark_master = os.getenv("SPARK_MASTER_URL", "local[*]")
    return SparkSession.builder \
        .appName(app_name) \
        .master(spark_master) \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()


def run_spark_batch():
    """Executes distributed batch job with PySpark."""
    print("[*] Starting PySpark Batch Aggregator...")
    spark = create_spark_session()
    
    lake_root = os.getenv("DATA_LAKE_PATH", "/opt/bitnami/spark/data_lake")
    raw_orders_path = f"{lake_root}/raw/orders/*/*/*/*.parquet"

    print(f"[*] Reading raw orders from: {raw_orders_path}")
    df_orders = spark.read.parquet(raw_orders_path)

    # 1. Daily Sales Summary
    df_daily = df_orders.groupBy(
        F.to_date("created_at").alias("order_date"),
        "country"
    ).agg(
        F.count("order_id").alias("total_orders"),
        F.round(F.sum("total_amount"), 2).alias("gross_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        F.round(F.sum("discount"), 2).alias("total_discounts"),
        F.sum(F.when(F.col("is_fraud_suspect"), 1).otherwise(0)).alias("fraud_suspect_count")
    )

    gold_daily_path = f"{lake_root}/gold/fct_daily_sales"
    print(f"[*] Writing Gold Daily Sales to: {gold_daily_path}")
    df_daily.write.mode("overwrite").parquet(gold_daily_path)

    # 2. Category Performance Summary
    category_col = "primary_category" if "primary_category" in df_orders.columns else "country"
    df_category = df_orders.groupBy(category_col).agg(
        F.count("order_id").alias("total_orders"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_basket")
    )
    gold_cat_path = f"{lake_root}/gold/fct_category_sales"
    print(f"[*] Writing Gold Category Sales to: {gold_cat_path}")
    df_category.write.mode("overwrite").parquet(gold_cat_path)

    # 3. RFM Customer Segmentation
    max_date = df_orders.select(F.max("created_at")).collect()[0][0]
    
    df_rfm = df_orders.groupBy("customer_id").agg(
        F.datediff(F.to_date(F.lit(str(max_date))), F.to_date(F.max("created_at"))).alias("recency_days"),
        F.count("order_id").alias("frequency"),
        F.round(F.sum("total_amount"), 2).alias("monetary")
    ).withColumn(
        "customer_segment",
        F.when((F.col("frequency") >= 4) & (F.col("monetary") >= 1500), "Champions")
        .when((F.col("frequency") >= 2) & (F.col("monetary") >= 500), "Loyal Customers")
        .when((F.col("recency_days") <= 7), "New Active")
        .otherwise("Standard")
    )

    gold_rfm_path = f"{lake_root}/gold/fct_customer_rfm"
    print(f"[*] Writing Gold RFM Mart to: {gold_rfm_path}")
    df_rfm.write.mode("overwrite").parquet(gold_rfm_path)

    print("[OK] PySpark Batch Processing completed successfully.")
    spark.stop()


if __name__ == "__main__":
    if SPARK_AVAILABLE:
        try:
            run_spark_batch()
        except Exception as e:
            print(f"[!] PySpark error ({e}). Falling back to columnar batch runner...")
            from batch.run_batch import execute_batch
            execute_batch()
    else:
        print("[!] PySpark not available on host. Running high-performance columnar batch aggregator...")
        from batch.run_batch import execute_batch
        execute_batch()
