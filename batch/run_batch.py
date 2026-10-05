"""
OmniFlow Unified Batch Job Executor
Seamlessly executes the batch aggregation pipeline.
Uses PySpark if available (e.g. inside Docker Spark cluster),
or high-speed columnar DuckDB/Polars for instant local execution.
"""

import os
import sys
import glob
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

DATA_LAKE_PATH = PROJECT_ROOT / "data_lake"
WAREHOUSE_PATH = PROJECT_ROOT / "data_warehouse" / "warehouse.duckdb"


def execute_batch():
    print("[*] Starting OmniFlow Batch Processing Engine...")
    raw_files = list(DATA_LAKE_PATH.glob("raw/orders/*/*/*/*.parquet"))

    if not raw_files and not WAREHOUSE_PATH.exists():
        print("[-] No raw orders found in Lake. Run streaming producer and consumer first.")
        return False

    gold_dir = DATA_LAKE_PATH / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(WAREHOUSE_PATH))

    # Auto-migrate raw_orders schema if needed
    try:
        con.execute("ALTER TABLE raw_orders ADD COLUMN IF NOT EXISTS primary_category VARCHAR DEFAULT 'General'")
    except Exception:
        pass

    # 1. Daily Sales Aggregation
    print("[*] Computing Daily Sales Aggregations...")
    con.execute("""
        CREATE OR REPLACE TABLE gold_daily_sales AS
        SELECT 
            CAST(created_at AS DATE) AS order_date,
            country,
            COUNT(order_id) AS total_orders,
            ROUND(SUM(total_amount), 2) AS gross_revenue,
            ROUND(AVG(total_amount), 2) AS avg_order_value,
            ROUND(SUM(discount), 2) AS total_discounts,
            COUNT(CASE WHEN is_fraud_suspect = true THEN 1 END) AS fraud_count
        FROM raw_orders
        GROUP BY 1, 2
        ORDER BY 1 DESC, 4 DESC
    """)

    # Export Gold Daily Sales to Data Lake Parquet
    daily_parquet = gold_dir / "fct_daily_sales.parquet"
    con.execute(f"COPY gold_daily_sales TO '{str(daily_parquet).replace(chr(92), '/')}' (FORMAT PARQUET)")
    print(f"[+] Saved Gold Daily Sales to Lake: {daily_parquet.name}")

    # 2. RFM Customer Value Segmentation
    print("[*] Computing RFM Customer Segments...")
    con.execute("""
        CREATE OR REPLACE TABLE gold_customer_rfm AS
        WITH customer_stats AS (
            SELECT 
                customer_id,
                MAX(created_at) AS last_order_date,
                COUNT(order_id) AS frequency,
                ROUND(SUM(total_amount), 2) AS monetary_value
            FROM raw_orders
            GROUP BY customer_id
        ),
        max_reference AS (
            SELECT MAX(created_at) AS ref_date FROM raw_orders
        )
        SELECT 
            cs.customer_id,
            DATE_DIFF('day', cs.last_order_date, r.ref_date) AS recency_days,
            cs.frequency,
            cs.monetary_value,
            CASE 
                WHEN cs.frequency >= 4 AND cs.monetary_value >= 1500 THEN 'Champions'
                WHEN cs.frequency >= 2 AND cs.monetary_value >= 500 THEN 'Loyal Customers'
                WHEN DATE_DIFF('day', cs.last_order_date, r.ref_date) <= 5 THEN 'New Active'
                WHEN cs.monetary_value >= 1000 THEN 'Big Spenders'
                ELSE 'Standard'
            END AS customer_segment,
            CURRENT_TIMESTAMP AS calculated_at
        FROM customer_stats cs
        CROSS JOIN max_reference r
    """)

    rfm_parquet = gold_dir / "fct_customer_rfm.parquet"
    con.execute(f"COPY gold_customer_rfm TO '{str(rfm_parquet).replace(chr(92), '/')}' (FORMAT PARQUET)")
    print(f"[+] Saved Gold Customer RFM to Lake: {rfm_parquet.name}")

    # 3. Category Performance
    print("[*] Computing Category Sales Performance...")
    con.execute("""
        CREATE OR REPLACE TABLE gold_category_sales AS
        SELECT 
            COALESCE(primary_category, 'General') AS category,
            COUNT(order_id) AS total_orders,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(total_amount), 2) AS avg_basket
        FROM raw_orders
        GROUP BY 1
        ORDER BY total_revenue DESC
    """)

    category_parquet = gold_dir / "fct_category_sales.parquet"
    con.execute(f"COPY gold_category_sales TO '{str(category_parquet).replace(chr(92), '/')}' (FORMAT PARQUET)")
    print(f"[+] Saved Gold Category Sales to Lake: {category_parquet.name}")

    con.close()
    print("[OK] Batch Processing completed successfully! Gold layer updated.")
    return True


if __name__ == "__main__":
    execute_batch()
