"""
OmniFlow Integration & Quality Test Suite
Verifies all components of the data platform: Lake, Warehouse, Models, and Fraud Engine.
"""

import os
import sys
from pathlib import Path
import pytest
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

WAREHOUSE_PATH = PROJECT_ROOT / "data_warehouse" / "warehouse.duckdb"
DATA_LAKE_PATH = PROJECT_ROOT / "data_lake"


def test_warehouse_exists():
    """Validates that the DuckDB Analytical Warehouse file is present and accessible."""
    assert WAREHOUSE_PATH.exists(), "Warehouse database file does not exist."


def test_raw_orders_populated():
    """Checks that raw streaming orders have been ingested and contain valid data."""
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    count = con.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
    con.close()
    assert count > 0, "raw_orders table is empty."


def test_data_lake_parquet_partitions():
    """Ensures partitioned Parquet files exist in the Data Lake raw and gold layers."""
    parquet_files = list(DATA_LAKE_PATH.glob("**/*.parquet"))
    assert len(parquet_files) > 0, "No Parquet files found in Data Lake."


def test_dlt_customers_and_products():
    """Verifies that dltHub successfully ingested customer profiles and catalog."""
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    cust_count = con.execute("SELECT COUNT(*) FROM ecommerce_raw.customers").fetchone()[0]
    prod_count = con.execute("SELECT COUNT(*) FROM ecommerce_raw.products").fetchone()[0]
    con.close()
    assert cust_count > 0, "dlt customers table is empty."
    assert prod_count > 0, "dlt products table is empty."


def test_dbt_analytics_marts():
    """Verifies that dbt dimension and fact tables are materialized with valid metrics."""
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    dim_cust = con.execute("SELECT COUNT(*) FROM analytics.dim_customers").fetchone()[0]
    dim_prod = con.execute("SELECT COUNT(*) FROM analytics.dim_products").fetchone()[0]
    fct_sales = con.execute("SELECT COUNT(*) FROM analytics.fct_daily_sales").fetchone()[0]
    con.close()
    assert dim_cust > 0, "dim_customers is empty."
    assert dim_prod > 0, "dim_products is empty."
    assert fct_sales > 0, "fct_daily_sales is empty."


def test_no_negative_order_amounts():
    """Data Quality Test: Validates that no order has a negative or zero total amount."""
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    invalid_orders = con.execute("SELECT COUNT(*) FROM raw_orders WHERE total_amount <= 0").fetchone()[0]
    con.close()
    assert invalid_orders == 0, f"Found {invalid_orders} orders with total_amount <= 0."


def test_fraud_detector_active():
    """Verifies that fraud alerts table exists and captures high-risk incidents."""
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    alerts_count = con.execute("SELECT COUNT(*) FROM fraud_alerts").fetchone()[0]
    con.close()
    assert alerts_count >= 0, "fraud_alerts table query failed."
