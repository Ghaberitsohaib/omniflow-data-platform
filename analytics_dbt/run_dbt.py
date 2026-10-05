"""
OmniFlow dbt Analytics Runner
Executes dbt transformations against the analytical warehouse (DuckDB / PostgreSQL).
Builds staging views, dimensional tables, fact tables, and runs data quality tests.
"""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

import duckdb

WAREHOUSE_PATH = PROJECT_ROOT / "data_warehouse" / "warehouse.duckdb"


def run_dbt_transformations():
    if hasattr(sys.stdout, 'reconfigure'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass

    print("[*] Starting dbt Analytics Engineering Pipeline...")
    if not WAREHOUSE_PATH.exists():
        print("[-] Warehouse database not found. Ingest data first.")
        return False

    con = duckdb.connect(str(WAREHOUSE_PATH))

    # Auto-migrate raw_orders schema if needed
    try:
        con.execute("ALTER TABLE raw_orders ADD COLUMN IF NOT EXISTS primary_category VARCHAR DEFAULT 'General'")
    except Exception:
        pass

    # Create analytics schema
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics")

    # 1. Staging Models
    print("[*] Building Staging Views (stg_orders, stg_customers, stg_products)...")
    con.execute("""
        CREATE OR REPLACE VIEW analytics.stg_orders AS
        SELECT
            order_id,
            customer_id,
            item_count,
            COALESCE(primary_category, 'General') AS primary_category,
            ROUND(subtotal, 2) AS subtotal_usd,
            ROUND(discount, 2) AS discount_usd,
            ROUND(tax, 2) AS tax_usd,
            ROUND(shipping, 2) AS shipping_usd,
            ROUND(total_amount, 2) AS total_amount_usd,
            LOWER(payment_method) AS payment_method,
            currency,
            city,
            country,
            device,
            ip_address,
            is_fraud_suspect,
            fraud_reason,
            order_status,
            CAST(created_at AS TIMESTAMP) AS created_at,
            CAST(created_at AS DATE) AS order_date
        FROM raw_orders
        WHERE order_id IS NOT NULL;
    """)

    con.execute("""
        CREATE OR REPLACE VIEW analytics.stg_customers AS
        SELECT
            customer_id,
            first_name,
            last_name,
            CONCAT(first_name, ' ', last_name) AS full_name,
            email,
            country,
            tier,
            loyalty_points,
            CAST(registered_at AS TIMESTAMP) AS registered_at
        FROM ecommerce_raw.customers
        WHERE customer_id IS NOT NULL;
    """)

    con.execute("""
        CREATE OR REPLACE VIEW analytics.stg_products AS
        SELECT
            product_id,
            name AS product_name,
            category,
            cost_price,
            retail_price,
            ROUND(retail_price - cost_price, 2) AS gross_margin_usd,
            ROUND(((retail_price - cost_price) / retail_price) * 100, 1) AS margin_percentage,
            stock_quantity,
            supplier
        FROM ecommerce_raw.products
        WHERE product_id IS NOT NULL;
    """)

    # 2. Dimensional Models
    print("[*] Materializing Mart Tables (dim_customers, dim_products)...")
    con.execute("""
        CREATE OR REPLACE TABLE analytics.dim_customers AS
        WITH customers AS (
            SELECT * FROM analytics.stg_customers
        ),
        order_aggregates AS (
            SELECT
                customer_id,
                COUNT(order_id) AS lifetime_orders,
                ROUND(SUM(total_amount_usd), 2) AS lifetime_spend_usd,
                ROUND(AVG(total_amount_usd), 2) AS avg_spend_per_order,
                MIN(created_at) AS first_order_date,
                MAX(created_at) AS last_order_date
            FROM analytics.stg_orders
            GROUP BY customer_id
        )
        SELECT
            c.customer_id,
            c.full_name,
            c.email,
            c.country,
            c.tier,
            c.loyalty_points,
            COALESCE(o.lifetime_orders, 0) AS lifetime_orders,
            COALESCE(o.lifetime_spend_usd, 0.0) AS lifetime_spend_usd,
            COALESCE(o.avg_spend_per_order, 0.0) AS avg_spend_per_order,
            o.first_order_date,
            o.last_order_date,
            CASE 
                WHEN COALESCE(o.lifetime_spend_usd, 0) >= 1500 THEN 'VIP Tier'
                WHEN COALESCE(o.lifetime_spend_usd, 0) >= 500 THEN 'High Value'
                WHEN COALESCE(o.lifetime_orders, 0) >= 1 THEN 'Active Buyer'
                ELSE 'Prospect'
            END AS customer_lifecycle_stage
        FROM customers c
        LEFT JOIN order_aggregates o ON c.customer_id = o.customer_id;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE analytics.dim_products AS
        SELECT
            product_id,
            product_name,
            category,
            cost_price,
            retail_price,
            gross_margin_usd,
            margin_percentage,
            stock_quantity,
            supplier,
            CASE 
                WHEN stock_quantity <= 30 THEN 'Low Stock'
                WHEN stock_quantity <= 100 THEN 'Optimal'
                ELSE 'Surplus'
            END AS inventory_health,
            CASE 
                WHEN retail_price >= 500 THEN 'Premium / Luxury'
                WHEN retail_price >= 100 THEN 'Mid-Range'
                ELSE 'Budget'
            END AS price_tier
        FROM analytics.stg_products;
    """)

    # 3. Fact Tables
    print("[*] Materializing Fact Tables (fct_daily_sales, fct_customer_rfm, fct_fraud_monitoring)...")
    con.execute("""
        CREATE OR REPLACE TABLE analytics.fct_daily_sales AS
        SELECT
            order_date,
            country,
            payment_method,
            COUNT(order_id) AS total_orders,
            ROUND(SUM(subtotal_usd), 2) AS gross_sales,
            ROUND(SUM(discount_usd), 2) AS total_discounts,
            ROUND(SUM(tax_usd), 2) AS total_tax,
            ROUND(SUM(shipping_usd), 2) AS total_shipping,
            ROUND(SUM(total_amount_usd), 2) AS net_revenue,
            ROUND(AVG(total_amount_usd), 2) AS avg_order_value,
            SUM(CASE WHEN is_fraud_suspect = true THEN 1 ELSE 0 END) AS flagged_fraud_orders
        FROM analytics.stg_orders
        GROUP BY order_date, country, payment_method;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE analytics.fct_customer_rfm AS
        WITH customer_orders AS (
            SELECT
                customer_id,
                MAX(created_at) AS last_purchase_at,
                COUNT(order_id) AS frequency,
                ROUND(SUM(total_amount_usd), 2) AS monetary_value
            FROM analytics.stg_orders
            GROUP BY customer_id
        ),
        reference_date AS (
            SELECT MAX(created_at) AS max_date FROM analytics.stg_orders
        )
        SELECT 
            c.customer_id,
            c.frequency,
            c.monetary_value,
            DATE_DIFF('day', c.last_purchase_at, r.max_date) AS recency_days,
            CASE 
                WHEN c.frequency >= 3 AND c.monetary_value >= 1200 THEN 'Champions'
                WHEN c.frequency >= 2 AND c.monetary_value >= 500 THEN 'Loyal Customers'
                WHEN DATE_DIFF('day', c.last_purchase_at, r.max_date) <= 3 THEN 'New Active'
                WHEN c.monetary_value >= 800 THEN 'High Value'
                ELSE 'Standard'
            END AS rfm_segment,
            CURRENT_TIMESTAMP AS snapshot_timestamp
        FROM customer_orders c
        CROSS JOIN reference_date r;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE analytics.fct_fraud_monitoring AS
        SELECT
            order_id,
            customer_id,
            total_amount_usd,
            payment_method,
            country,
            city,
            ip_address,
            device,
            COALESCE(fraud_reason, 'HIGH_VALUE_THRESHOLD') AS fraud_reason,
            CASE 
                WHEN total_amount_usd >= 4000 THEN 'CRITICAL'
                WHEN total_amount_usd >= 2000 THEN 'HIGH'
                ELSE 'MEDIUM'
            END AS risk_severity,
            created_at AS incident_time
        FROM analytics.stg_orders
        WHERE is_fraud_suspect = true OR total_amount_usd >= 2000;
    """)

    # 4. Data Quality Tests
    print("[*] Running dbt Data Quality Tests...")
    test_failures = con.execute("""
        SELECT COUNT(*) FROM analytics.stg_orders WHERE total_amount_usd <= 0
    """).fetchone()[0]

    null_pk_failures = con.execute("""
        SELECT COUNT(*) FROM analytics.dim_customers WHERE customer_id IS NULL
    """).fetchone()[0]

    if test_failures == 0 and null_pk_failures == 0:
        print("[OK] All dbt Data Quality Tests Passed! (0 assertions failed)")
    else:
        print(f"[!] Warning: Data test failures detected! (negative amounts: {test_failures}, null PKs: {null_pk_failures})")

    # Metrics summary
    dim_cust_cnt = con.execute("SELECT COUNT(*) FROM analytics.dim_customers").fetchone()[0]
    dim_prod_cnt = con.execute("SELECT COUNT(*) FROM analytics.dim_products").fetchone()[0]
    fct_sales_cnt = con.execute("SELECT COUNT(*) FROM analytics.fct_daily_sales").fetchone()[0]

    print(f"[OK] dbt build finished: dim_customers={dim_cust_cnt}, dim_products={dim_prod_cnt}, fct_daily_sales={fct_sales_cnt}")
    con.close()
    return True


if __name__ == "__main__":
    run_dbt_transformations()
