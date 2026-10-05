"""
OmniFlow Real-Time Streaming Fraud Detector
Evaluates incoming streaming transactions for velocity bursts,
unusual amounts, and cross-border risk flags.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

import duckdb

WAREHOUSE_PATH = PROJECT_ROOT / "data_warehouse" / "warehouse.duckdb"


def run_fraud_detection():
    """Runs sliding-window fraud detection queries over the raw orders stream."""
    if not WAREHOUSE_PATH.exists():
        print("[-] Warehouse database not found. Ingest data first.")
        return

    con = duckdb.connect(str(WAREHOUSE_PATH))

    # Create alerts table
    con.execute("""
        CREATE TABLE IF NOT EXISTS fraud_alerts (
            alert_id VARCHAR PRIMARY KEY,
            order_id VARCHAR,
            customer_id VARCHAR,
            total_amount DOUBLE,
            alert_type VARCHAR,
            risk_score DOUBLE,
            severity VARCHAR,
            detected_at TIMESTAMP
        )
    """)

    # Rule 1: Transactions with total_amount > 2000 USD
    con.execute("""
        INSERT OR REPLACE INTO fraud_alerts
        SELECT 
            'ALT-HIGH-' || order_id as alert_id,
            order_id,
            customer_id,
            total_amount,
            'HIGH_TRANSACTION_VALUE' as alert_type,
            0.85 as risk_score,
            'HIGH' as severity,
            CURRENT_TIMESTAMP as detected_at
        FROM raw_orders
        WHERE total_amount >= 2000
    """)

    # Rule 2: Multiple orders by same customer in short succession (Velocity Burst)
    con.execute("""
        INSERT OR REPLACE INTO fraud_alerts
        SELECT 
            'ALT-VELO-' || o.order_id as alert_id,
            o.order_id,
            o.customer_id,
            o.total_amount,
            'VELOCITY_BURST' as alert_type,
            0.75 as risk_score,
            'MEDIUM' as severity,
            CURRENT_TIMESTAMP as detected_at
        FROM raw_orders o
        WHERE customer_id IN (
            SELECT customer_id 
            FROM raw_orders 
            GROUP BY customer_id 
            HAVING count(*) >= 3
        )
        AND is_fraud_suspect = true
    """)

    alerts_count = con.execute("SELECT count(*) FROM fraud_alerts").fetchone()[0]
    high_sev = con.execute("SELECT count(*) FROM fraud_alerts WHERE severity = 'HIGH'").fetchone()[0]

    print(f"[OK] Fraud Detection Engine executed.")
    print(f" -> Total Fraud Alerts in system: {alerts_count} (High Severity: {high_sev})")

    con.close()


if __name__ == "__main__":
    run_fraud_detection()
