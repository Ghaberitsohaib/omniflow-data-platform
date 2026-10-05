"""
OmniFlow Historical Data Seeder
Populates the Data Lake and Warehouse with 30 days of realistic e-commerce transactions
so analytical charts, trends, and models are rich and populated out-of-the-box.
"""

import os
import sys
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from streaming.producer import generate_order_event
from streaming.consumer_to_lake import process_orders_microbatch
from ingestion.dlt_pipeline import run_pipeline as run_dlt


def seed_historical_orders(num_orders=250):
    if hasattr(sys.stdout, 'reconfigure'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass

    print(f"[*] Seeding {num_orders} historical orders across the last 30 days...")
    orders = []
    base_date = datetime.now(timezone.utc)

    for i in range(num_orders):
        order = generate_order_event()
        # Distribute date over last 30 days
        days_ago = random.randint(0, 30)
        hours_ago = random.randint(0, 23)
        order_time = base_date - timedelta(days=days_ago, hours=hours_ago, minutes=random.randint(0, 59))
        order["created_at"] = order_time.strftime("%Y-%m-%d %H:%M:%S")
        order["timestamp"] = order_time.isoformat()
        orders.append(order)

    # Ingest in microbatches
    batch_size = 50
    for idx in range(0, len(orders), batch_size):
        chunk = orders[idx:idx + batch_size]
        process_orders_microbatch(chunk)

    print(f"[OK] Successfully seeded {len(orders)} historical orders.")


def main():
    print("[*] =========================================")
    print("[*] OmniFlow Complete Seeding Sequence")
    print("[*] =========================================")
    
    # 1. Run dlt to ingest customers, products, exchange rates
    print("\nStep 1: Running dltHub Ingestion...")
    run_dlt(destination_name="duckdb")

    # 2. Seed orders
    print("\nStep 2: Generating Historical Orders...")
    seed_historical_orders(num_orders=300)

    print("\n[OK] Seeding complete! You can now run batch processing and dbt.")


if __name__ == "__main__":
    main()
