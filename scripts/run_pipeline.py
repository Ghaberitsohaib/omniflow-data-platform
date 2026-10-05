"""
OmniFlow Master End-to-End Pipeline Runner
Executes the full pipeline locally:
Ingestion (dlt) -> Streaming (Kafka/Lake) -> Batch (Spark) -> Analytics (dbt) -> Fraud Detection
"""

import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from scripts.seed_data import main as seed_all
from batch.run_batch import execute_batch
from analytics_dbt.run_dbt import run_dbt_transformations
from streaming.fraud_detector import run_fraud_detection


def run_all():
    if hasattr(sys.stdout, 'reconfigure'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass

    start_time = time.time()
    print("=================================================================")
    print("OMNIFLOW DATA PLATFORM - END-TO-END PIPELINE EXECUTION")
    print("=================================================================\n")

    print("[STEP 1/5] Running Ingestion & Seeding (dltHub + Historical Data)...")
    seed_all()

    print("\n[STEP 2/5] Running Stream Ingestion to Lake (Parquet Partitioning)...")
    from streaming.producer import main as run_producer
    from streaming.consumer_to_lake import consume_from_buffer
    run_producer(num_messages=30, delay_seconds=0.005)
    consume_from_buffer()

    print("\n[STEP 3/5] Running Distributed Batch Aggregator (Apache Spark / Columnar)...")
    execute_batch()

    print("\n[STEP 4/5] Running Analytics Engineering (dbt Models & Data Tests)...")
    run_dbt_transformations()

    print("\n[STEP 5/5] Running Real-Time Anomaly & Fraud Detection Engine...")
    run_fraud_detection()

    elapsed = round(time.time() - start_time, 2)
    print("\n=================================================================")
    print(f"PIPELINE COMPLETED SUCCESSFULLY IN {elapsed}s")
    print("To launch the interactive Executive Dashboard, run:")
    print("   python -m streamlit run data_platform/dashboard.py")
    print("=================================================================")


if __name__ == "__main__":
    run_all()
