"""
OmniFlow dltHub Data Ingestion Pipeline
Demonstrates modern schema-inferring, incremental ELT with dlt (data load tool).
Extracts customer profiles, product catalog, and exchange rates from API sources,
normalizes nested JSON data structures, and loads into Data Warehouse / Lake.
"""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

import dlt
from ingestion.mock_api import fetch_mock_customers, fetch_mock_products, fetch_exchange_rates


@dlt.source(name="ecommerce_api_source")
def ecommerce_source():
    """Defines dlt source with three extracted endpoints."""

    @dlt.resource(name="customers", write_disposition="merge", primary_key="customer_id")
    def customers():
        yield fetch_mock_customers(count=60)

    @dlt.resource(name="products", write_disposition="replace", primary_key="product_id")
    def products():
        yield fetch_mock_products()

    @dlt.resource(name="exchange_rates", write_disposition="append")
    def exchange_rates():
        yield fetch_exchange_rates()

    return customers(), products(), exchange_rates()


def run_pipeline(destination_name="duckdb"):
    """Executes the dlt pipeline with the selected destination."""
    print(f"[*] Initializing dlt pipeline (Destination: {destination_name})...")

    # Local DuckDB warehouse path
    warehouse_dir = PROJECT_ROOT / "data_warehouse"
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    db_path = str(warehouse_dir / "warehouse.duckdb")

    pipeline = dlt.pipeline(
        pipeline_name="ecommerce_dlt_ingestion",
        destination=dlt.destinations.duckdb(db_path) if destination_name == "duckdb" else destination_name,
        dataset_name="ecommerce_raw"
    )

    source = ecommerce_source()
    print("[*] Running dlt extraction and normalization...")
    load_info = pipeline.run(source)

    if hasattr(sys.stdout, 'reconfigure'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass

    print("[OK] dlt Ingestion finished successfully!")
    print(load_info)
    return load_info


if __name__ == "__main__":
    dest = sys.argv[1] if len(sys.argv) > 1 else "duckdb"
    run_pipeline(destination_name=dest)
