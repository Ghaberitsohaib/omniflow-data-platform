"""
OmniFlow Streaming Consumer to Data Lake
Consumes streaming events from Kafka (or stream buffer),
micro-batches records, and writes partitioned Parquet files into the Data Lake.
Also updates the raw staging table in DuckDB/PostgreSQL.
"""

import os
import sys
import json
import glob
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# pyright: reportMissingImports=false
# Optional Kafka Consumer
try:
    from kafka import KafkaConsumer  # type: ignore
    KAFKA_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    KAFKA_AVAILABLE = False

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_ORDERS = os.getenv("KAFKA_TOPIC_ORDERS", "ecommerce.orders")
DATA_LAKE_PATH = PROJECT_ROOT / "data_lake"
WAREHOUSE_PATH = PROJECT_ROOT / "data_warehouse" / "warehouse.duckdb"


def process_orders_microbatch(orders_list):
    """Writes micro-batch of orders into Data Lake (Parquet) and updates Warehouse."""
    if not orders_list:
        return 0

    now = datetime.now(timezone.utc)
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    target_dir = DATA_LAKE_PATH / "raw" / "orders" / f"year={year}" / f"month={month}" / f"day={day}"
    target_dir.mkdir(parents=True, exist_ok=True)
    parquet_filename = target_dir / f"batch_{int(time.time())}_{len(orders_list)}.parquet"

    # Flatten nested items for tabular parquet representation
    flat_rows = []
    for o in orders_list:
        items = o.get("items") or []
        first_cat = items[0].get("category", "General") if items and isinstance(items, list) and len(items) > 0 and isinstance(items[0], dict) else "General"
        primary_cat = str(o.get("primary_category") or first_cat)

        flat_rows.append({
            "order_id": str(o.get("order_id")),
            "customer_id": str(o.get("customer_id")),
            "item_count": int(o.get("item_count", 1)),
            "primary_category": primary_cat,
            "subtotal": float(o.get("subtotal", 0.0)),
            "discount": float(o.get("discount", 0.0)),
            "tax": float(o.get("tax", 0.0)),
            "shipping": float(o.get("shipping", 0.0)),
            "total_amount": float(o.get("total_amount", 0.0)),
            "payment_method": str(o.get("payment_method", "credit_card")),
            "currency": str(o.get("currency", "USD")),
            "city": str(o.get("city", "")),
            "country": str(o.get("country", "")),
            "device": str(o.get("device", "")),
            "ip_address": str(o.get("ip_address", "")),
            "is_fraud_suspect": bool(o.get("is_fraud_suspect", False)),
            "fraud_reason": str(o.get("fraud_reason") or ""),
            "order_status": str(o.get("order_status", "COMPLETED")),
            "created_at": str(o.get("created_at", now.strftime("%Y-%m-%d %H:%M:%S")))
        })

    # Write Parquet with pyarrow
    table = pa.Table.from_pylist(flat_rows)
    pq.write_table(table, str(parquet_filename), compression="snappy")
    print(f"[+] Saved {len(flat_rows)} orders to Data Lake: {parquet_filename.name}")

    # Mirror to DuckDB Warehouse raw landing table
    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(WAREHOUSE_PATH))
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_orders (
            order_id VARCHAR PRIMARY KEY,
            customer_id VARCHAR,
            item_count INTEGER,
            primary_category VARCHAR,
            subtotal DOUBLE,
            discount DOUBLE,
            tax DOUBLE,
            shipping DOUBLE,
            total_amount DOUBLE,
            payment_method VARCHAR,
            currency VARCHAR,
            city VARCHAR,
            country VARCHAR,
            device VARCHAR,
            ip_address VARCHAR,
            is_fraud_suspect BOOLEAN,
            fraud_reason VARCHAR,
            order_status VARCHAR,
            created_at TIMESTAMP
        )
    """)

    # Ensure schema migration if table was created previously without primary_category
    try:
        con.execute("ALTER TABLE raw_orders ADD COLUMN IF NOT EXISTS primary_category VARCHAR DEFAULT 'General'")
    except Exception:
        pass

    # Upsert/Insert records
    for row in flat_rows:
        con.execute("""
            INSERT OR REPLACE INTO raw_orders (
                order_id, customer_id, item_count, primary_category,
                subtotal, discount, tax, shipping, total_amount,
                payment_method, currency, city, country,
                device, ip_address, is_fraud_suspect, fraud_reason,
                order_status, created_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, [
            row["order_id"], row["customer_id"], row["item_count"], row["primary_category"],
            row["subtotal"], row["discount"], row["tax"], row["shipping"], row["total_amount"],
            row["payment_method"], row["currency"], row["city"], row["country"],
            row["device"], row["ip_address"], row["is_fraud_suspect"], row["fraud_reason"],
            row["order_status"], row["created_at"]
        ])
    con.close()
    return len(flat_rows)


def consume_from_buffer(max_files=10):
    """Processes stream buffer files when running offline or testing."""
    buffer_dir = DATA_LAKE_PATH / "raw" / "stream_buffer"
    if not buffer_dir.exists():
        print("[-] No stream buffer directory found.")
        return 0

    files = sorted(glob.glob(str(buffer_dir / "*.jsonl")))
    if not files:
        print("[-] Stream buffer is empty. Run producer first.")
        return 0

    orders = []
    processed_files = []
    for filepath in files[:max_files]:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    if record.get("type") == "order":
                        orders.append(record["payload"])
                except Exception:
                    pass
        processed_files.append(filepath)

    if orders:
        count = process_orders_microbatch(orders)
        # Clean up processed buffer files
        for fp in processed_files:
            try:
                os.remove(fp)
            except OSError:
                pass
        print(f"[OK] Successfully ingested {count} orders from {len(processed_files)} buffer files into Data Lake.")
        return count
    return 0


def consume_from_kafka():
    """Runs continuous Kafka consumer."""
    print(f"[*] Connecting to Kafka broker {KAFKA_BROKER} on topic {TOPIC_ORDERS}...")
    try:
        consumer = KafkaConsumer(
            TOPIC_ORDERS,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='lake-ingestion-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000
        )
        print("[+] Kafka Consumer connected successfully.")
    except Exception as e:
        print(f"[!] Kafka connection failed: {e}. Switching to buffer reader.")
        return consume_from_buffer()

    batch = []
    total_consumed = 0
    print("[*] Listening for Kafka messages...")
    try:
        for message in consumer:
            batch.append(message.value)
            if len(batch) >= 20:
                process_orders_microbatch(batch)
                total_consumed += len(batch)
                batch = []
    except KeyboardInterrupt:
        pass
    finally:
        if batch:
            process_orders_microbatch(batch)
            total_consumed += len(batch)
        consumer.close()

    print(f"[OK] Ingested {total_consumed} messages from Kafka to Data Lake.")
    return total_consumed


if __name__ == "__main__":
    # If Kafka broker reachable, use it; else read buffer
    print("[*] OmniFlow Stream Ingestion to Data Lake starting...")
    res = consume_from_buffer()
    if res == 0 and KAFKA_AVAILABLE:
        consume_from_kafka()
