"""
OmniFlow Streaming Producer
Simulates live E-Commerce events (Orders, Clickstream, Fraud anomalies)
Publishes to Apache Kafka or buffers locally to the Data Lake landing zone.
"""

import os
import sys
import json
import time
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

# pyright: reportMissingImports=false
# Optional kafka client
try:
    from kafka import KafkaProducer  # type: ignore
    KAFKA_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    try:
        from kafka.producer import KafkaProducer  # type: ignore
        KAFKA_AVAILABLE = True
    except (ImportError, ModuleNotFoundError):
        KAFKA_AVAILABLE = False

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_ORDERS = os.getenv("KAFKA_TOPIC_ORDERS", "ecommerce.orders")
TOPIC_EVENTS = os.getenv("KAFKA_TOPIC_EVENTS", "ecommerce.events")

# Sample data generators
CATEGORIES = ["Electronics", "Fashion", "Home & Kitchen", "Books", "Sports", "Beauty", "Automotive"]
PRODUCTS = [
    {"id": "PROD-101", "name": "Apple MacBook Pro M3", "category": "Electronics", "price": 1999.00},
    {"id": "PROD-102", "name": "Sony WH-1000XM5 Headphones", "category": "Electronics", "price": 399.00},
    {"id": "PROD-103", "name": "Nike Air Max 270", "category": "Fashion", "price": 160.00},
    {"id": "PROD-104", "name": "Dyson V15 Cordless Vacuum", "category": "Home & Kitchen", "price": 749.00},
    {"id": "PROD-105", "name": "Kindle Paperwhite 16GB", "category": "Books", "price": 149.99},
    {"id": "PROD-106", "name": "Under Armour Gym Duffle", "category": "Sports", "price": 45.00},
    {"id": "PROD-107", "name": "La Roche-Posay Anthelios SPF", "category": "Beauty", "price": 32.50},
    {"id": "PROD-108", "name": "Samsung 65' 4K OLED TV", "category": "Electronics", "price": 1499.00},
    {"id": "PROD-109", "name": "Levi's 501 Original Jeans", "category": "Fashion", "price": 89.00},
    {"id": "PROD-110", "name": "Nespresso Vertuo Pop Machine", "category": "Home & Kitchen", "price": 129.00},
]
CITIES_COUNTRIES = [
    ("Casablanca", "MA"), ("Rabat", "MA"), ("Marrakech", "MA"), ("Tangier", "MA"),
    ("Paris", "FR"), ("Lyon", "FR"), ("London", "UK"), ("New York", "US"),
    ("Berlin", "DE"), ("Dubai", "AE"), ("Madrid", "ES"), ("Montreal", "CA")
]
PAYMENT_METHODS = ["credit_card", "paypal", "apple_pay", "cash_on_delivery", "crypto"]
DEVICE_TYPES = ["mobile_ios", "mobile_android", "desktop_chrome", "desktop_safari"]


def generate_order_event():
    """Generates a realistic order transaction event, with periodic fraud signals."""
    customer_id = f"CUST-{random.randint(1001, 1200)}"
    city, country = random.choice(CITIES_COUNTRIES)
    num_items = random.choices([1, 2, 3, 4, 8], weights=[50, 30, 12, 6, 2])[0]
    
    selected_products = random.sample(PRODUCTS, k=min(num_items, len(PRODUCTS)))
    items = []
    subtotal = 0.0
    for p in selected_products:
        qty = random.choices([1, 2, 3], weights=[80, 15, 5])[0]
        line_total = round(p["price"] * qty, 2)
        subtotal += line_total
        items.append({
            "product_id": p["id"],
            "product_name": p["name"],
            "category": p["category"],
            "unit_price": p["price"],
            "quantity": qty,
            "line_total": line_total
        })

    discount = round(random.choice([0.0, 5.0, 10.0, 20.0, 50.0]), 2)
    tax = round(subtotal * 0.10, 2)
    shipping = 0.0 if subtotal > 100 else 9.99
    total_amount = round(max(5.0, subtotal - discount + tax + shipping), 2)

    # Fraud injection (~4% probability)
    is_fraud_suspect = random.random() < 0.04
    if is_fraud_suspect:
        total_amount = round(random.uniform(2500.0, 7500.0), 2)
        payment_method = "crypto" if random.random() > 0.5 else "credit_card"
        fraud_reason = random.choice(["ABNORMAL_HIGH_AMOUNT", "VELOCITY_BURST", "RISKY_GEOLOCATION"])
    else:
        payment_method = random.choice(PAYMENT_METHODS)
        fraud_reason = None

    now = datetime.now(timezone.utc)
    return {
        "order_id": f"ORD-{uuid.uuid4().hex[:10].upper()}",
        "customer_id": customer_id,
        "items": items,
        "item_count": len(items),
        "primary_category": selected_products[0]["category"] if selected_products else "General",
        "subtotal": round(subtotal, 2),
        "discount": discount,
        "tax": tax,
        "shipping": shipping,
        "total_amount": total_amount,
        "payment_method": payment_method,
        "currency": "USD",
        "city": city,
        "country": country,
        "device": random.choice(DEVICE_TYPES),
        "ip_address": f"{random.randint(40, 195)}.{random.randint(10, 240)}.{random.randint(1, 250)}.{random.randint(1, 250)}",
        "is_fraud_suspect": is_fraud_suspect,
        "fraud_reason": fraud_reason,
        "order_status": "FLAGGED" if is_fraud_suspect else random.choice(["COMPLETED", "COMPLETED", "COMPLETED", "PROCESSING"]),
        "timestamp": now.isoformat(),
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S")
    }


def generate_clickstream_event():
    """Generates clickstream events (product view, add to cart, checkout)."""
    p = random.choice(PRODUCTS)
    city, country = random.choice(CITIES_COUNTRIES)
    event_type = random.choices(["product_view", "add_to_cart", "search", "remove_from_cart"], weights=[65, 20, 12, 3])[0]
    now = datetime.now(timezone.utc)

    return {
        "event_id": f"EVT-{uuid.uuid4().hex[:12].upper()}",
        "session_id": f"SES-{uuid.uuid4().hex[:8]}",
        "customer_id": f"CUST-{random.randint(1001, 1200)}",
        "event_type": event_type,
        "product_id": p["id"],
        "category": p["category"],
        "page_url": f"/shop/{p['category'].lower().replace(' ', '-')}/{p['id']}",
        "duration_seconds": random.randint(3, 180),
        "device": random.choice(DEVICE_TYPES),
        "country": country,
        "timestamp": now.isoformat()
    }


def main(num_messages=50, delay_seconds=0.2):
    print(f"[*] Starting OmniFlow Streaming Producer...")
    print(f"[*] Kafka broker configured: {KAFKA_BROKER}")

    producer = None
    if KAFKA_AVAILABLE:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=3000
            )
            print("[+] Successfully connected to Kafka Broker!")
        except Exception as e:
            print(f"[!] Warning: Could not connect to Kafka broker ({e}). Falling back to local Lake buffer.")
            producer = None
    else:
        print("[!] kafka-python not installed or offline. Using local buffer mode.")

    local_lake_dir = PROJECT_ROOT / "data_lake" / "raw" / "stream_buffer"
    local_lake_dir.mkdir(parents=True, exist_ok=True)
    buffer_file = local_lake_dir / f"stream_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"

    print(f"[*] Generating {num_messages} streaming events...")
    orders_sent = 0
    clicks_sent = 0

    with open(buffer_file, "a", encoding="utf-8") as f_buff:
        for i in range(num_messages):
            # Generate 1 order
            order = generate_order_event()
            # Generate 2 clickstream events
            click1 = generate_clickstream_event()
            click2 = generate_clickstream_event()

            if producer:
                try:
                    producer.send(TOPIC_ORDERS, value=order)
                    producer.send(TOPIC_EVENTS, value=click1)
                    producer.send(TOPIC_EVENTS, value=click2)
                except Exception as err:
                    print(f"[!] Failed to push to Kafka: {err}")

            # Always write to local buffer for offline fallback & data lake replication
            f_buff.write(json.dumps({"type": "order", "payload": order}) + "\n")
            f_buff.write(json.dumps({"type": "event", "payload": click1}) + "\n")
            f_buff.write(json.dumps({"type": "event", "payload": click2}) + "\n")

            orders_sent += 1
            clicks_sent += 2

            if (i + 1) % 10 == 0:
                print(f" -> Emitted {orders_sent} orders, {clicks_sent} click events...")
            time.sleep(delay_seconds)

    if hasattr(sys.stdout, 'reconfigure'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass
    print(f"[OK] Producer run complete! {orders_sent} orders, {clicks_sent} events saved to {buffer_file.name}")


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    delay = float(sys.argv[2]) if len(sys.argv) > 2 else 0.05
    main(num_messages=count, delay_seconds=delay)
