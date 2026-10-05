"""
Mock REST API Generator for dltHub Ingestion
Provides customer profiles, product catalog metadata, and currency rates.
Can be queried as Python functions or served via FastAPI/Flask.
"""

import random
from datetime import datetime, timezone

COUNTRIES = ["Morocco", "France", "United States", "Germany", "United Kingdom", "United Arab Emirates", "Spain", "Canada"]
TIERS = ["Bronze", "Silver", "Gold", "Platinum", "VIP"]
CATEGORIES = ["Electronics", "Fashion", "Home & Kitchen", "Books", "Sports", "Beauty", "Automotive"]

FIRST_NAMES = ["Omar", "Youssef", "Fatima", "Amina", "Salma", "Mehdi", "Karim", "Zineb", "Hamza", "Sarah", "Alex", "Chloe", "Lucas", "Elena", "Liam"]
LAST_NAMES = ["Alaoui", "Bennani", "Idrissi", "El Fassi", "Mansouri", "Tazi", "Dupont", "Martin", "Smith", "Johnson", "Muller", "Garcia"]


def fetch_mock_customers(count=50):
    """Returns a list of rich customer profiles with nested JSON structures."""
    customers = []
    for i in range(1, count + 1):
        cust_id = f"CUST-{1000 + i}"
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        country = random.choice(COUNTRIES)
        customers.append({
            "customer_id": cust_id,
            "first_name": fn,
            "last_name": ln,
            "email": f"{fn.lower()}.{ln.lower()}{random.randint(1, 99)}@example.com",
            "country": country,
            "tier": random.choice(TIERS),
            "loyalty_points": random.randint(50, 4500),
            "preferences": {
                "preferred_category": random.choice(CATEGORIES),
                "newsletter_subscribed": random.random() > 0.3,
                "preferred_currency": "MAD" if country == "Morocco" else ("EUR" if country in ["France", "Germany", "Spain"] else "USD")
            },
            "registered_at": f"2025-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}T10:00:00Z"
        })
    return customers


def fetch_mock_products():
    """Returns product catalog with inventory, tags, and margins."""
    products = [
        {"product_id": "PROD-101", "name": "Apple MacBook Pro M3", "category": "Electronics", "cost_price": 1400.0, "retail_price": 1999.0, "stock_quantity": 45, "supplier": "TechWholesale Corp"},
        {"product_id": "PROD-102", "name": "Sony WH-1000XM5 Headphones", "category": "Electronics", "cost_price": 240.0, "retail_price": 399.0, "stock_quantity": 80, "supplier": "AudioElite"},
        {"product_id": "PROD-103", "name": "Nike Air Max 270", "category": "Fashion", "cost_price": 85.0, "retail_price": 160.0, "stock_quantity": 150, "supplier": "Athletix Inc"},
        {"product_id": "PROD-104", "name": "Dyson V15 Cordless Vacuum", "category": "Home & Kitchen", "cost_price": 480.0, "retail_price": 749.0, "stock_quantity": 30, "supplier": "HomeAppliances Ltd"},
        {"product_id": "PROD-105", "name": "Kindle Paperwhite 16GB", "category": "Books", "cost_price": 95.0, "retail_price": 149.99, "stock_quantity": 200, "supplier": "TechWholesale Corp"},
        {"product_id": "PROD-106", "name": "Under Armour Gym Duffle", "category": "Sports", "cost_price": 22.0, "retail_price": 45.0, "stock_quantity": 110, "supplier": "Athletix Inc"},
        {"product_id": "PROD-107", "name": "La Roche-Posay Anthelios SPF", "category": "Beauty", "cost_price": 16.0, "retail_price": 32.50, "stock_quantity": 320, "supplier": "DermaCare Direct"},
        {"product_id": "PROD-108", "name": "Samsung 65' 4K OLED TV", "category": "Electronics", "cost_price": 980.0, "retail_price": 1499.0, "stock_quantity": 25, "supplier": "TechWholesale Corp"},
        {"product_id": "PROD-109", "name": "Levi's 501 Original Jeans", "category": "Fashion", "cost_price": 42.0, "retail_price": 89.0, "stock_quantity": 190, "supplier": "Athletix Inc"},
        {"product_id": "PROD-110", "name": "Nespresso Vertuo Pop Machine", "category": "Home & Kitchen", "cost_price": 75.0, "retail_price": 129.0, "stock_quantity": 95, "supplier": "HomeAppliances Ltd"},
    ]
    return products


def fetch_exchange_rates():
    """Returns currency exchange rates relative to USD."""
    return [
        {"base_currency": "USD", "target_currency": "EUR", "rate": 0.92, "updated_at": datetime.now(timezone.utc).isoformat()},
        {"base_currency": "USD", "target_currency": "MAD", "rate": 10.05, "updated_at": datetime.now(timezone.utc).isoformat()},
        {"base_currency": "USD", "target_currency": "GBP", "rate": 0.79, "updated_at": datetime.now(timezone.utc).isoformat()},
        {"base_currency": "USD", "target_currency": "CAD", "rate": 1.36, "updated_at": datetime.now(timezone.utc).isoformat()},
    ]
