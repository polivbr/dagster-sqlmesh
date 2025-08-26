#!/usr/bin/env python3
"""
Generate test data for dg-sqlmesh tests.

This script generates CSV files with synthetic data for testing purposes.
The data is similar to the Jaffle Shop dataset but generated programmatically.
"""

import csv
import uuid
import random
from datetime import datetime, timedelta
from pathlib import Path


def generate_customers(num_customers=100):
    """Generate customer data.

    Expected columns: id, name
    """
    customers = []
    for i in range(num_customers):
        customer_id = str(uuid.uuid4())
        name = f"Customer {i + 1}"
        customers.append([customer_id, name])
    return customers


def generate_products():
    """Generate product data.

    Expected columns: sku, name, type, price, description
    """
    products = [
        ["JAF-001", "nutellaphone who dis?", "food", 1100, "nutella and banana jaffle"],
        ["JAF-002", "doctor stew", "food", 1100, "house-made beef stew jaffle"],
        [
            "JAF-003",
            "the krautback",
            "food",
            1200,
            "lamb and pork bratwurst with house-pickled cabbage sauerkraut and mustard",
        ],
        ["JAF-004", "adele-ade", "food", 1000, "classic ham and cheese jaffle"],
        ["JAF-005", "tangaroo", "food", 950, "tangy chicken and cheese jaffle"],
        ["BEV-001", "coffee", "drink", 300, "hot coffee"],
        ["BEV-002", "tea", "drink", 250, "hot tea"],
        ["BEV-003", "juice", "drink", 400, "fresh orange juice"],
        ["BEV-004", "water", "drink", 100, "bottled water"],
        ["BEV-005", "soda", "drink", 350, "soft drink"],
    ]
    return products


def generate_stores():
    """Generate store data.

    Expected columns: id, name, opened_at, tax_rate
    """
    stores = [
        [
            "10fa6637-5363-44dd-8202-1fca446cd778",
            "Philadelphia",
            "2018-09-01T00:00:00",
            0.06,
        ],
        [
            "220b1ab8-3567-4ccb-ba36-e210d9fe6884",
            "Brooklyn",
            "2019-03-12T00:00:00",
            0.04,
        ],
        [
            "0f8baf84-89b7-4bfd-88ec-47bc168fed09",
            "Chicago",
            "2020-04-28T00:00:00",
            0.0625,
        ],
        [
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "Los Angeles",
            "2021-01-15T00:00:00",
            0.085,
        ],
        ["b2c3d4e5-f6g7-8901-bcde-f23456789012", "Miami", "2021-06-20T00:00:00", 0.07],
        [
            "c3d4e5f6-g7h8-9012-cdef-345678901234",
            "Seattle",
            "2021-11-10T00:00:00",
            0.065,
        ],
    ]
    return stores


def generate_supplies():
    """Generate supplies data.

    Expected columns: id, sku, name, cost, perishable
    """
    supplies = [
        ["SUP-001", "JAF-001", "compostable cutlery - knife", 7, False],
        ["SUP-002", "JAF-001", "cutlery - fork", 7, False],
        ["SUP-003", "JAF-001", "serving boat", 11, False],
        ["SUP-004", "JAF-002", "napkins", 5, False],
        ["SUP-005", "JAF-002", "to-go container", 15, True],
        ["SUP-006", "JAF-003", "sauce packet", 3, False],
        ["SUP-007", "JAF-004", "paper bag", 8, True],
        ["SUP-008", "BEV-001", "straw", 2, False],
        ["SUP-009", "BEV-001", "cup lid", 4, False],
        ["SUP-010", "BEV-005", "bottle cap", 3, False],
        ["SUP-011", "JAF-005", "sauce packet", 4, False],
        ["SUP-012", "JAF-005", "napkins", 3, False],
        ["SUP-013", "BEV-002", "tea bag", 1, False],
        ["SUP-014", "BEV-003", "juice cup", 6, True],
        ["SUP-015", "BEV-004", "water bottle", 5, True],
    ]
    return supplies


def generate_orders(customers, stores, num_orders=1000):
    """Generate order data.

    Expected columns: id, customer, ordered_at, store_id, subtotal, tax_paid, order_total
    """
    orders = []
    start_date = datetime(2018, 9, 1)

    for i in range(num_orders):
        order_id = str(uuid.uuid4())
        customer_id = random.choice(customers)[0]
        store_id = random.choice(stores)[0]

        # Random date between 2018-2023
        days_offset = random.randint(0, 365 * 5)
        order_date = start_date + timedelta(days=days_offset)

        # Random order total between $5 and $50
        subtotal = random.randint(500, 5000)
        tax_paid = int(subtotal * 0.06)  # 6% tax
        order_total = subtotal + tax_paid

        orders.append(
            [
                order_id,
                customer_id,
                order_date.isoformat(),
                store_id,
                subtotal,
                tax_paid,
                order_total,
            ]
        )

    return orders


def generate_order_items(orders, products, num_items=2000):
    """Generate order items data.

    Expected columns: id, order_id, sku
    """
    items = []

    for i in range(num_items):
        item_id = str(uuid.uuid4())
        order_id = random.choice(orders)[0]
        product_sku = random.choice(products)[0]

        items.append([item_id, order_id, product_sku])

    return items


def generate_tweets(customers, num_tweets=500):
    """Generate tweet data.

    Expected columns: id, user_id, tweeted_at, content
    """
    tweets = []
    start_date = datetime(2018, 9, 1)

    tweet_templates = [
        "Jaffles from the Jaffle Shop are my favorite! Ordered a {product}.",
        "Jaffles from the Jaffle Shop are sooo gooood! Ordered a {product}.",
        "Just had the best {product} at Jaffle Shop! Highly recommend!",
        "Can't get enough of the {product} from Jaffle Shop!",
        "Jaffle Shop never disappoints! The {product} was amazing!",
    ]

    products = [
        "adele-ade",
        "tangaroo",
        "mel-bun",
        "flame impala",
        "doctor stew",
        "krautback",
    ]

    for i in range(num_tweets):
        tweet_id = str(uuid.uuid4())
        user_id = random.choice(customers)[0]
        product = random.choice(products)

        # Random date between 2018-2023
        days_offset = random.randint(0, 365 * 5)
        tweet_date = start_date + timedelta(days=days_offset)

        template = random.choice(tweet_templates)
        content = template.format(product=product)

        tweets.append([tweet_id, user_id, tweet_date.isoformat(), content])

    return tweets


def write_csv(data, filename, headers):
    """Write data to CSV file."""
    fixtures_dir = Path("tests/fixtures")
    fixtures_dir.mkdir(exist_ok=True)

    filepath = fixtures_dir / filename
    with open(filepath, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(data)

    print(f"✅ Generated {len(data)} rows in {filepath}")


def main():
    """Generate all test data files."""
    print("🚀 Generating test data...")

    # Generate base data
    customers = generate_customers(100)
    products = generate_products()
    stores = generate_stores()
    supplies = generate_supplies()

    # Generate dependent data
    orders = generate_orders(customers, stores, 1000)
    items = generate_order_items(orders, products, 2000)
    tweets = generate_tweets(customers, 500)

    # Write CSV files with correct column names
    write_csv(customers, "raw_source_customers.csv", ["id", "name"])
    write_csv(
        products,
        "raw_source_products.csv",
        ["sku", "name", "type", "price", "description"],
    )
    write_csv(stores, "raw_source_stores.csv", ["id", "name", "opened_at", "tax_rate"])
    write_csv(
        supplies, "raw_source_supplies.csv", ["id", "sku", "name", "cost", "perishable"]
    )
    write_csv(
        orders,
        "raw_source_orders.csv",
        [
            "id",
            "customer",
            "ordered_at",
            "store_id",
            "subtotal",
            "tax_paid",
            "order_total",
        ],
    )
    write_csv(items, "raw_source_items.csv", ["id", "order_id", "sku"])
    write_csv(
        tweets, "raw_source_tweets.csv", ["id", "user_id", "tweeted_at", "content"]
    )

    print("✅ All test data generated successfully!")


if __name__ == "__main__":
    main()
