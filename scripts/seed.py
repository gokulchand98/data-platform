import psycopg2
import random
from faker import Faker
from datetime import datetime, timedelta
import sys

fake = Faker()
Faker.seed(42)       # reproducible data — same seed = same fake data every run
random.seed(42)

# ── connection ────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="ecommerce",
        user="platform",
        password="platform"
    )

# ── constants ─────────────────────────────────────────────────
CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Sports"]
STATUSES   = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
CURRENCIES = ["USD", "EUR", "GBP", "BRL", "CAD"]

def seed_products(cur, n=50):
    print(f"  seeding {n} products...")
    product_ids = []
    for _ in range(n):
        cur.execute("""
            INSERT INTO products (name, category, price, stock_qty)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, (
            fake.catch_phrase(),
            random.choice(CATEGORIES),
            round(random.uniform(5.00, 999.99), 2),
            random.randint(0, 500)
        ))
        product_ids.append(cur.fetchone()[0])
    return product_ids


def seed_customers(cur, n=200):
    print(f"  seeding {n} customers...")
    customer_ids = []
    for _ in range(n):
        # fake.unique ensures no duplicate emails
        cur.execute("""
            INSERT INTO customers (email, full_name, country, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (
            fake.unique.email(),
            fake.name(),
            fake.country_code(),
            fake.date_time_between(start_date="-2y", end_date="-90d"),
            fake.date_time_between(start_date="-90d", end_date="now"),
        ))
        customer_ids.append(cur.fetchone()[0])
    return customer_ids


def seed_orders(cur, customer_ids, product_ids, n=1000):
    print(f"  seeding {n} orders with items...")
    for _ in range(n):
        ordered_at = fake.date_time_between(start_date="-90d", end_date="now")

        # orders created recently might still be pending
        # older orders are more likely delivered
        days_ago = (datetime.now() - ordered_at).days
        if days_ago < 3:
            status = random.choice(["pending", "confirmed"])
        elif days_ago < 10:
            status = random.choice(["confirmed", "shipped"])
        else:
            status = random.choice(["shipped", "delivered", "cancelled"])

        cur.execute("""
            INSERT INTO orders
                (customer_id, status, currency, ordered_at, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (
            random.choice(customer_ids),
            status,
            random.choice(CURRENCIES),
            ordered_at,
            ordered_at + timedelta(hours=random.randint(1, 48))
        ))
        order_id = cur.fetchone()[0]

        # 1-5 line items per order
        total = 0.00
        for _ in range(random.randint(1, 5)):
            product_id = random.choice(product_ids)
            quantity   = random.randint(1, 4)

            cur.execute("SELECT price FROM products WHERE id = %s", (product_id,))
            unit_price = float(cur.fetchone()[0])

            cur.execute("""
                INSERT INTO order_items
                    (order_id, product_id, quantity, unit_price)
                VALUES (%s, %s, %s, %s)
            """, (order_id, product_id, quantity, unit_price))

            total += quantity * unit_price

        # write final total back to the order
        cur.execute(
            "UPDATE orders SET total_amount = %s WHERE id = %s",
            (round(total, 2), order_id)
        )


def main():
    print("connecting to postgres...")
    conn = get_conn()
    cur  = conn.cursor()

    try:
        print("starting seed...")
        product_ids  = seed_products(cur)
        customer_ids = seed_customers(cur)
        seed_orders(cur, customer_ids, product_ids)

        conn.commit()
        print("\ndone.")
        print("  products  : 50")
        print("  customers : 200")
        print("  orders    : 1000")

        # quick sanity check — print row counts from DB
        print("\nverifying row counts:")
        for table in ["products", "customers", "orders", "order_items"]:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"  {table:<15} {count} rows")

    except Exception as e:
        conn.rollback()
        print(f"\nerror: {e}")
        sys.exit(1)

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()