-- ============================================================
-- extensions
-- ============================================================
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
-- separate databases for airflow and metabase metadata
-- ============================================================
CREATE DATABASE airflow;
CREATE DATABASE metabase;

-- ============================================================
-- customers
-- ============================================================
CREATE TABLE customers (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email       TEXT NOT NULL UNIQUE,
    full_name   TEXT NOT NULL,
    country     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_customers_updated ON customers(updated_at);

-- ============================================================
-- products
-- ============================================================
CREATE TABLE products (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    category    TEXT,
    price       NUMERIC(10,2) NOT NULL,
    stock_qty   INT DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_products_updated ON products(updated_at);

-- ============================================================
-- orders
-- ============================================================
CREATE TABLE orders (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id   UUID NOT NULL REFERENCES customers(id),
    status        TEXT NOT NULL DEFAULT 'pending',
    currency      TEXT NOT NULL DEFAULT 'USD',
    total_amount  NUMERIC(12,2),
    ordered_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_updated  ON orders(updated_at);

-- ============================================================
-- order_items
-- ============================================================
CREATE TABLE order_items (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID NOT NULL REFERENCES orders(id),
    product_id  UUID NOT NULL REFERENCES products(id),
    quantity    INT NOT NULL,
    unit_price  NUMERIC(10,2) NOT NULL
);

CREATE INDEX idx_items_order   ON order_items(order_id);
CREATE INDEX idx_items_product ON order_items(product_id);

-- ============================================================
-- auto-update updated_at on every row change
-- ============================================================
CREATE OR REPLACE FUNCTION trigger_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_updated_at_customers
  BEFORE UPDATE ON customers
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TRIGGER set_updated_at_products
  BEFORE UPDATE ON products
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

CREATE TRIGGER set_updated_at_orders
  BEFORE UPDATE ON orders
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();