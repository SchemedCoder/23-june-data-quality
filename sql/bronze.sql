-- =========================================
-- BRONZE LAYER (RAW TABLES)
-- =========================================

CREATE TABLE bronze_customers (
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    email VARCHAR(100),
    signup_date DATE
);

CREATE TABLE bronze_orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_amount DECIMAL(12,2),
    order_date DATE
);

CREATE TABLE bronze_payments (
    payment_id VARCHAR(50),
    order_id VARCHAR(50),
    payment_amount DECIMAL(12,2),
    payment_status VARCHAR(50),
    payment_date DATE
);

CREATE TABLE bronze_products (
    product_id VARCHAR(50),
    product_name VARCHAR(100),
    category VARCHAR(100),
    price DECIMAL(12,2)
);
