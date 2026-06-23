-- =========================================
-- SILVER LAYER (CLEANED DATA)
-- =========================================

CREATE TABLE silver_customers AS
SELECT DISTINCT
    customer_id,
    TRIM(customer_name) AS customer_name,
    LOWER(TRIM(email)) AS email,
    signup_date
FROM bronze_customers
WHERE customer_id IS NOT NULL;

CREATE TABLE silver_orders AS
SELECT DISTINCT
    order_id,
    customer_id,
    order_amount,
    order_date
FROM bronze_orders
WHERE order_id IS NOT NULL
AND order_amount > 0;

CREATE TABLE silver_payments AS
SELECT DISTINCT
    payment_id,
    order_id,
    payment_amount,
    payment_status,
    payment_date
FROM bronze_payments
WHERE payment_amount > 0;

CREATE TABLE silver_products AS
SELECT DISTINCT
    product_id,
    product_name,
    category,
    price
FROM bronze_products
WHERE price > 0
AND product_name IS NOT NULL;
