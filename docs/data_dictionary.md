# Data Dictionary

## customers.csv
| Column | Type | Description |
|---|---|---|
| customer_id | string | Unique customer ID |
| customer_name | string | Full customer name |
| email | string | Customer email |
| signup_date | date | Registration date |

---

## orders.csv
| Column | Type | Description |
|---|---|---|
| order_id | string | Unique order ID |
| customer_id | string | Customer ID |
| order_amount | decimal | Order value |
| order_date | date | Order date |

---

## payments.csv
| Column | Type | Description |
|---|---|---|
| payment_id | string | Payment ID |
| order_id | string | Order ID |
| payment_amount | decimal | Payment amount |
| payment_status | string | Status |

---

## products.csv
| Column | Type | Description |
|---|---|---|
| product_id | string | Product ID |
| product_name | string | Product name |
| category | string | Product category |
| price | decimal | Product price |
