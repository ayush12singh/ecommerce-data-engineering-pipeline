/*Script to create tables in gold layer and populate them with transformed data from silver layer*/

USE SCHEMA gold;

#Fact Table
CREATE OR REPLACE TABLE gold.fact_orders AS
SELECT
    order_id,
    customer_id,
    product_id,
    order_purchase_timestamp,
    order_purchase_year,
    price,
    freight_value,
    payment_value,
    payment_type,
    payment_installments
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id, product_id
               ORDER BY order_purchase_timestamp DESC
           ) AS rn
    FROM gold.fact_orders
)
WHERE rn = 1;

#Dimension tables
CREATE OR REPLACE TABLE dim_customers AS
SELECT
customer_id,
customer_city,
customer_state,
first_name,
last_name,
email,
phone
FROM ecommerce_db.silver.customers;

CREATE OR REPLACE TABLE dim_products AS
SELECT
product_id,
product_category_name,
price,
rating,
stock
FROM ecommerce_db.silver.products;
