/*Script to create streams and tasks for incremental data loading from silver to gold layer in Snowflake*/

#create streams in silver layer
USE ecommerce_db
USE SCHEMA silver

CREATE OR REPLACE STREAM orders_stream
ON TABLE silver.orders;

CREATE OR REPLACE STREAM order_items_stream
ON TABLE silver.order_items;

CREATE OR REPLACE STREAM payments_stream
ON TABLE silver.payments;

#creating task for gold to load new data through streams

CREATE OR REPLACE TASK gold_fact_orders_task
WAREHOUSE = de_wh
WHEN
    SYSTEM$STREAM_HAS_DATA('silver.orders_stream')
    OR SYSTEM$STREAM_HAS_DATA('silver.order_items_stream')
    OR SYSTEM$STREAM_HAS_DATA('silver.payments_stream')
AS

MERGE INTO gold.fact_orders t
USING (
    SELECT *
    FROM (
        SELECT
            o.order_id,
            o.customer_id,
            oi.product_id,
            o.order_purchase_timestamp,
            o.order_purchase_year,
            oi.price,
            oi.freight_value,

            SUM(p.payment_value) OVER (PARTITION BY o.order_id) AS payment_value,
            MAX(p.payment_type) OVER (PARTITION BY o.order_id) AS payment_type,
            MAX(p.payment_installments) OVER (PARTITION BY o.order_id) AS payment_installments,

            ROW_NUMBER() OVER (
                PARTITION BY o.order_id, oi.product_id
                ORDER BY o.order_purchase_timestamp DESC
            ) AS rn

        FROM ecommerce_db.silver.orders o
        JOIN ecommerce_db.silver.order_items oi
            ON o.order_id = oi.order_id
        JOIN ecommerce_db.silver.payments p
            ON o.order_id = p.order_id
    )
    WHERE rn = 1
) s

ON t.order_id = s.order_id
AND t.product_id = s.product_id

WHEN MATCHED THEN UPDATE SET
    price = s.price,
    freight_value = s.freight_value,
    payment_value = s.payment_value,
    payment_type = s.payment_type,
    payment_installments = s.payment_installments

WHEN NOT MATCHED THEN INSERT (
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
)
VALUES (
    s.order_id,
    s.customer_id,
    s.product_id,
    s.order_purchase_timestamp,
    s.order_purchase_year,
    s.price,
    s.freight_value,
    s.payment_value,
    s.payment_type,
    s.payment_installments
);

ALTER TASK gold.gold_fact_orders_task RESUME;