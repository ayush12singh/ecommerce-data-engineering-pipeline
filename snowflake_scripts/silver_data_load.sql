/*Script to load data from S3 to Snowflake silver layer daily using COPY INTO command*/

COPY INTO silver.customers
FROM @external_stage.s3_silver_stage/customers
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO silver.orders
FROM @external_stage.s3_silver_stage/orders
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO silver.order_items
FROM @external_stage.s3_silver_stage/order_items
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO silver.payments
FROM @external_stage.s3_silver_stage/payments
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO silver.products
FROM @external_stage.s3_silver_stage/products
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;