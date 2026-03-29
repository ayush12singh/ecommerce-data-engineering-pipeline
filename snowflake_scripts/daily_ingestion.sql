/*Script to create Snowpipe for daily ingestion of data from S3 to Snowflake*/

CREATE PIPE silver.orders_pipe
AUTO_INGEST = TRUE
AS
COPY INTO silver.orders
FROM @external_stage.s3_silver_stage/orders/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


CREATE PIPE silver.order_items_pipe
AUTO_INGEST = TRUE
AS
COPY INTO silver.order_items
From @external_stage.s3_silver_stage/order_items/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


CREATE PIPE silver.payments_pipe
AUTO_INGEST = TRUE
AS
COPY INTO silver.payments
FROM @external_stage.s3_silver_stage/payments/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


-- Create a role to contain the Snowpipe privileges
USE ROLE SECURITYADMIN;

CREATE OR REPLACE ROLE snowpipe_role;

-- Grant the required privileges on the database objects
GRANT USAGE ON DATABASE ecommerce_db TO ROLE snowpipe_role;

GRANT USAGE ON SCHEMA ecommerce_db.silver TO ROLE snowpipe_role;

GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA ecommerce_db.silver TO ROLE snowpipe_role;

GRANT USAGE ON SCHEMA ecommerce_db.external_stage TO ROLE snowpipe_role;
GRANT READ ON STAGE ecommerce_db.external_stage.s3_silver_stage TO ROLE snowpipe_role;
GRANT USAGE ON FILE FORMAT ecommerce_db.silver.parquet_format TO ROLE snowpipe_role;

-- Pause the pipe for OWNERSHIP transfer
ALTER PIPE  ecommerce_db.silver.orders_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE  ecommerce_db.silver.order_items_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE  ecommerce_db.silver.payments_pipe SET PIPE_EXECUTION_PAUSED = TRUE;


-- Grant the OWNERSHIP privilege on the pipe object
GRANT OWNERSHIP ON PIPE  ecommerce_db.silver.orders_pipe TO ROLE snowpipe_role;
GRANT OWNERSHIP ON PIPE  ecommerce_db.silver.order_items_pipe TO ROLE snowpipe_role;
GRANT OWNERSHIP ON PIPE  ecommerce_db.silver.payments_pipe TO ROLE snowpipe_role;



-- Grant the role to a user
GRANT ROLE snowpipe_role TO USER ayush;

-- Set the role as the default role for the user
ALTER USER ayush SET DEFAULT_ROLE = snowpipe_role;

-- Force resume the pipes after ownership transfer
SELECT SYSTEM$PIPE_FORCE_RESUME('ecommerce_db.silver.orders_pipe', 'OWNERSHIP_TRANSFER_CHECK_OVERRIDE');
SELECT SYSTEM$PIPE_FORCE_RESUME('ecommerce_db.silver.order_items_pipe', 'OWNERSHIP_TRANSFER_CHECK_OVERRIDE');
SELECT SYSTEM$PIPE_FORCE_RESUME('ecommerce_db.silver.payments_pipe', 'OWNERSHIP_TRANSFER_CHECK_OVERRIDE');

