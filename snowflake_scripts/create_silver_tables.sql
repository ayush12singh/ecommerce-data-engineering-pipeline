CREATE TABLE silver.customers (
    customer_id STRING,
    customer_unique_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    customer_zip_code_prefix INT,
    customer_city STRING,
    customer_state STRING,
    ingestion_date DATE
);

CREATE TABLE silver.orders (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    ingestion_date DATE,
    order_purchase_year STRING
);

CREATE TABLE silver.order_items(
  order_id STRING, 
  order_item_id INT, 
  product_id STRING, 
  seller_id STRING, 
  shipping_limit_date TIMESTAMP, 
  price DOUBLE, 
  freight_value DOUBLE,
  ingestion_date DATE,
  order_purchase_timestamp TIMESTAMP,
  order_purchase_year STRING
  );

  CREATE  TABLE silver.products(
  product_id STRING, 
  product_category_name STRING, 
  product_name_length INT, 
  product_description_length INT, 
  product_photos_qty INT, 
  product_weight_g INT, 
  product_length_cm DOUBLE, 
  product_height_cm DOUBLE, 
  product_width_cm DOUBLE, 
  price DOUBLE, 
  stock INT, 
  rating DOUBLE,
  ingestion_date DATE
  );

  CREATE TABLE silver.payments(
  order_id STRING, 
  payment_sequential INT, 
  payment_type STRING, 
  payment_installments INT, 
  payment_value DOUBLE,
  ingestion_date DATE,
  order_purchase_timestamp TIMESTAMP,
  order_purchase_year STRING
  );
