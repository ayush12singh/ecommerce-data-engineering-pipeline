/*Initial setup for etl project*/

CREATE WAREHOUSE de_wh
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE de_wh;

/*Create database and schemas for the project*/
CREATE DATABASE ecommerce_db;
USE DATABASE ecommerce_db;

CREATE SCHEMA external_stage;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

/*Create storage integration for S3 access*/
CREATE STORAGE INTEGRATION s3_snowflake_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::200488541900:role/snowflake-s3-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://de-ecommerce-datalake/silver/');


DESC INTEGRATION s3_snowflake_integration;

/*Create file format and stage for accessing data in S3*/
CREATE FILE FORMAT parquet_format TYPE = PARQUET;

/*Create  external stage to access silver data in S3*/
CREATE STAGE external_stage.s3_silver_stage
URL='s3://de-ecommerce-datalake/silver/'
STORAGE_INTEGRATION = s3_snowflake_integration
FILE_FORMAT = parquet_format;

LIST @external_stage.s3_silver_stage;