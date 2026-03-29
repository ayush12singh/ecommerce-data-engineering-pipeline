import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, max as spark_max, lit
from pyspark.sql.utils import AnalysisException

# -------------------------------
# Functions
# -------------------------------
def get_last_ingestion_date(table, SILVER_PATH):
    """Get latest ingestion date from silver"""
    try:
        df = spark.read.parquet(f"{SILVER_PATH}{table}/")
        last_date = df.select(spark_max("ingestion_date")).collect()[0][0]
        return last_date
    except AnalysisException:
        return None

# -------------------------------
# Glue Job Setup
# -------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SILVER_PATH = "s3://de-ecommerce-datalake/silver/"

# -------------------------------
# PRODUCTS (non-partitioned)
# -------------------------------
raw_products = spark.sql("SELECT * FROM olist_raw_db.products")
last_date = get_last_ingestion_date("products", SILVER_PATH)
bronze_products = spark.sql(f"SELECT * FROM olist_bronze_db.daily_products WHERE ingestion_date > '{last_date}'") \
                    if last_date else spark.sql("SELECT * FROM olist_bronze_db.daily_products")

if not bronze_products.rdd.isEmpty():
    # First ingestion: union raw + bronze
    if last_date is None:
        raw_products = raw_products.withColumnRenamed("product_name_lenght","product_name_length") \
                                        .withColumnRenamed("product_description_lenght","product_description_length") \
                                        .withColumn("price", lit(None)) \
                                        .withColumn("stock", lit(None)) \
                                        .withColumn("rating", lit(None)) \
                                        .withColumn("ingestion_date", lit(None)) \
                                        .select(bronze_products.columns)
    
        products_silver = raw_products.unionByName(bronze_products)
        
    else:
        products_silver = bronze_products

    if products_silver is not None and not products_silver.rdd.isEmpty():
        products_silver = products_silver.dropDuplicates(["product_id"])
        products_silver.write.mode("append").parquet(SILVER_PATH + "products/")

# -------------------------------
# CUSTOMERS (non-partitioned)
# -------------------------------
raw_customers = spark.sql("SELECT * FROM olist_raw_db.customers")
last_date = get_last_ingestion_date("customers", SILVER_PATH)
bronze_customers = spark.sql(f"SELECT * FROM olist_bronze_db.daily_customers WHERE ingestion_date > '{last_date}'") \
                     if last_date else spark.sql("SELECT * FROM olist_bronze_db.daily_customers")

if not bronze_customers.rdd.isEmpty():

    # First ingestion: union raw + bronze
    if last_date is None:
        raw_customers = raw_customers.withColumn("email", lit(None)) \
                            .withColumn("first_name", lit(None)) \
                            .withColumn("last_name", lit(None)) \
                            .withColumn("phone", lit(None)) \
                            .withColumn("ingestion_date", lit(None)) \
                            .select(bronze_customers.columns)

        customers_silver = raw_customers.unionByName(bronze_customers)
    else:
        customers_silver = bronze_customers

    if customers_silver is not None and not customers_silver.rdd.isEmpty():
        customers_silver = customers_silver.dropDuplicates(["customer_id"])
        customers_silver.write.mode("append").parquet(SILVER_PATH + "customers/")

# -------------------------------
# ORDERS (partitioned by year)
# -------------------------------
raw_orders = spark.sql("SELECT * FROM olist_raw_db.orders")
last_date = get_last_ingestion_date("orders", SILVER_PATH)
bronze_orders = spark.sql(f"SELECT * FROM olist_bronze_db.daily_orders WHERE ingestion_date > '{last_date}'") \
                 if last_date else spark.sql("SELECT * FROM olist_bronze_db.daily_orders")

if not bronze_orders.rdd.isEmpty():
    # First ingestion: union raw + bronze
    if last_date is None:
        raw_orders = raw_orders.withColumn("ingestion_date", lit(None)).select(bronze_orders.columns)
        orders_silver = raw_orders.unionByName(bronze_orders)
    else:
        orders_silver = bronze_orders

    if orders_silver is not None and not orders_silver.rdd.isEmpty():
        orders_silver = orders_silver.withColumn("order_purchase_year", year(col("order_purchase_timestamp"))) \
                                     .withColumn("partition_year", col("order_purchase_year")) \
                                     
        orders_silver = orders_silver.filter(col("partition_year").isNotNull()) \
                                     .dropDuplicates(["order_id"])

        orders_silver.write.mode("append").partitionBy("partition_year").parquet(SILVER_PATH + "orders/")

# -------------------------------
# ORDER ITEMS (partitioned by year)
# -------------------------------
raw_order_items = spark.sql("SELECT * FROM olist_raw_db.order_items")
last_date = get_last_ingestion_date("order_items", SILVER_PATH)
bronze_order_items = spark.sql(f"SELECT * FROM olist_bronze_db.daily_order_items WHERE ingestion_date > '{last_date}'") \
                     if last_date else spark.sql("SELECT * FROM olist_bronze_db.daily_order_items")

if not bronze_order_items.rdd.isEmpty():
    if last_date is None:
        raw_order_items= raw_order_items.withColumn("ingestion_date", lit(None)).select(bronze_order_items.columns)
        order_items_silver = raw_order_items.unionByName(bronze_order_items)
    else:
        order_items_silver = bronze_order_items

    if orders_silver is not None:
        order_items_silver = order_items_silver.join(
            orders_silver.select("order_id","order_purchase_timestamp"), "order_id", "left"
        )

    order_items_silver = order_items_silver.withColumn("order_purchase_year", year(col("order_purchase_timestamp"))) \
                                           .withColumn("partition_year", col("order_purchase_year")) \
                                           
    order_items_silver = order_items_silver.filter(col("partition_year").isNotNull()) \
                                           .dropDuplicates(["order_item_id","order_id"])

    if not order_items_silver.rdd.isEmpty():
        order_items_silver.write.mode("append").partitionBy("partition_year").parquet(SILVER_PATH + "order_items/")

# -------------------------------
# PAYMENTS (partitioned by year)
# -------------------------------
raw_payments = spark.sql("SELECT * FROM olist_raw_db.payments")
last_date = get_last_ingestion_date("payments", SILVER_PATH)
bronze_payments = spark.sql(f"SELECT * FROM olist_bronze_db.daily_payments WHERE ingestion_date > '{last_date}'") \
                  if last_date else spark.sql("SELECT * FROM olist_bronze_db.daily_payments")

if not bronze_payments.rdd.isEmpty():
    if last_date is None:
        raw_payments = raw_payments.withColumn("ingestion_date", lit(None)).select(bronze_payments.columns)
        payments_silver = raw_payments.unionByName(bronze_payments)
    else:
        payments_silver = bronze_payments

    if orders_silver is not None:
        payments_silver = payments_silver.join(
            orders_silver.select("order_id","order_purchase_timestamp"), "order_id", "left"
        )

    payments_silver = payments_silver.withColumn("order_purchase_year", year(col("order_purchase_timestamp"))) \
                                     .withColumn("partition_year", col("order_purchase_year")) \
                                     
    payments_silver = payments_silver.filter(col("partition_year").isNotNull())

    if not payments_silver.rdd.isEmpty():
        payments_silver.write.mode("append").partitionBy("partition_year").parquet(SILVER_PATH + "payments/")

job.commit()