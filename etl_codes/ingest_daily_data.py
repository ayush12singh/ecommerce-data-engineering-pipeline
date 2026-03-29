from datetime import datetime
from datetime import timedelta
from faker import Faker
import pandas as pd
import requests
import sys,os
import hashlib
import random

def generate_orders(customers, products, generate_uuid):
    orders,order_items,payments = [],[],[]
    fake = Faker()
    for _ in range(random.randint(50, 120)):
        customer = customers.sample(1).iloc[0]
        order_id = generate_uuid()

        purchase_time = fake.date_time_between(start_date="-2d", end_date="now")

        approved_time = purchase_time + timedelta(minutes=random.randint(5, 30))
        carrier_date = approved_time + timedelta(days=random.randint(1, 2))
        delivered_date = carrier_date + timedelta(days=random.randint(2, 5))
        estimated_delivery = delivered_date + timedelta(days=random.randint(1, 3))

        # ----------------
        # Orders table
        # ----------------

        orders.append({
            "order_id": order_id,
            "customer_id": customer["customer_id"],
            "order_status": "delivered",
            "order_purchase_timestamp": purchase_time,
            "order_approved_at": approved_time,
            "order_delivered_carrier_date": carrier_date,
            "order_delivered_customer_date": delivered_date,
            "order_estimated_delivery_date": estimated_delivery
        })

        # ----------------
        # MULTI ITEM LOGIC
        # ----------------

        num_items = random.randint(1, 5)
        total_payment = 0

        for item_id in range(1, num_items + 1):

            product = products.sample(1).iloc[0]
            freight = round(random.uniform(10, 50), 2)
            order_items.append({
                "order_id": order_id,
                "order_item_id": item_id,
                "product_id": product["product_id"],
                "seller_id": fake.uuid4(),
                "shipping_limit_date": approved_time + timedelta(days=2),
                "price": product["price"],
                "freight_value": freight
            })

            total_payment += product["price"] + freight

        # ----------------
        # Payments table
        # ----------------

        payments.append({
            "order_id": order_id,
            "payment_sequential": 1,
            "payment_type": random.choice(
                ["credit_card", "debit_card", "voucher", "boleto"]
            ),
            "payment_installments": random.randint(1, 6),
            "payment_value": round(total_payment, 2)
        })

    return (
        pd.DataFrame(orders),
        pd.DataFrame(order_items),
        pd.DataFrame(payments)
    )

def fetch_users(base_url,limit,skip):
    api_url = f"{base_url}?limit={limit}&skip={skip}"
    data = requests.get(api_url).json()
    df = pd.json_normalize(data["users"])
    return df, data["total"]

def fetch_products(base_url,limit, skip):
    api_url = f"{base_url}?limit={limit}&skip={skip}"
    data = requests.get(api_url).json()
    df = pd.json_normalize(data["products"])
    return df, data["total"]

def read_from_s3(bucket_name, prefix, table_name):
    
    table_path = f"s3://{bucket_name}/{prefix}{table_name}/"
    df = pd.read_parquet(table_path,engine="pyarrow",storage_options={"anon": False})
    return df

def upload_data_to_s3(df, bucket_name, prefix, table_name):
    if df is None or df.empty:
        return
    today = datetime.now().strftime("%Y-%m-%d")

    file_prefix = f"s3://{bucket_name}/{prefix}{table_name}/ingestion_date={today}/{table_name}.parquet"
    df.to_parquet(
        file_prefix,
        engine="pyarrow",
        compression="snappy",
        index=False,
        storage_options={"anon": False}
        )

def transform_customers(df, generate_hash):
    df["customer_id"] = df["id"].apply(lambda x: generate_hash("dummyjson_customer", x))
    df["customer_unique_id"] = df["username"].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    df["customer_city"] = df["address.city"]
    df["customer_state"] = df["address.state"]
    df["customer_zip_code_prefix"] = df["address.postalCode"]
    df["first_name"] = df["firstName"]
    df["last_name"] = df["lastName"]

    return df[["customer_id","customer_unique_id","first_name","last_name","email","phone","customer_zip_code_prefix","customer_city","customer_state"]]

def transform_products(df, generate_hash):
    df["product_id"] = df["id"].apply(lambda x: generate_hash("dummyjson_product", x))
    df["product_category_name"] = df["category"]
    df["product_name_length"] = df["title"].str.len()
    df["product_description_length"] = df["description"].str.len()
    df["product_photos_qty"] = df["images"].apply(len)
    df["product_weight_g"] = df['weight']
    df["product_length_cm"] = df['dimensions.depth']
    df["product_height_cm"] = df['dimensions.height']
    df["product_width_cm"] = df['dimensions.width']
    return df[["product_id","product_category_name","product_name_length","product_description_length","product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm","price","stock","rating"]]

def daily_ingestion_pipline():
    from utils.state_manager import load_state, save_state
    from utils.id_generator import generate_hash, generate_uuid
    from utils.config_reader import read_config
    
    config = read_config()
    bucket_name = config.get('aws_upload_bucket','bucket_name')
    daily_prefix = config.get('aws_upload_bucket','daily_prefix')
    products_url = config.get('api_urls', 'products')
    users_url = config.get('api_urls', 'users')
    LIMIT_USERS = 20
    LIMIT_PRODUCTS = 30
    state = load_state()

    users_offset = state["users_offset"]
    products_offset = state["products_offset"]

    users_df, total_users = fetch_users(users_url, limit=LIMIT_USERS, skip=users_offset)
    products_df, total_products = fetch_products(products_url, limit=LIMIT_PRODUCTS, skip=products_offset)
    
    if users_offset < total_users:
            customers = transform_customers(users_df, generate_hash)
            upload_data_to_s3(customers, bucket_name, daily_prefix, "customers")
            state["users_offset"] += LIMIT_USERS
    else:
        print("All users have been ingested.")

    if products_offset < total_products:
            products = transform_products(products_df, generate_hash)
            upload_data_to_s3(products, bucket_name, daily_prefix, "products")
            state["products_offset"] += LIMIT_PRODUCTS
    else:
        print("All products have been ingested.")

    save_state(state)

    all_customers = read_from_s3(bucket_name, daily_prefix, "customers")
    all_products = read_from_s3(bucket_name, daily_prefix, "products")

    orders, items, payments = generate_orders(
        all_customers,
        all_products,
        generate_uuid
    )

    upload_data_to_s3(orders, bucket_name, daily_prefix, "orders")
    upload_data_to_s3(items, bucket_name, daily_prefix, "order_items")
    upload_data_to_s3(payments, bucket_name, daily_prefix, "payments")


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    daily_ingestion_pipline()