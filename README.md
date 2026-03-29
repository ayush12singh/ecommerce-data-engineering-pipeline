# 🚀 End-to-End E-commerce Data Engineering Pipeline

## 📌 Overview

This project implements a **production-grade, end-to-end data engineering pipeline** that ingests, processes, and transforms e-commerce data into an analytics-ready format.

The pipeline combines:

* Historical batch data (Olist dataset)
* Simulated real-time incremental data (DummyJSON API)

It follows a **Medallion Architecture (Raw → Bronze → Silver → Gold)** and leverages **event-driven processing** using Snowflake.

---

## 🏗️ Architecture

```
DummyJSON API + Olist Dataset
            ↓
     Airflow DAGs
            ↓
     Python Ingestion
            ↓
        Amazon S3
   (Raw / Bronze Layer)
            ↓
       AWS Glue (Spark)
            ↓
        S3 Silver Layer
            ↓
        Snowpipe (Auto Ingest)
            ↓
   Snowflake Silver Tables
            ↓
   Streams (Change Tracking)
            ↓
   Tasks (Automation)
            ↓
        MERGE Logic
            ↓
   Snowflake Gold Tables
            ↓
        Analytics
```

---

## ⚙️ Tech Stack

* AWS S3 (Data Lake)
* AWS Glue (ETL with Spark)
* Apache Airflow (Orchestration)
* Python
* Snowflake
* Docker

---

## 📂 Project Structure

```
OLIST_ETL_DATALAKE/

├── dags/
│   ├── daily_api_ingestion_dag.py
│   ├── olist_bronze_ingestion_dag.py
│
├── etl_codes/
│   ├── ingest_daily_data.py
│   ├── upload_to_s3.py
│
├── config/
│   ├── configs.conf
│
├── utils/
│
├── data/
│
├── logs/
├── state/
│   ├── pipeline_state.json
│
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

---

## 🔄 Pipeline Workflow

### 1️⃣ Data Ingestion (Airflow)

Airflow DAGs orchestrate ingestion:

* `daily_api_ingestion_dag.py`

  * Fetches incremental data from DummyJSON API
* `olist_bronze_ingestion_dag.py`

  * Loads historical dataset into S3

---

### 2️⃣ Data Lake Layers (S3)

#### Raw Layer

* Stores original Olist dataset

#### Bronze Layer

* Stores daily API ingestion
* Partitioned by `ingestion_date`

#### Silver Layer

* Created using AWS Glue (Spark)
* Combines raw + incremental data
* Deduplicated and cleaned

---

### 3️⃣ Snowflake Ingestion

* Snowpipe automatically loads Silver data from S3 into Snowflake
* Uses S3 event notifications for real-time ingestion

---

### 4️⃣ Incremental Processing (Core Feature)

#### Streams

* Created on Silver tables
* Track newly inserted data

#### Tasks

* Triggered using `SYSTEM$STREAM_HAS_DATA`
* Execute MERGE logic

---

### 5️⃣ Gold Layer (Star Schema)

#### Fact Table

* `fact_orders`
* Grain: `(order_id, product_id)`

#### Dimensions

* `dim_customers`
* `dim_products`

---

## 🔥 Key Features

* ✅ End-to-end data pipeline (batch + incremental)
* ✅ Event-driven ingestion using Snowpipe
* ✅ Incremental processing using Streams & Tasks
* ✅ Automated data transformation with MERGE
* ✅ Medallion architecture implementation
* ✅ Airflow-based orchestration
* ✅ Dockerized setup for reproducibility

---

## ⚡ Key Engineering Challenges & Solutions

### 🔴 Duplicate Rows in Fact Table

* Cause: Join multiplication between order_items and payments
* Solution:

  * Applied window functions
  * Enforced fact table grain using `ROW_NUMBER`
  * Aggregated payment data

---

### 🔴 Incremental Processing

* Challenge: Avoid full reloads
* Solution:

  * Used Snowflake Streams for change tracking
  * Implemented MERGE-based upserts

---

## 📊 Sample Analytics Queries

### Total Revenue

```sql
SELECT SUM(payment_value) FROM gold.fact_orders;
```

### Top Products

```sql
SELECT product_id, SUM(price) AS revenue
FROM gold.fact_orders
GROUP BY product_id
ORDER BY revenue DESC;
```

---

## 🚀 How to Run the Project

### 1. Start Airflow (Docker)

```bash
docker-compose up
```

---

### 2. Trigger DAGs

* Run ingestion DAGs from Airflow UI

---

### 3. Data Flow

* Data gets ingested → S3 → Glue → Silver
* Snowpipe loads into Snowflake automatically
* Tasks update Gold tables

---

## 🧠 Key Learnings

* Built a **production-style data pipeline**
* Implemented **event-driven architecture**
* Designed **incremental ETL workflows**
* Solved **real-world data duplication issues**
* Used **Snowflake Streams & Tasks for automation**

---

## 💼 Resume Highlight

> Built an end-to-end event-driven data pipeline using Snowpipe, Streams, and Tasks in Snowflake, enabling incremental data processing and automated transformation into a star schema.

---

## 📌 Future Improvements

* Add SCD Type 2 for dimensions
* Integrate data quality checks
* Add BI dashboard (Power BI / Tableau)
* Implement CI/CD pipeline

---

## 🤝 Acknowledgements

* Olist E-commerce Dataset
* DummyJSON API
