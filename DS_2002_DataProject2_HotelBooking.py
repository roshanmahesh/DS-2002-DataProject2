# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # DS-2002 Data Project 2 – Hotel Booking Data Lakehouse
# MAGIC **Author:** Roshan Mahesh (dmg4fy)
# MAGIC
# MAGIC **Course:** DS-2002 – Data Science Systems
# MAGIC
# MAGIC **Date:** May 2026
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook builds on Data Project 1 (Hotel Booking ETL Pipeline) and implements a full **dimensional Data Lakehouse** using **Azure Databricks** with **Delta Tables** and **Structured Streaming**.
# MAGIC
# MAGIC ### Business Process
# MAGIC The **hotel booking and reservation** process from two Portuguese hotels (City Hotel and Resort Hotel), sourced from 119,390 real booking records spanning July 2015 – August 2017 (Antonio, Almeida & Nunes, 2019).
# MAGIC
# MAGIC ### Architecture: Bronze → Silver → Gold (Medallion)
# MAGIC | Layer | Description |
# MAGIC |-------|-------------|
# MAGIC | **Bronze** | Raw streaming fact data ingested via Spark AutoLoader from 3 JSON files |
# MAGIC | **Silver** | Cleansed & enriched fact data joined with dimension tables |
# MAGIC | **Gold** | Aggregated analytical tables optimized for business queries |
# MAGIC
# MAGIC ### Data Sources (3 required)
# MAGIC | # | Source Type | Technology | Tables/Data |
# MAGIC |---|-------------|------------|-------------|
# MAGIC | 1 | Relational Database | **Azure SQL Database** | `hotels`, `room_types` (reference dims) |
# MAGIC | 2 | NoSQL Database | **MongoDB Atlas** | `guest_profiles` (177 country-level dim) |
# MAGIC | 3 | File System | **DBFS (Databricks File System)** | `fact_stream_2015/2016/2017.json` (streaming) + CSV dimension source |
# MAGIC
# MAGIC ### Star Schema
# MAGIC ```
# MAGIC               dim_date
# MAGIC                  │
# MAGIC  dim_hotel ── fact_bookings ── dim_guest_origin
# MAGIC                  │
# MAGIC             dim_room_type
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1 – Setup & Configuration
# MAGIC
# MAGIC Install required libraries and configure all connection settings for Azure SQL and MongoDB Atlas.
# MAGIC Install `pymongo` for MongoDB connectivity and `pyodbc` / `mssql` driver for Azure SQL.

# COMMAND ----------

# Install required packages
%pip install pymongo pymssql

# COMMAND ----------

# Restart Python to pick up installed packages
dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import json
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, LongType, DateType
)

spark = SparkSession.builder.getOrCreate()

# MAGIC %md
# MAGIC ### 1.1 – Connection Configuration
# MAGIC
# MAGIC **IMPORTANT:** Update the placeholders below with your actual credentials before running.
# MAGIC Azure SQL credentials can be stored as Databricks secrets for security:
# MAGIC `dbutils.secrets.get(scope="ds2002", key="azure-sql-password")`

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────
# AZURE SQL – Relational source (hotels & room_types reference tables)
# Replace with your actual Azure SQL server name, database, user, password
# ─────────────────────────────────────────────────────────────────
AZURE_SQL_SERVER   = "<your-server>.database.windows.net"
AZURE_SQL_DATABASE = "hotel_reference"
AZURE_SQL_USER     = "<your-username>"
AZURE_SQL_PASSWORD = "<your-password>"   # or: dbutils.secrets.get("ds2002","azure-sql-pass")

AZURE_SQL_URL = (
    f"jdbc:sqlserver://{AZURE_SQL_SERVER}:1433;"
    f"database={AZURE_SQL_DATABASE};"
    f"user={AZURE_SQL_USER};"
    f"password={AZURE_SQL_PASSWORD};"
    f"encrypt=true;trustServerCertificate=false;"
    f"hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
)
AZURE_SQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# ─────────────────────────────────────────────────────────────────
# MONGODB ATLAS – NoSQL source (guest_profiles → dim_guest_origin)
# Replace with your Atlas connection string
# ─────────────────────────────────────────────────────────────────
MONGO_URI        = "mongodb+srv://<user>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority"
MONGO_DB         = "hotel_booking_nosql"
MONGO_COLLECTION = "guest_profiles"

# ─────────────────────────────────────────────────────────────────
# DBFS paths – streaming JSON files and Delta table storage
# ─────────────────────────────────────────────────────────────────
DBFS_STREAMING_DIR  = "/FileStore/hotel_streaming"       # 3 JSON fact files land here
DBFS_CHECKPOINT_DIR = "/FileStore/checkpoints"
DELTA_BASE          = "/delta/hotel_lakehouse"

# Delta table paths
BRONZE_PATH = f"{DELTA_BASE}/bronze/fact_bookings_raw"
SILVER_PATH = f"{DELTA_BASE}/silver/fact_bookings_enriched"
GOLD_REV    = f"{DELTA_BASE}/gold/revenue_by_hotel_year"
GOLD_CTRY   = f"{DELTA_BASE}/gold/top_countries_revenue"
GOLD_ROOM   = f"{DELTA_BASE}/gold/room_type_performance"

print("✔  Configuration loaded.")
print(f"   Azure SQL: {AZURE_SQL_SERVER} / {AZURE_SQL_DATABASE}")
print(f"   MongoDB:   {MONGO_DB}.{MONGO_COLLECTION}")
print(f"   DBFS streaming dir: {DBFS_STREAMING_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 2 – Prepare DBFS: Upload Streaming JSON Files
# MAGIC
# MAGIC The original `booking_transactions.csv` (119,390 rows) is split by **arrival year** into three JSON files to simulate a real-time streaming source arriving in distinct intervals:
# MAGIC
# MAGIC | File | Year | Rows | Represents |
# MAGIC |------|------|------|-----------|
# MAGIC | `fact_stream_2015.json` | 2015 | ~21,996 | Interval 1 |
# MAGIC | `fact_stream_2016.json` | 2016 | ~56,707 | Interval 2 |
# MAGIC | `fact_stream_2017.json` | 2017 | ~40,687 | Interval 3 |
# MAGIC
# MAGIC **How to upload:** In the Databricks UI go to **Data → DBFS → FileStore → hotel_streaming** and upload each JSON file, OR run the cell below which generates and writes them directly to DBFS.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 – Generate & Write Streaming JSON Files to DBFS
# MAGIC
# MAGIC This cell reads the master CSV (uploaded to DBFS at `/FileStore/hotel_streaming/booking_transactions.csv`)
# MAGIC and splits it into the 3 yearly JSON streaming files. Run once before the streaming cells.

# COMMAND ----------

MONTH_MAP = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12
}

# Read master CSV from DBFS
csv_dbfs_path = f"dbfs:{DBFS_STREAMING_DIR}/booking_transactions.csv"
df_master = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_dbfs_path)

# Convert month name to number and build arrival_date
df_master = df_master.withColumn(
    "month_num",
    F.create_map([F.lit(k) for kv in MONTH_MAP.items() for k in kv])["arrival_date_month"]
).withColumn(
    "arrival_date",
    F.to_date(
        F.concat_ws("-",
            F.col("arrival_date_year").cast("string"),
            F.lpad(F.col("month_num").cast("string"), 2, "0"),
            F.lpad(F.col("arrival_date_day_of_month").cast("string"), 2, "0")
        ), "yyyy-MM-dd"
    )
).withColumn(
    "date_key",
    F.date_format(F.col("arrival_date"), "yyyyMMdd").cast("int")
)

# Add row-number as booking_id surrogate
from pyspark.sql.window import Window
df_master = df_master.withColumn(
    "booking_id",
    F.row_number().over(Window.orderBy(F.col("arrival_date"), F.col("hotel")))
)

# Compute derived measures once
df_master = (
    df_master
    .withColumn("total_nights", F.col("stays_in_weekend_nights") + F.col("stays_in_week_nights"))
    .withColumn("total_guests", F.col("adults") + F.col("children") + F.col("babies"))
    .withColumn("total_revenue",
        F.when(F.col("adr") < 0, 0.0)
         .otherwise(F.round(F.col("adr") * F.col("total_nights"), 2))
    )
    .withColumn("adr", F.when(F.col("adr") < 0, 0.0).otherwise(F.col("adr")))
)

# Select final streaming payload columns (mirrors fact schema)
STREAM_COLS = [
    "booking_id", "date_key", "arrival_date",
    "hotel", "reserved_room_type",
    "is_canceled", "lead_time",
    "stays_in_weekend_nights", "stays_in_week_nights", "total_nights",
    "adults", "children", "babies", "total_guests",
    "meal", "country", "market_segment", "distribution_channel",
    "is_repeated_guest", "deposit_type", "customer_type",
    "adr", "total_revenue", "total_of_special_requests", "reservation_status",
    "arrival_date_year"
]
df_stream = df_master.select(STREAM_COLS)

# Write 3 yearly JSON files to DBFS
for year in [2015, 2016, 2017]:
    out_path = f"dbfs:{DBFS_STREAMING_DIR}/fact_stream_{year}.json"
    df_year = df_stream.filter(F.col("arrival_date_year") == year)
    cnt = df_year.count()
    # Write as single JSON lines file
    df_year.coalesce(1).write.mode("overwrite").json(out_path + "_tmp")
    # Rename the part file
    files = dbutils.fs.ls(out_path + "_tmp")
    part_file = [f.path for f in files if f.name.startswith("part-")][0]
    dbutils.fs.cp(part_file, out_path)
    dbutils.fs.rm(out_path + "_tmp", recurse=True)
    print(f"  ✔  fact_stream_{year}.json  →  {cnt:,} rows  →  {out_path}")

print("\n✔  All 3 streaming JSON files written to DBFS.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 3 – Batch Load: Dimension Tables (Static Reference Data)
# MAGIC
# MAGIC Dimension tables are **static reference data** loaded via batch ETL. They are extracted from
# MAGIC two cloud sources — **Azure SQL** (relational) and **MongoDB Atlas** (NoSQL) — and one
# MAGIC **DBFS file** (CSV), then written as **Delta tables** into the Silver layer where they serve
# MAGIC as the reference data joined to the streaming Bronze fact data.
# MAGIC
# MAGIC This satisfies Functional Requirement 1: **batch execution with incremental load demonstration**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 – dim_date (Generated from data range)
# MAGIC
# MAGIC The date dimension covers every day in the dataset (July 1, 2015 – August 31, 2017).
# MAGIC It is generated programmatically from the dataset's date range and written as a Delta table.

# COMMAND ----------

# Generate date dimension covering full dataset range
date_rows = []
start = datetime(2015, 7, 1)
end   = datetime(2017, 8, 31)
current = start
day_names = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
month_names = ["January","February","March","April","May","June",
               "July","August","September","October","November","December"]

while current <= end:
    date_rows.append({
        "date_key":     int(current.strftime("%Y%m%d")),
        "full_date":    current.date(),
        "day_of_month": current.day,
        "month_number": current.month,
        "month_name":   month_names[current.month - 1],
        "quarter":      (current.month - 1) // 3 + 1,
        "year":         current.year,
        "day_of_week":  current.weekday(),
        "day_name":     day_names[current.weekday()],
        "is_weekend":   1 if current.weekday() >= 5 else 0
    })
    current = current.replace(day=current.day + 1) if current.day < 28 else \
              (current.replace(month=current.month + 1, day=1) if current.month < 12 else
               datetime(current.year + 1, 1, 1))

dim_date_pd = pd.DataFrame(date_rows)
dim_date_sdf = spark.createDataFrame(dim_date_pd)

dim_date_sdf.write.format("delta").mode("overwrite").save(f"{DELTA_BASE}/dims/dim_date")
spark.sql(f"CREATE TABLE IF NOT EXISTS dim_date USING DELTA LOCATION '{DELTA_BASE}/dims/dim_date'")

print(f"✔  dim_date: {dim_date_sdf.count():,} rows → Delta table written")
dim_date_sdf.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 – dim_hotel & dim_room_type (Source: Azure SQL – Relational Database)
# MAGIC
# MAGIC `hotels` and `room_types` are reference tables from **Azure SQL Database** (`hotel_reference` DB),
# MAGIC which was migrated from the local MySQL used in Project 1. These are extracted via JDBC,
# MAGIC transformed to add descriptive columns, and written as Delta dimension tables.

# COMMAND ----------

# ── EXTRACT from Azure SQL ──────────────────────────────────────────────────
hotels_sdf = (
    spark.read
    .format("jdbc")
    .option("url", AZURE_SQL_URL)
    .option("dbtable", "hotels")
    .option("driver", AZURE_SQL_DRIVER)
    .load()
)
room_types_sdf = (
    spark.read
    .format("jdbc")
    .option("url", AZURE_SQL_URL)
    .option("dbtable", "room_types")
    .option("driver", AZURE_SQL_DRIVER)
    .load()
)

print(f"Extracted from Azure SQL:")
print(f"  hotels:     {hotels_sdf.count()} rows, {len(hotels_sdf.columns)} columns")
print(f"  room_types: {room_types_sdf.count()} rows, {len(room_types_sdf.columns)} columns")

# ── TRANSFORM: dim_hotel ─────────────────────────────────────────────────────
# Source: 2 columns (hotel_id, hotel_name) → Destination: 3 columns (+ hotel_type)
dim_hotel_sdf = (
    hotels_sdf
    .withColumnRenamed("hotel_id", "hotel_key")
    .withColumnRenamed("hotel_name", "hotel_name")
    .withColumn("hotel_type",
        F.when(F.col("hotel_name").contains("Resort"), F.lit("Resort"))
         .otherwise(F.lit("City"))
    )
)

# ── TRANSFORM: dim_room_type ─────────────────────────────────────────────────
dim_room_type_sdf = (
    room_types_sdf
    .withColumnRenamed("room_type_id", "room_type_key")
)

# ── LOAD to Delta ────────────────────────────────────────────────────────────
dim_hotel_sdf.write.format("delta").mode("overwrite").save(f"{DELTA_BASE}/dims/dim_hotel")
spark.sql(f"CREATE TABLE IF NOT EXISTS dim_hotel USING DELTA LOCATION '{DELTA_BASE}/dims/dim_hotel'")

dim_room_type_sdf.write.format("delta").mode("overwrite").save(f"{DELTA_BASE}/dims/dim_room_type")
spark.sql(f"CREATE TABLE IF NOT EXISTS dim_room_type USING DELTA LOCATION '{DELTA_BASE}/dims/dim_room_type'")

print("\n✔  dim_hotel written to Delta:")
dim_hotel_sdf.show()
print("✔  dim_room_type written to Delta:")
dim_room_type_sdf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 – dim_guest_origin (Source: MongoDB Atlas – NoSQL Database)
# MAGIC
# MAGIC `guest_profiles` is a MongoDB collection with 119,390 documents (one per booking), containing
# MAGIC country, market segment, and prior cancellation data. We aggregate these into a country-level
# MAGIC dimension table (`dim_guest_origin`) with 177 rows — one per unique guest origin country.
# MAGIC
# MAGIC **Column mapping:** 9 MongoDB fields → 5 destination columns (aggregated/reduced).

# COMMAND ----------

import pymongo

# ── EXTRACT from MongoDB Atlas ───────────────────────────────────────────────
mongo_client = pymongo.MongoClient(MONGO_URI)
mongo_col    = mongo_client[MONGO_DB][MONGO_COLLECTION]
doc_count    = mongo_col.count_documents({})
print(f"Connected to MongoDB Atlas: {MONGO_DB}.{MONGO_COLLECTION}  ({doc_count:,} documents)")

# Pull all documents (9 fields each) → pandas → aggregate → Spark
guest_docs      = list(mongo_col.find({}, {"_id": 0}))
guest_pd        = pd.DataFrame(guest_docs)
mongo_client.close()

print(f"Extracted {len(guest_pd):,} guest profile documents, {len(guest_pd.columns)} fields")

# ── TRANSFORM: aggregate by country ─────────────────────────────────────────
# 9 source fields → 5 destination columns in dim_guest_origin
countries_pd = (
    guest_pd.groupby("country")
    .agg(
        total_bookings       = ("country", "size"),
        cancellation_rate    = ("previous_cancellations", lambda x: round((x > 0).mean(), 4)),
        top_market_segment   = ("market_segment", lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else "Unknown")
    )
    .reset_index()
    .rename(columns={"country": "country_code"})
)
countries_pd.insert(0, "guest_origin_key", range(1, len(countries_pd) + 1))
print(f"Aggregated {doc_count:,} documents → {len(countries_pd)} country-level dimension rows")
print(f"Column transformation: 9 MongoDB fields → {len(countries_pd.columns)} destination columns")

# ── LOAD to Delta ────────────────────────────────────────────────────────────
dim_guest_origin_sdf = spark.createDataFrame(countries_pd)
dim_guest_origin_sdf.write.format("delta").mode("overwrite").save(f"{DELTA_BASE}/dims/dim_guest_origin")
spark.sql(f"CREATE TABLE IF NOT EXISTS dim_guest_origin USING DELTA LOCATION '{DELTA_BASE}/dims/dim_guest_origin'")

print(f"\n✔  dim_guest_origin written to Delta  ({len(countries_pd)} rows)")
dim_guest_origin_sdf.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 – Batch Incremental Load Demonstration
# MAGIC
# MAGIC To satisfy **Functional Requirement 1** (batch incremental load), the cell below demonstrates
# MAGIC appending a new hotel record to `dim_hotel` — simulating how a new property would be added
# MAGIC to the reference data mart without rewriting the full table (using Delta's `MERGE` / append).

# COMMAND ----------

# Simulate an incremental batch load: add a new hotel to dim_hotel
new_hotel_data = [(99, "Lisbon Boutique Hotel", "City")]
new_hotel_schema = StructType([
    StructField("hotel_key",  IntegerType(), False),
    StructField("hotel_name", StringType(),  False),
    StructField("hotel_type", StringType(),  False),
])
new_hotel_sdf = spark.createDataFrame(new_hotel_data, schema=new_hotel_schema)

# Append-mode write = incremental load (adds without dropping existing rows)
new_hotel_sdf.write.format("delta").mode("append").save(f"{DELTA_BASE}/dims/dim_hotel")
print("✔  Incremental batch load: appended 1 new hotel record to dim_hotel")

# Verify
spark.read.format("delta").load(f"{DELTA_BASE}/dims/dim_hotel").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 4 – Bronze Layer: Structured Streaming via AutoLoader
# MAGIC
# MAGIC **Functional Requirement 2:** Use Spark AutoLoader (`cloudFiles`) to read the 3 JSON files
# MAGIC as mini-batches, simulating real-time fact data arriving in 3 intervals.
# MAGIC
# MAGIC AutoLoader continuously monitors the `hotel_streaming` DBFS directory and processes each
# MAGIC newly-arrived JSON file as a mini-batch. Using `trigger(availableNow=True)` processes
# MAGIC all currently available files in sequence and then stops — ideal for structured mini-batch
# MAGIC streaming demos.
# MAGIC
# MAGIC **Data flow:**
# MAGIC ```
# MAGIC DBFS /FileStore/hotel_streaming/
# MAGIC   ├── fact_stream_2015.json  (interval 1 – ~22K rows)
# MAGIC   ├── fact_stream_2016.json  (interval 2 – ~57K rows)
# MAGIC   └── fact_stream_2017.json  (interval 3 – ~41K rows)
# MAGIC          │
# MAGIC          ▼  Spark AutoLoader (cloudFiles format=json)
# MAGIC   Bronze Delta Table  →  /delta/hotel_lakehouse/bronze/fact_bookings_raw
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 – Define Streaming Schema
# MAGIC
# MAGIC AutoLoader requires an explicit schema so Spark can parse the JSON without scanning all files upfront.
# MAGIC Schema matches the columns written in Section 2 when generating the 3 JSON files.

# COMMAND ----------

# Explicit schema for streaming JSON files — mirrors the fact_stream columns written in Section 2
streaming_schema = StructType([
    StructField("booking_id",                 LongType(),    False),
    StructField("date_key",                   IntegerType(), True),
    StructField("arrival_date",               StringType(),  True),
    StructField("hotel",                      StringType(),  True),
    StructField("reserved_room_type",         StringType(),  True),
    StructField("is_canceled",                IntegerType(), True),
    StructField("lead_time",                  IntegerType(), True),
    StructField("stays_in_weekend_nights",    IntegerType(), True),
    StructField("stays_in_week_nights",       IntegerType(), True),
    StructField("total_nights",               IntegerType(), True),
    StructField("adults",                     IntegerType(), True),
    StructField("children",                   IntegerType(), True),
    StructField("babies",                     IntegerType(), True),
    StructField("total_guests",               IntegerType(), True),
    StructField("meal",                       StringType(),  True),
    StructField("country",                    StringType(),  True),
    StructField("market_segment",             StringType(),  True),
    StructField("distribution_channel",       StringType(),  True),
    StructField("is_repeated_guest",          IntegerType(), True),
    StructField("deposit_type",               StringType(),  True),
    StructField("customer_type",              StringType(),  True),
    StructField("adr",                        DoubleType(),  True),
    StructField("total_revenue",              DoubleType(),  True),
    StructField("total_of_special_requests",  IntegerType(), True),
    StructField("reservation_status",         StringType(),  True),
    StructField("arrival_date_year",          IntegerType(), True),
])

print("✔  Streaming schema defined:")
for field in streaming_schema.fields:
    print(f"   {field.name:<35} {field.dataType}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 – Start AutoLoader Stream → Bronze Delta Table
# MAGIC
# MAGIC AutoLoader reads each JSON file as a mini-batch. The checkpoint directory ensures
# MAGIC exactly-once semantics — if the stream is restarted, already-processed files are skipped.
# MAGIC
# MAGIC **Three intervals** are processed sequentially:
# MAGIC - **Interval 1:** `fact_stream_2015.json` (~22K rows)
# MAGIC - **Interval 2:** `fact_stream_2016.json` (~57K rows)
# MAGIC - **Interval 3:** `fact_stream_2017.json` (~41K rows)

# COMMAND ----------

# ── BRONZE: Read streaming JSON via AutoLoader ───────────────────────────────
bronze_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{DBFS_CHECKPOINT_DIR}/bronze_schema")
    .option("cloudFiles.inferColumnTypes", "false")   # use explicit schema
    .schema(streaming_schema)
    .load(f"dbfs:{DBFS_STREAMING_DIR}/fact_stream_*.json")
)

# Add ingestion metadata columns for lineage tracking
bronze_enriched = (
    bronze_stream_df
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file",
        F.regexp_extract(F.input_file_name(), r"(fact_stream_\d{4}\.json)", 1)
    )
)

# ── WRITE to Bronze Delta table ──────────────────────────────────────────────
bronze_query = (
    bronze_enriched.writeStream
    .format("delta")
    .option("checkpointLocation", f"{DBFS_CHECKPOINT_DIR}/bronze_fact")
    .outputMode("append")
    .trigger(availableNow=True)          # processes all 3 files as mini-batches then stops
    .start(BRONZE_PATH)
)

bronze_query.awaitTermination()
print("✔  Bronze streaming complete. All 3 intervals ingested.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 – Verify Bronze Layer
# MAGIC
# MAGIC Confirm all 3 streaming intervals landed in the Bronze Delta table with correct row counts
# MAGIC and source file tracking.

# COMMAND ----------

# Register Bronze table for SQL queries
spark.sql(f"CREATE TABLE IF NOT EXISTS bronze_fact_bookings USING DELTA LOCATION '{BRONZE_PATH}'")

bronze_df = spark.read.format("delta").load(BRONZE_PATH)
total_bronze = bronze_df.count()
print(f"✔  Bronze table total rows: {total_bronze:,}")
print(f"   (Expected: ~119,390 across 3 intervals)\n")

# Show row counts by source file (each file = one streaming interval)
print("Row counts by streaming interval:")
bronze_df.groupBy("_source_file").count().orderBy("_source_file").show()

# Preview schema and sample rows
print("Schema:")
bronze_df.printSchema()
bronze_df.select(
    "booking_id", "arrival_date", "hotel", "reserved_room_type",
    "adr", "total_revenue", "_source_file", "_ingest_timestamp"
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 5 – Silver Layer: Enriched Fact Table (Fact + Dimensions Joined)
# MAGIC
# MAGIC The Silver layer joins the raw Bronze fact data with all four dimension tables to produce
# MAGIC a fully enriched, analysis-ready dataset. This satisfies **Functional Requirement 2b**:
# MAGIC illustrating relationships between real-time fact data and static reference data.
# MAGIC
# MAGIC **Joins applied:**
# MAGIC | Bronze Column | Joins To | Dimension |
# MAGIC |---------------|----------|-----------|
# MAGIC | `hotel` | `hotel_name` | `dim_hotel` |
# MAGIC | `country` | `country_code` | `dim_guest_origin` |
# MAGIC | `reserved_room_type` | `room_code` | `dim_room_type` |
# MAGIC | `date_key` | `date_key` | `dim_date` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 – Load Dimension Delta Tables

# COMMAND ----------

dim_date_delta         = spark.read.format("delta").load(f"{DELTA_BASE}/dims/dim_date")
dim_hotel_delta        = spark.read.format("delta").load(f"{DELTA_BASE}/dims/dim_hotel")
dim_guest_origin_delta = spark.read.format("delta").load(f"{DELTA_BASE}/dims/dim_guest_origin")
dim_room_type_delta    = spark.read.format("delta").load(f"{DELTA_BASE}/dims/dim_room_type")

# Keep only original 2 hotels (hotel_key 1 and 2); exclude the demo incremental record (key 99)
dim_hotel_delta = dim_hotel_delta.filter(F.col("hotel_key") <= 2)

print("Dimension tables loaded from Delta:")
print(f"  dim_date:          {dim_date_delta.count():,} rows")
print(f"  dim_hotel:         {dim_hotel_delta.count()} rows")
print(f"  dim_guest_origin:  {dim_guest_origin_delta.count()} rows")
print(f"  dim_room_type:     {dim_room_type_delta.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 – Join Bronze Fact with Dimensions → Silver

# COMMAND ----------

# Join Bronze fact rows with all dimension tables
silver_df = (
    bronze_df

    # Join → dim_hotel (on hotel name string match)
    .join(
        dim_hotel_delta.select("hotel_key", "hotel_name", "hotel_type"),
        bronze_df["hotel"] == dim_hotel_delta["hotel_name"],
        how="left"
    )

    # Join → dim_guest_origin (on country code)
    .join(
        dim_guest_origin_delta.select("guest_origin_key", "country_code",
                                       "total_bookings", "cancellation_rate",
                                       "top_market_segment"),
        bronze_df["country"] == dim_guest_origin_delta["country_code"],
        how="left"
    )

    # Join → dim_room_type (on room code)
    .join(
        dim_room_type_delta.select("room_type_key", "room_code", "room_name", "base_rate_tier"),
        bronze_df["reserved_room_type"] == dim_room_type_delta["room_code"],
        how="left"
    )

    # Join → dim_date (on integer date key)
    .join(
        dim_date_delta.select("date_key", "full_date", "month_name",
                               "month_number", "quarter", "year",
                               "day_name", "is_weekend"),
        on="date_key",
        how="left"
    )
)

# Select clean Silver output columns (surrogate keys + enriched context)
silver_final = silver_df.select(
    "booking_id",
    "date_key",
    "hotel_key",
    "guest_origin_key",
    "room_type_key",
    "hotel_name",
    "hotel_type",
    "country_code",
    "room_name",
    "base_rate_tier",
    "full_date",
    "year",
    "quarter",
    "month_name",
    "month_number",
    "day_name",
    "is_weekend",
    "is_canceled",
    "lead_time",
    "stays_in_weekend_nights",
    "stays_in_week_nights",
    "total_nights",
    "adults",
    "children",
    "babies",
    "total_guests",
    F.col("meal").alias("meal_plan"),
    "market_segment",
    "distribution_channel",
    "is_repeated_guest",
    "deposit_type",
    "customer_type",
    "adr",
    "total_revenue",
    F.col("total_of_special_requests").alias("special_requests"),
    "reservation_status",
    "_source_file",
    "_ingest_timestamp"
)

# Fill nulls for unmatched foreign keys (should be 0 for unknown)
silver_final = (
    silver_final
    .fillna({"hotel_key": 0, "guest_origin_key": 0, "room_type_key": 0})
)

# Write Silver Delta table
silver_final.write.format("delta").mode("overwrite").save(SILVER_PATH)
spark.sql(f"CREATE TABLE IF NOT EXISTS silver_fact_bookings_enriched USING DELTA LOCATION '{SILVER_PATH}'")

silver_count = silver_final.count()
print(f"✔  Silver table written: {silver_count:,} rows")
print(f"   Columns: {len(silver_final.columns)}")
silver_final.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 – Verify Silver Joins
# MAGIC
# MAGIC Confirm the surrogate key joins resolved correctly — null `hotel_key` or `room_type_key`
# MAGIC would indicate unmatched dimension records.

# COMMAND ----------

silver_sdf = spark.read.format("delta").load(SILVER_PATH)

print("Silver layer join verification:")
print(f"  Total rows:              {silver_sdf.count():,}")
print(f"  Null hotel_key:          {silver_sdf.filter(F.col('hotel_key').isNull()).count()}")
print(f"  Null guest_origin_key:   {silver_sdf.filter(F.col('guest_origin_key').isNull()).count()}")
print(f"  Null room_type_key:      {silver_sdf.filter(F.col('room_type_key').isNull()).count()}")
print(f"  Null date_key:           {silver_sdf.filter(F.col('date_key').isNull()).count()}")
print()

# Sample enriched rows showing fact + dimension columns together
silver_sdf.select(
    "booking_id", "hotel_name", "hotel_type", "room_name",
    "base_rate_tier", "country_code", "full_date",
    "year", "quarter", "adr", "total_revenue", "_source_file"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 6 – Gold Layer: Aggregated Analytical Tables
# MAGIC
# MAGIC The Gold layer materializes pre-aggregated summary tables optimized for business intelligence
# MAGIC queries. Three Gold tables are created from the Silver layer, each answering a specific
# MAGIC business question using joins across 3+ tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 – Gold Table 1: Revenue & Bookings by Hotel and Year
# MAGIC
# MAGIC **Business question:** How do the two hotels compare in revenue, cancellations, and ADR across years?
# MAGIC **Tables used:** `silver_fact_bookings_enriched` (contains joined fact + dim_hotel + dim_date)

# COMMAND ----------

gold_rev_df = (
    silver_sdf
    .groupBy("hotel_name", "hotel_type", "year")
    .agg(
        F.count("booking_id").alias("total_bookings"),
        F.sum("is_canceled").alias("total_cancellations"),
        F.round(F.sum("is_canceled") / F.count("booking_id") * 100, 1).alias("cancel_pct"),
        F.round(F.avg("adr"), 2).alias("avg_daily_rate"),
        F.round(F.avg("total_nights"), 1).alias("avg_stay_nights"),
        F.round(F.sum("total_revenue"), 2).alias("total_revenue")
    )
    .orderBy("year", "hotel_name")
)

gold_rev_df.write.format("delta").mode("overwrite").save(GOLD_REV)
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_revenue_by_hotel_year USING DELTA LOCATION '{GOLD_REV}'")

print("✔  Gold Table 1: Revenue & Bookings by Hotel and Year")
gold_rev_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 – Gold Table 2: Top 15 Guest Countries by Revenue
# MAGIC
# MAGIC **Business question:** Which guest origin countries generate the most revenue, and what is their typical stay profile?
# MAGIC **Tables used:** Silver (fact + dim_guest_origin + dim_hotel context)

# COMMAND ----------

gold_ctry_df = (
    silver_sdf
    .filter(F.col("is_canceled") == 0)          # only completed stays
    .groupBy("country_code", "hotel_name")
    .agg(
        F.count("booking_id").alias("total_bookings"),
        F.round(F.avg("total_nights"), 1).alias("avg_stay_nights"),
        F.round(F.avg("adr"), 2).alias("avg_daily_rate"),
        F.round(F.avg("total_guests"), 1).alias("avg_party_size"),
        F.round(F.sum("total_revenue"), 2).alias("total_revenue")
    )
    .orderBy(F.col("total_revenue").desc())
    .limit(15)
)

gold_ctry_df.write.format("delta").mode("overwrite").save(GOLD_CTRY)
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_top_countries_revenue USING DELTA LOCATION '{GOLD_CTRY}'")

print("✔  Gold Table 2: Top 15 Guest Countries by Revenue (non-cancelled bookings)")
gold_ctry_df.show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 – Gold Table 3: Room Type Performance (Weekend vs Weekday)
# MAGIC
# MAGIC **Business question:** Which room types generate the most revenue, and how does weekend vs weekday split affect ADR?
# MAGIC **Tables used:** Silver (fact + dim_room_type + dim_date context)

# COMMAND ----------

gold_room_df = (
    silver_sdf
    .filter(F.col("is_canceled") == 0)
    .groupBy("room_name", "base_rate_tier")
    .agg(
        F.count("booking_id").alias("total_bookings"),
        F.round(F.avg("stays_in_weekend_nights"), 1).alias("avg_weekend_nights"),
        F.round(F.avg("stays_in_week_nights"), 1).alias("avg_weekday_nights"),
        F.round(F.avg("adr"), 2).alias("avg_daily_rate"),
        F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
        F.round(F.sum("total_revenue") / F.count("booking_id"), 2).alias("revenue_per_booking")
    )
    .orderBy(F.col("total_revenue").desc())
)

gold_room_df.write.format("delta").mode("overwrite").save(GOLD_ROOM)
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_room_type_performance USING DELTA LOCATION '{GOLD_ROOM}'")

print("✔  Gold Table 3: Room Type Performance (Weekend vs Weekday)")
gold_room_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 7 – Business Value Queries
# MAGIC
# MAGIC Analytical SQL queries that demonstrate the business value of the Data Lakehouse.
# MAGIC These queries run against the Gold layer Delta tables using Databricks SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1 – Revenue & Cancellation Trend by Hotel and Year
# MAGIC Directly answers: *"Are cancellation rates improving? Which hotel is more profitable?"*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     hotel_name,
# MAGIC     hotel_type,
# MAGIC     year,
# MAGIC     total_bookings,
# MAGIC     total_cancellations,
# MAGIC     cancel_pct             AS cancellation_pct,
# MAGIC     avg_daily_rate,
# MAGIC     avg_stay_nights,
# MAGIC     total_revenue
# MAGIC FROM gold_revenue_by_hotel_year
# MAGIC ORDER BY year, hotel_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2 – Top 10 Countries by Revenue with Stay Profile
# MAGIC *"Which guest nationalities should marketing prioritize?"*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     country_code,
# MAGIC     hotel_name,
# MAGIC     total_bookings,
# MAGIC     avg_stay_nights,
# MAGIC     avg_daily_rate,
# MAGIC     avg_party_size,
# MAGIC     total_revenue
# MAGIC FROM gold_top_countries_revenue
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3 – Room Type Revenue vs Weekend Proportion
# MAGIC *"Which room types are most valuable for weekend vs weekday pricing strategy?"*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     room_name,
# MAGIC     base_rate_tier,
# MAGIC     total_bookings,
# MAGIC     avg_weekend_nights,
# MAGIC     avg_weekday_nights,
# MAGIC     ROUND(avg_weekend_nights / NULLIF(avg_weekend_nights + avg_weekday_nights, 0) * 100, 1)
# MAGIC         AS pct_weekend_stays,
# MAGIC     avg_daily_rate,
# MAGIC     total_revenue,
# MAGIC     revenue_per_booking
# MAGIC FROM gold_room_type_performance
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 4 – Quarterly Cancellation Rates by Top 5 Countries (Silver layer, multi-dim join)
# MAGIC *"Which countries cancel most by quarter — useful for overbooking policy decisions?"*
# MAGIC Uses Silver layer directly (joins across fact + dim_hotel + dim_date + dim_guest_origin).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     year,
# MAGIC     quarter,
# MAGIC     hotel_name,
# MAGIC     country_code,
# MAGIC     COUNT(booking_id)                                   AS total_bookings,
# MAGIC     SUM(is_canceled)                                    AS cancellations,
# MAGIC     ROUND(SUM(is_canceled) / COUNT(booking_id) * 100, 1) AS cancel_pct,
# MAGIC     ROUND(AVG(lead_time), 0)                            AS avg_lead_time_days
# MAGIC FROM silver_fact_bookings_enriched
# MAGIC WHERE country_code IN ('PRT', 'GBR', 'FRA', 'ESP', 'DEU')
# MAGIC   AND year = 2016
# MAGIC GROUP BY year, quarter, hotel_name, country_code
# MAGIC HAVING total_bookings >= 50
# MAGIC ORDER BY quarter, cancel_pct DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 5 – Monthly Booking Volume and ADR (All Years, Silver layer)
# MAGIC *"When is peak season? How does average rate fluctuate month-to-month?"*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     year,
# MAGIC     month_number,
# MAGIC     month_name,
# MAGIC     hotel_name,
# MAGIC     COUNT(booking_id)                   AS total_bookings,
# MAGIC     SUM(is_canceled)                    AS cancellations,
# MAGIC     ROUND(AVG(adr), 2)                  AS avg_daily_rate,
# MAGIC     ROUND(SUM(total_revenue), 2)        AS total_revenue,
# MAGIC     ROUND(AVG(total_guests), 1)         AS avg_party_size
# MAGIC FROM silver_fact_bookings_enriched
# MAGIC GROUP BY year, month_number, month_name, hotel_name
# MAGIC ORDER BY year, month_number, hotel_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 8 – Delta Table Registry & Summary
# MAGIC
# MAGIC Final verification of all Delta tables created in this notebook.

# COMMAND ----------

print("=" * 65)
print("DATA LAKEHOUSE – DELTA TABLE REGISTRY")
print("=" * 65)

tables = {
    "dim_date":                        f"{DELTA_BASE}/dims/dim_date",
    "dim_hotel":                       f"{DELTA_BASE}/dims/dim_hotel",
    "dim_guest_origin":                f"{DELTA_BASE}/dims/dim_guest_origin",
    "dim_room_type":                   f"{DELTA_BASE}/dims/dim_room_type",
    "bronze_fact_bookings":            BRONZE_PATH,
    "silver_fact_bookings_enriched":   SILVER_PATH,
    "gold_revenue_by_hotel_year":      GOLD_REV,
    "gold_top_countries_revenue":      GOLD_CTRY,
    "gold_room_type_performance":      GOLD_ROOM,
}

print(f"\n{'Table':<40} {'Layer':<10} {'Rows':>10}")
print("-" * 65)
for name, path in tables.items():
    try:
        cnt = spark.read.format("delta").load(path).count()
        layer = "DIM" if "dim" in name else ("BRONZE" if "bronze" in name else
                ("SILVER" if "silver" in name else "GOLD"))
        print(f"  {name:<38} {layer:<10} {cnt:>10,}")
    except Exception as e:
        print(f"  {name:<38} ERROR: {e}")

print("\n" + "=" * 65)
print("PIPELINE COMPLETE")
print(f"  Sources:  Azure SQL  +  MongoDB Atlas  +  DBFS (3 JSON files)")
print(f"  Pattern:  ELT  →  Medallion Architecture (Bronze / Silver / Gold)")
print(f"  Streaming: Spark AutoLoader (3 mini-batch intervals via cloudFiles)")
print("=" * 65)
