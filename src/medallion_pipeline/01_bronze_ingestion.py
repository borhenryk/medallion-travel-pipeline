# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Raw Data Ingestion
# MAGIC 
# MAGIC This notebook ingests raw travel data from source tables into the Bronze layer.
# MAGIC - Preserves original data without transformations
# MAGIC - Adds audit columns for lineage tracking
# MAGIC - Creates MD5 hash for change data capture

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get parameters from job or use defaults
try:
    catalog = dbutils.widgets.get("catalog")
    source_schema = dbutils.widgets.get("source_schema")
    target_schema = dbutils.widgets.get("target_schema")
except:
    catalog = "dbdemos_henryk"
    source_schema = "dbdemos_fs_travel"
    target_schema = "medallion_pipeline"

print(f"Catalog: {catalog}")
print(f"Source Schema: {source_schema}")
print(f"Target Schema: {target_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema if Not Exists

# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog}.{target_schema}
COMMENT 'Medallion Architecture Data Engineering Pipeline - Bronze, Silver, Gold layers for travel data analytics'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Table 1: Travel Purchases

# COMMAND ----------

print("Creating bronze_travel_purchases...")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{target_schema}.bronze_travel_purchases
COMMENT 'Bronze layer: Raw travel purchase transactions - unmodified source data with ingestion metadata'
TBLPROPERTIES (
    'layer' = 'bronze',
    'source_system' = '{source_schema}',
    'source_table' = 'travel_purchase',
    'pipeline' = 'medallion_travel_analytics'
)
AS SELECT 
    id,
    ts,
    user_id,
    destination_id,
    clicked,
    purchased,
    booking_date,
    price,
    user_latitude,
    user_longitude,
    current_timestamp() as _ingested_at,
    '{source_schema}.travel_purchase' as _source_table,
    md5(concat_ws('|', id, ts, user_id, destination_id)) as _row_hash
FROM {catalog}.{source_schema}.travel_purchase
""")

# Add column descriptions
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.bronze_travel_purchases ALTER COLUMN id COMMENT 'Unique transaction identifier (Primary Key)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.bronze_travel_purchases ALTER COLUMN ts COMMENT 'Transaction timestamp'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.bronze_travel_purchases ALTER COLUMN user_id COMMENT 'User identifier (Foreign Key)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.bronze_travel_purchases ALTER COLUMN destination_id COMMENT 'Destination identifier (Foreign Key)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.bronze_travel_purchases ALTER COLUMN purchased COMMENT 'Whether user completed the purchase'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.bronze_travel_purchases ALTER COLUMN price COMMENT 'Transaction price in USD'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.bronze_travel_purchases ALTER COLUMN _ingested_at COMMENT 'Timestamp when record was ingested'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.bronze_travel_purchases ALTER COLUMN _row_hash COMMENT 'MD5 hash for CDC'")

count = spark.table(f"{catalog}.{target_schema}.bronze_travel_purchases").count()
print(f"Created with {count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Table 2: User Features

# COMMAND ----------

print("Creating bronze_user_features...")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{target_schema}.bronze_user_features
COMMENT 'Bronze layer: Raw user feature data - user attributes and behavior metrics'
TBLPROPERTIES (
    'layer' = 'bronze',
    'source_system' = '{source_schema}',
    'source_table' = 'user_features',
    'pipeline' = 'medallion_travel_analytics'
)
AS SELECT 
    user_id,
    ts,
    mean_price_7d,
    last_6m_purchases,
    user_longitude,
    user_latitude,
    current_timestamp() as _ingested_at,
    '{source_schema}.user_features' as _source_table
FROM {catalog}.{source_schema}.user_features
""")

spark.sql(f"ALTER TABLE {catalog}.{target_schema}.bronze_user_features ALTER COLUMN user_id COMMENT 'Unique user identifier (Primary Key)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.bronze_user_features ALTER COLUMN mean_price_7d COMMENT 'Average price over last 7 days'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.bronze_user_features ALTER COLUMN last_6m_purchases COMMENT 'Purchases in last 6 months'")

count = spark.table(f"{catalog}.{target_schema}.bronze_user_features").count()
print(f"Created with {count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Table 3: Destinations

# COMMAND ----------

print("Creating bronze_destinations...")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{target_schema}.bronze_destinations
COMMENT 'Bronze layer: Raw destination location data - travel destination reference data'
TBLPROPERTIES (
    'layer' = 'bronze',
    'source_system' = '{source_schema}',
    'source_table' = 'destination_location',
    'pipeline' = 'medallion_travel_analytics'
)
AS SELECT 
    destination_id,
    name as destination_name,
    latitude as dest_latitude,
    longitude as dest_longitude,
    current_timestamp() as _ingested_at,
    '{source_schema}.destination_location' as _source_table
FROM {catalog}.{source_schema}.destination_location
""")

spark.sql(f"ALTER TABLE {catalog}.{target_schema}.bronze_destinations ALTER COLUMN destination_id COMMENT 'Unique destination identifier (Primary Key)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.bronze_destinations ALTER COLUMN destination_name COMMENT 'Destination name'")

count = spark.table(f"{catalog}.{target_schema}.bronze_destinations").count()
print(f"Created with {count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("BRONZE LAYER COMPLETE")
print("=" * 60)
print(f"\nTables created in {catalog}.{target_schema}:")
print("  - bronze_travel_purchases")
print("  - bronze_user_features")
print("  - bronze_destinations")
