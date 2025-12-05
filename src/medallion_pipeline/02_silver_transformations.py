# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Data Cleaning & Standardization
# MAGIC 
# MAGIC This notebook transforms Bronze data into Silver layer:
# MAGIC - Applies data quality rules
# MAGIC - Handles null values and outliers
# MAGIC - Standardizes formats
# MAGIC - Adds derived columns and segmentation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

try:
    catalog = dbutils.widgets.get("catalog")
    target_schema = dbutils.widgets.get("target_schema")
except:
    catalog = "dbdemos_henryk"
    target_schema = "medallion_pipeline"

print(f"Catalog: {catalog}")
print(f"Target Schema: {target_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table 1: Travel Purchases (Cleaned)

# COMMAND ----------

print("Creating silver_travel_purchases...")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{target_schema}.silver_travel_purchases
COMMENT 'Silver layer: Cleaned travel purchases with data quality rules applied'
TBLPROPERTIES (
    'layer' = 'silver',
    'data_quality' = 'validated',
    'pipeline' = 'medallion_travel_analytics'
)
AS 
WITH cleaned_data AS (
    SELECT 
        id as transaction_id,
        ts as transaction_timestamp,
        user_id,
        destination_id,
        COALESCE(clicked, false) as clicked,
        COALESCE(purchased, false) as purchased,
        booking_date,
        CASE 
            WHEN price IS NULL OR price < 0 THEN NULL
            WHEN price > 50000 THEN NULL
            ELSE ROUND(price, 2)
        END as price_usd,
        user_latitude,
        user_longitude,
        CASE 
            WHEN price IS NULL OR price < 0 OR price > 50000 THEN true
            ELSE false
        END as _price_invalid,
        CASE 
            WHEN user_latitude IS NULL OR user_longitude IS NULL THEN true
            WHEN user_latitude < -90 OR user_latitude > 90 THEN true
            WHEN user_longitude < -180 OR user_longitude > 180 THEN true
            ELSE false
        END as _location_invalid,
        EXTRACT(YEAR FROM ts) as transaction_year,
        EXTRACT(MONTH FROM ts) as transaction_month,
        EXTRACT(DAYOFWEEK FROM ts) as transaction_day_of_week,
        EXTRACT(HOUR FROM ts) as transaction_hour,
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM ts) IN (1, 7) THEN 'weekend'
            ELSE 'weekday'
        END as day_type,
        _ingested_at,
        current_timestamp() as _processed_at
    FROM {catalog}.{target_schema}.bronze_travel_purchases
    WHERE id IS NOT NULL 
      AND user_id IS NOT NULL 
      AND destination_id IS NOT NULL
)
SELECT * FROM cleaned_data
""")

spark.sql(f"ALTER TABLE {catalog}.{target_schema}.silver_travel_purchases ALTER COLUMN transaction_id COMMENT 'Unique transaction ID (PK)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.silver_travel_purchases ALTER COLUMN user_id COMMENT 'User ID (FK to silver_users)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.silver_travel_purchases ALTER COLUMN destination_id COMMENT 'Destination ID (FK to silver_destinations)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.silver_travel_purchases ALTER COLUMN price_usd COMMENT 'Validated price in USD'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.silver_travel_purchases ALTER COLUMN _price_invalid COMMENT 'DQ flag: invalid price'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.silver_travel_purchases ALTER COLUMN day_type COMMENT 'weekend or weekday'")

count = spark.table(f"{catalog}.{target_schema}.silver_travel_purchases").count()
invalid = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{target_schema}.silver_travel_purchases WHERE _price_invalid").collect()[0][0]
print(f"Created with {count:,} records ({invalid} with invalid prices)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table 2: Users (Cleaned)

# COMMAND ----------

print("Creating silver_users...")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{target_schema}.silver_users
COMMENT 'Silver layer: Cleaned user data with segmentation'
TBLPROPERTIES (
    'layer' = 'silver',
    'data_quality' = 'validated',
    'pipeline' = 'medallion_travel_analytics'
)
AS 
SELECT 
    user_id,
    ts as last_updated_at,
    ROUND(COALESCE(mean_price_7d, 0), 2) as avg_price_7day,
    COALESCE(last_6m_purchases, 0) as purchases_6month,
    CASE 
        WHEN user_latitude IS NULL OR user_latitude < -90 OR user_latitude > 90 THEN NULL
        ELSE ROUND(user_latitude, 6)
    END as user_latitude,
    CASE 
        WHEN user_longitude IS NULL OR user_longitude < -180 OR user_longitude > 180 THEN NULL
        ELSE ROUND(user_longitude, 6)
    END as user_longitude,
    CASE 
        WHEN last_6m_purchases >= 10 THEN 'high_frequency'
        WHEN last_6m_purchases >= 3 THEN 'medium_frequency'
        ELSE 'low_frequency'
    END as purchase_frequency_segment,
    CASE 
        WHEN mean_price_7d >= 500 THEN 'premium'
        WHEN mean_price_7d >= 200 THEN 'standard'
        ELSE 'budget'
    END as price_segment,
    _ingested_at,
    current_timestamp() as _processed_at
FROM {catalog}.{target_schema}.bronze_user_features
WHERE user_id IS NOT NULL
""")

spark.sql(f"ALTER TABLE {catalog}.{target_schema}.silver_users ALTER COLUMN user_id COMMENT 'Unique user ID (PK)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.silver_users ALTER COLUMN purchase_frequency_segment COMMENT 'User segment: high/medium/low'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.silver_users ALTER COLUMN price_segment COMMENT 'Price preference: premium/standard/budget'")

count = spark.table(f"{catalog}.{target_schema}.silver_users").count()
print(f"Created with {count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table 3: Destinations (Cleaned)

# COMMAND ----------

print("Creating silver_destinations...")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{target_schema}.silver_destinations
COMMENT 'Silver layer: Cleaned destination reference data'
TBLPROPERTIES (
    'layer' = 'silver',
    'data_quality' = 'validated',
    'pipeline' = 'medallion_travel_analytics'
)
AS 
SELECT 
    destination_id,
    TRIM(INITCAP(destination_name)) as destination_name,
    CASE 
        WHEN dest_latitude IS NULL OR dest_latitude < -90 OR dest_latitude > 90 THEN NULL
        ELSE ROUND(dest_latitude, 6)
    END as latitude,
    CASE 
        WHEN dest_longitude IS NULL OR dest_longitude < -180 OR dest_longitude > 180 THEN NULL
        ELSE ROUND(dest_longitude, 6)
    END as longitude,
    CASE 
        WHEN dest_latitude >= 0 THEN 'Northern'
        ELSE 'Southern'
    END as hemisphere,
    _ingested_at,
    current_timestamp() as _processed_at
FROM {catalog}.{target_schema}.bronze_destinations
WHERE destination_id IS NOT NULL
""")

spark.sql(f"ALTER TABLE {catalog}.{target_schema}.silver_destinations ALTER COLUMN destination_id COMMENT 'Unique destination ID (PK)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.silver_destinations ALTER COLUMN destination_name COMMENT 'Standardized name (Title Case)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.silver_destinations ALTER COLUMN hemisphere COMMENT 'Northern or Southern'")

count = spark.table(f"{catalog}.{target_schema}.silver_destinations").count()
print(f"Created with {count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("SILVER LAYER COMPLETE")
print("=" * 60)
print(f"\nTables created in {catalog}.{target_schema}:")
print("  - silver_travel_purchases")
print("  - silver_users")
print("  - silver_destinations")
