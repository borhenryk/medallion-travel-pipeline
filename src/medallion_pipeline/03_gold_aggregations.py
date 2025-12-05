# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Aggregated Analytics
# MAGIC 
# MAGIC This notebook creates business-ready analytics tables:
# MAGIC - Daily revenue metrics
# MAGIC - Destination performance
# MAGIC - User engagement summary
# MAGIC - Monthly executive summary

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
# MAGIC ## Gold Table 1: Daily Revenue Metrics

# COMMAND ----------

print("Creating gold_daily_revenue_metrics...")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{target_schema}.gold_daily_revenue_metrics
COMMENT 'Gold layer: Daily revenue and transaction metrics'
TBLPROPERTIES (
    'layer' = 'gold',
    'business_domain' = 'finance',
    'refresh_frequency' = 'daily',
    'pipeline' = 'medallion_travel_analytics'
)
AS 
SELECT 
    DATE(transaction_timestamp) as report_date,
    transaction_year,
    transaction_month,
    day_type,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN clicked THEN 1 ELSE 0 END) as total_clicks,
    SUM(CASE WHEN purchased THEN 1 ELSE 0 END) as total_purchases,
    ROUND(SUM(CASE WHEN purchased THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as conversion_rate_pct,
    ROUND(SUM(CASE WHEN purchased THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN clicked THEN 1 ELSE 0 END), 0), 2) as click_to_purchase_rate_pct,
    ROUND(SUM(CASE WHEN purchased THEN price_usd ELSE 0 END), 2) as total_revenue_usd,
    ROUND(AVG(CASE WHEN purchased THEN price_usd END), 2) as avg_transaction_value_usd,
    MIN(CASE WHEN purchased THEN price_usd END) as min_transaction_usd,
    MAX(CASE WHEN purchased THEN price_usd END) as max_transaction_usd,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT destination_id) as unique_destinations,
    current_timestamp() as _generated_at
FROM {catalog}.{target_schema}.silver_travel_purchases
GROUP BY DATE(transaction_timestamp), transaction_year, transaction_month, day_type
ORDER BY report_date
""")

spark.sql(f"ALTER TABLE {catalog}.{target_schema}.gold_daily_revenue_metrics ALTER COLUMN report_date COMMENT 'Report date (PK)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.gold_daily_revenue_metrics ALTER COLUMN conversion_rate_pct COMMENT 'Purchase conversion rate'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.gold_daily_revenue_metrics ALTER COLUMN total_revenue_usd COMMENT 'Total revenue in USD'")

count = spark.table(f"{catalog}.{target_schema}.gold_daily_revenue_metrics").count()
revenue = spark.sql(f"SELECT SUM(total_revenue_usd) FROM {catalog}.{target_schema}.gold_daily_revenue_metrics").collect()[0][0]
print(f"Created with {count:,} daily records (${revenue:,.2f} total revenue)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 2: Destination Performance

# COMMAND ----------

print("Creating gold_destination_performance...")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{target_schema}.gold_destination_performance
COMMENT 'Gold layer: Destination performance metrics'
TBLPROPERTIES (
    'layer' = 'gold',
    'business_domain' = 'marketing',
    'refresh_frequency' = 'daily',
    'pipeline' = 'medallion_travel_analytics'
)
AS 
SELECT 
    d.destination_id,
    d.destination_name,
    d.latitude,
    d.longitude,
    d.hemisphere,
    COUNT(*) as total_views,
    SUM(CASE WHEN t.clicked THEN 1 ELSE 0 END) as total_clicks,
    SUM(CASE WHEN t.purchased THEN 1 ELSE 0 END) as total_bookings,
    ROUND(SUM(CASE WHEN t.clicked THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as click_rate_pct,
    ROUND(SUM(CASE WHEN t.purchased THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN t.clicked THEN 1 ELSE 0 END), 0), 2) as booking_rate_pct,
    ROUND(SUM(CASE WHEN t.purchased THEN t.price_usd ELSE 0 END), 2) as total_revenue_usd,
    ROUND(AVG(CASE WHEN t.purchased THEN t.price_usd END), 2) as avg_booking_value_usd,
    COUNT(DISTINCT t.user_id) as unique_visitors,
    COUNT(DISTINCT CASE WHEN t.purchased THEN t.user_id END) as unique_buyers,
    MIN(t.transaction_timestamp) as first_transaction,
    MAX(t.transaction_timestamp) as last_transaction,
    RANK() OVER (ORDER BY SUM(CASE WHEN t.purchased THEN 1 ELSE 0 END) DESC) as booking_rank,
    RANK() OVER (ORDER BY SUM(CASE WHEN t.purchased THEN t.price_usd ELSE 0 END) DESC) as revenue_rank,
    current_timestamp() as _generated_at
FROM {catalog}.{target_schema}.silver_destinations d
LEFT JOIN {catalog}.{target_schema}.silver_travel_purchases t 
    ON d.destination_id = t.destination_id
GROUP BY d.destination_id, d.destination_name, d.latitude, d.longitude, d.hemisphere
ORDER BY total_bookings DESC
""")

spark.sql(f"ALTER TABLE {catalog}.{target_schema}.gold_destination_performance ALTER COLUMN destination_id COMMENT 'Destination ID (PK)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.gold_destination_performance ALTER COLUMN booking_rank COMMENT 'Rank by bookings (1=top)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.gold_destination_performance ALTER COLUMN revenue_rank COMMENT 'Rank by revenue (1=top)'")

count = spark.table(f"{catalog}.{target_schema}.gold_destination_performance").count()
print(f"Created with {count:,} destination records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 3: User Engagement

# COMMAND ----------

print("Creating gold_user_engagement...")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{target_schema}.gold_user_engagement
COMMENT 'Gold layer: User engagement and behavior metrics'
TBLPROPERTIES (
    'layer' = 'gold',
    'business_domain' = 'customer',
    'refresh_frequency' = 'daily',
    'pipeline' = 'medallion_travel_analytics'
)
AS 
SELECT 
    u.user_id,
    u.purchase_frequency_segment,
    u.price_segment,
    u.avg_price_7day as historical_avg_price,
    u.purchases_6month as historical_purchases,
    u.user_latitude,
    u.user_longitude,
    COUNT(t.transaction_id) as total_interactions,
    SUM(CASE WHEN t.clicked THEN 1 ELSE 0 END) as total_clicks,
    SUM(CASE WHEN t.purchased THEN 1 ELSE 0 END) as total_purchases,
    ROUND(SUM(CASE WHEN t.purchased THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as conversion_rate_pct,
    ROUND(SUM(CASE WHEN t.purchased THEN t.price_usd ELSE 0 END), 2) as total_spend_usd,
    ROUND(AVG(CASE WHEN t.purchased THEN t.price_usd END), 2) as avg_purchase_usd,
    COUNT(DISTINCT t.destination_id) as destinations_viewed,
    COUNT(DISTINCT CASE WHEN t.purchased THEN t.destination_id END) as destinations_booked,
    MIN(t.transaction_timestamp) as first_activity,
    MAX(t.transaction_timestamp) as last_activity,
    DATEDIFF(MAX(t.transaction_timestamp), MIN(t.transaction_timestamp)) as engagement_days,
    CASE 
        WHEN SUM(CASE WHEN t.purchased THEN t.price_usd ELSE 0 END) >= 5000 THEN 'platinum'
        WHEN SUM(CASE WHEN t.purchased THEN t.price_usd ELSE 0 END) >= 2000 THEN 'gold'
        WHEN SUM(CASE WHEN t.purchased THEN t.price_usd ELSE 0 END) >= 500 THEN 'silver'
        ELSE 'bronze'
    END as customer_tier,
    current_timestamp() as _generated_at
FROM {catalog}.{target_schema}.silver_users u
LEFT JOIN {catalog}.{target_schema}.silver_travel_purchases t 
    ON u.user_id = t.user_id
GROUP BY u.user_id, u.purchase_frequency_segment, u.price_segment, 
         u.avg_price_7day, u.purchases_6month, u.user_latitude, u.user_longitude
""")

spark.sql(f"ALTER TABLE {catalog}.{target_schema}.gold_user_engagement ALTER COLUMN user_id COMMENT 'User ID (PK)'")
spark.sql(f"ALTER TABLE {catalog}.{target_schema}.gold_user_engagement ALTER COLUMN customer_tier COMMENT 'Value tier: platinum/gold/silver/bronze'")

count = spark.table(f"{catalog}.{target_schema}.gold_user_engagement").count()
platinum = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{target_schema}.gold_user_engagement WHERE customer_tier = 'platinum'").collect()[0][0]
print(f"Created with {count:,} user records ({platinum} platinum)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 4: Monthly Summary

# COMMAND ----------

print("Creating gold_monthly_summary...")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{target_schema}.gold_monthly_summary
COMMENT 'Gold layer: Monthly executive summary'
TBLPROPERTIES (
    'layer' = 'gold',
    'business_domain' = 'executive',
    'refresh_frequency' = 'monthly',
    'pipeline' = 'medallion_travel_analytics'
)
AS 
SELECT 
    transaction_year,
    transaction_month,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN purchased THEN 1 ELSE 0 END) as total_bookings,
    ROUND(SUM(CASE WHEN purchased THEN price_usd ELSE 0 END), 2) as monthly_revenue_usd,
    ROUND(AVG(CASE WHEN purchased THEN price_usd END), 2) as avg_booking_value_usd,
    COUNT(DISTINCT user_id) as active_users,
    COUNT(DISTINCT destination_id) as destinations_in_demand,
    ROUND(SUM(CASE WHEN purchased THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) as monthly_conversion_pct,
    LAG(ROUND(SUM(CASE WHEN purchased THEN price_usd ELSE 0 END), 2)) OVER (ORDER BY transaction_year, transaction_month) as prev_month_revenue,
    current_timestamp() as _generated_at
FROM {catalog}.{target_schema}.silver_travel_purchases
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year, transaction_month
""")

count = spark.table(f"{catalog}.{target_schema}.gold_monthly_summary").count()
print(f"Created with {count:,} monthly records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("GOLD LAYER COMPLETE")
print("=" * 60)
print(f"\nTables created in {catalog}.{target_schema}:")
print("  - gold_daily_revenue_metrics")
print("  - gold_destination_performance")
print("  - gold_user_engagement")
print("  - gold_monthly_summary")
