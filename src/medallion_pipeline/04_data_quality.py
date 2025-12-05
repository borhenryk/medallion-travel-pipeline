# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Validation
# MAGIC 
# MAGIC This notebook validates data quality across all medallion layers:
# MAGIC - Record counts and completeness
# MAGIC - Data freshness
# MAGIC - Referential integrity
# MAGIC - Business rule validation

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
# MAGIC ## Data Quality Checks

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

dq_results = []

# Check 1: Record Counts
print("Checking record counts...")

tables = [
    ("bronze_travel_purchases", "bronze"),
    ("bronze_user_features", "bronze"),
    ("bronze_destinations", "bronze"),
    ("silver_travel_purchases", "silver"),
    ("silver_users", "silver"),
    ("silver_destinations", "silver"),
    ("gold_daily_revenue_metrics", "gold"),
    ("gold_destination_performance", "gold"),
    ("gold_user_engagement", "gold"),
    ("gold_monthly_summary", "gold")
]

for table_name, layer in tables:
    try:
        count = spark.table(f"{catalog}.{target_schema}.{table_name}").count()
        status = "PASS" if count > 0 else "FAIL"
        dq_results.append({
            "check": "record_count",
            "table": table_name,
            "layer": layer,
            "expected": "> 0",
            "actual": count,
            "status": status
        })
        print(f"  {status}: {table_name} = {count:,} records")
    except Exception as e:
        dq_results.append({
            "check": "record_count",
            "table": table_name,
            "layer": layer,
            "expected": "> 0",
            "actual": str(e),
            "status": "FAIL"
        })
        print(f"  FAIL: {table_name} - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referential Integrity Checks

# COMMAND ----------

print("\nChecking referential integrity...")

# Check: All user_ids in silver_travel_purchases exist in silver_users
orphan_users = spark.sql(f"""
    SELECT COUNT(DISTINCT t.user_id) as orphan_count
    FROM {catalog}.{target_schema}.silver_travel_purchases t
    LEFT JOIN {catalog}.{target_schema}.silver_users u ON t.user_id = u.user_id
    WHERE u.user_id IS NULL
""").collect()[0][0]

status = "PASS" if orphan_users == 0 else "WARN"
dq_results.append({
    "check": "referential_integrity",
    "table": "silver_travel_purchases -> silver_users",
    "layer": "silver",
    "expected": "0 orphans",
    "actual": orphan_users,
    "status": status
})
print(f"  {status}: Orphan user_ids = {orphan_users}")

# Check: All destination_ids in silver_travel_purchases exist in silver_destinations
orphan_dests = spark.sql(f"""
    SELECT COUNT(DISTINCT t.destination_id) as orphan_count
    FROM {catalog}.{target_schema}.silver_travel_purchases t
    LEFT JOIN {catalog}.{target_schema}.silver_destinations d ON t.destination_id = d.destination_id
    WHERE d.destination_id IS NULL
""").collect()[0][0]

status = "PASS" if orphan_dests == 0 else "WARN"
dq_results.append({
    "check": "referential_integrity",
    "table": "silver_travel_purchases -> silver_destinations",
    "layer": "silver",
    "expected": "0 orphans",
    "actual": orphan_dests,
    "status": status
})
print(f"  {status}: Orphan destination_ids = {orphan_dests}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Freshness Check

# COMMAND ----------

print("\nChecking data freshness...")

freshness_tables = [
    ("bronze_travel_purchases", "_ingested_at"),
    ("silver_travel_purchases", "_processed_at"),
    ("gold_daily_revenue_metrics", "_generated_at")
]

for table_name, ts_col in freshness_tables:
    try:
        max_ts = spark.sql(f"""
            SELECT MAX({ts_col}) as max_ts 
            FROM {catalog}.{target_schema}.{table_name}
        """).collect()[0][0]
        
        hours_old = (datetime.now() - max_ts).total_seconds() / 3600 if max_ts else float('inf')
        status = "PASS" if hours_old < 24 else "WARN"
        
        dq_results.append({
            "check": "data_freshness",
            "table": table_name,
            "layer": table_name.split("_")[0],
            "expected": "< 24 hours",
            "actual": f"{hours_old:.1f} hours",
            "status": status
        })
        print(f"  {status}: {table_name} = {hours_old:.1f} hours old")
    except Exception as e:
        print(f"  FAIL: {table_name} - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Rule Validation

# COMMAND ----------

print("\nChecking business rules...")

# Check: No negative prices in silver
neg_prices = spark.sql(f"""
    SELECT COUNT(*) as count
    FROM {catalog}.{target_schema}.silver_travel_purchases
    WHERE price_usd < 0
""").collect()[0][0]

status = "PASS" if neg_prices == 0 else "FAIL"
dq_results.append({
    "check": "business_rule",
    "table": "silver_travel_purchases",
    "layer": "silver",
    "expected": "0 negative prices",
    "actual": neg_prices,
    "status": status
})
print(f"  {status}: Negative prices = {neg_prices}")

# Check: Conversion rate is reasonable (0-100%)
invalid_conv = spark.sql(f"""
    SELECT COUNT(*) as count
    FROM {catalog}.{target_schema}.gold_daily_revenue_metrics
    WHERE conversion_rate_pct < 0 OR conversion_rate_pct > 100
""").collect()[0][0]

status = "PASS" if invalid_conv == 0 else "FAIL"
dq_results.append({
    "check": "business_rule",
    "table": "gold_daily_revenue_metrics",
    "layer": "gold",
    "expected": "0 invalid rates",
    "actual": invalid_conv,
    "status": status
})
print(f"  {status}: Invalid conversion rates = {invalid_conv}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

print("\n" + "=" * 60)
print("DATA QUALITY SUMMARY")
print("=" * 60)

df_results = spark.createDataFrame(dq_results)

pass_count = df_results.filter(F.col("status") == "PASS").count()
warn_count = df_results.filter(F.col("status") == "WARN").count()
fail_count = df_results.filter(F.col("status") == "FAIL").count()
total_count = df_results.count()

print(f"\nPASS: {pass_count}/{total_count}")
print(f"WARN: {warn_count}/{total_count}")
print(f"FAIL: {fail_count}/{total_count}")

overall_status = "PASS" if fail_count == 0 else "FAIL"
print(f"\nOverall Status: {overall_status}")

# Store results for audit
df_results.write.mode("overwrite").saveAsTable(f"{catalog}.{target_schema}.dq_audit_log")
print(f"\nResults saved to {catalog}.{target_schema}.dq_audit_log")

# COMMAND ----------

# Fail the job if critical checks fail
if fail_count > 0:
    raise Exception(f"Data quality checks failed: {fail_count} failures detected")
else:
    print("\nAll data quality checks passed!")
