# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Notebook Configuration
# =============================================================================
# Notebook: 03 - Aggregation Layer
# Purpose:  Create aggregate table showing profit by Year, Product Category,
#           Sub-Category, and Customer.
# Input:    hive_metastore.pei.enriched_orders
# Output:   hive_metastore.pei.agg_profit_summary
# =============================================================================

from pyspark.sql import functions as F

DATABASE = "hive_metastore"
SCHEMA = "pei"
spark.sql(f"USE {DATABASE}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Import Utils
# MAGIC %run /Users/lakrathe@publicisgroupe.net/PEI/utils/validations.py

# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ## 03 — Aggregation Layer
# MAGIC
# MAGIC **Task 4:** Create an aggregate table summarising profit by:
# MAGIC - Year (extracted from `order_date`)
# MAGIC - Product Category
# MAGIC - Sub-Category
# MAGIC - Customer Name
# MAGIC
# MAGIC Source: `hive_metastore.pei.enriched_orders`  
# MAGIC Output: `hive_metastore.pei.agg_profit_summary`

# COMMAND ----------

# DBTITLE 1,Load Enriched Orders
enriched_orders = spark.table(f"{SCHEMA}.enriched_orders")
print(f"enriched_orders: {enriched_orders.count()} rows, {len(enriched_orders.columns)} columns")
enriched_orders.printSchema()

# COMMAND ----------

# DBTITLE 1,Build Aggregation
# Extract year from order_date and aggregate profit
# No NULL filter needed: category is derived from product_id prefix for all rows
agg_profit_summary = (
    enriched_orders
    .withColumn("year", F.year(F.col("order_date")))
    .groupBy("year", "category", "sub_category", "customer_name")
    .agg(
        F.round(F.sum("profit"), 2).alias("total_profit"),
        F.count("order_id").alias("order_count"),
        F.round(F.sum("price"), 2).alias("total_revenue")
    )
    .orderBy("year", "category", "sub_category", "customer_name")
)

print(f"Aggregation rows: {agg_profit_summary.count()}")
print(f"Distinct categories: {sorted([r[0] for r in agg_profit_summary.select('category').distinct().collect()])}")
display(agg_profit_summary.limit(10))

# COMMAND ----------

# DBTITLE 1,Validate Aggregation
# Verify profit reconciliation: ALL enriched orders profit = aggregated profit
# No filtering needed — every order now has a valid category (derived from product_id)
assert_profit_reconciliation(enriched_orders, agg_profit_summary)

# Verify required columns
assert_required_columns(agg_profit_summary, [
    "year", "category", "sub_category", "customer_name", "total_profit"
])

# Verify only real categories exist (no NULLs, no invented values)
categories = sorted([r[0] for r in agg_profit_summary.select("category").distinct().collect()])
assert categories == ["Furniture", "Office Supplies", "Technology"], f"Unexpected categories: {categories}"

print(f"Categories: {categories}")
print("All aggregation validations passed.")

# COMMAND ----------

# DBTITLE 1,Write Aggregation Table
try:
    agg_profit_summary.write.mode("overwrite").saveAsTable(f"{SCHEMA}.agg_profit_summary")
    print(f"  \u2713 {SCHEMA}.agg_profit_summary written successfully")
    print(f"  Row count: {spark.table(f'{SCHEMA}.agg_profit_summary').count()}")
except Exception as e:
    print(f"  \u2717 Failed to write {SCHEMA}.agg_profit_summary: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,Verify Written Table
# Final verification
display(
    spark.table(f"{SCHEMA}.agg_profit_summary")
    .orderBy(F.col("total_profit").desc())
    .limit(10)
)