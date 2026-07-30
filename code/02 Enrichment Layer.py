# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Notebook Configuration
# =============================================================================
# Notebook: 02 - Enrichment Layer
# Purpose:  Create enriched versions of raw tables with business-level cleaning
#           and a denormalised enriched_orders table joining all three sources.
# Inputs:   hive_metastore.pei.raw_customers
#           hive_metastore.pei.raw_products
#           hive_metastore.pei.raw_orders
# Outputs:  hive_metastore.pei.enriched_customers
#           hive_metastore.pei.enriched_products
#           hive_metastore.pei.enriched_orders
# =============================================================================

from pyspark.sql import functions as F

DATABASE = "hive_metastore"
SCHEMA = "pei"
spark.sql(f"USE {DATABASE}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Import Utils
# MAGIC %run /Users/lakrathe@publicisgroupe.net/PEI/utils/transformations.py

# COMMAND ----------

# MAGIC %run /Users/lakrathe@publicisgroupe.net/PEI/utils/validations.py

# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ## 02 — Enrichment Layer
# MAGIC
# MAGIC **Task 2:** Create enriched tables for customers and products with business-level cleaning.  
# MAGIC **Task 3:** Create an enriched orders table joining all three sources with:
# MAGIC - Full order information
# MAGIC - Profit rounded to 2 decimal places
# MAGIC - Customer name and country
# MAGIC - Product category and sub-category
# MAGIC
# MAGIC **Enrichment logic applied:**
# MAGIC - Customer names: remove special characters (`@#$`), embedded digits, and excessive whitespace
# MAGIC - Product price: cast from string to numeric
# MAGIC - Order/Ship dates: parsed from `d/M/yyyy` string to proper `DateType`
# MAGIC - Addresses: normalise embedded newlines to comma-separated format

# COMMAND ----------

# DBTITLE 1,Load Raw Tables
# Read the raw layer tables
raw_customers = spark.table(f"{SCHEMA}.raw_customers")
raw_products = spark.table(f"{SCHEMA}.raw_products")
raw_orders = spark.table(f"{SCHEMA}.raw_orders")

print(f"raw_customers: {raw_customers.count()} rows")
print(f"raw_products:  {raw_products.count()} rows")
print(f"raw_orders:    {raw_orders.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Enriched Customers
# MAGIC Cleaning applied:
# MAGIC - Remove special characters and digits from customer names
# MAGIC - Collapse multiple spaces into single space
# MAGIC - Normalise address field (replace newlines with comma-space)

# COMMAND ----------

# DBTITLE 1,Enrich Customers
# Apply enrichment using shared utility functions
enriched_customers = clean_customer_name(raw_customers)
enriched_customers = normalize_address(enriched_customers)

print(f"Enriched customers: {enriched_customers.count()} rows")
display(enriched_customers.limit(150))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Enriched Products
# MAGIC Cleaning applied:
# MAGIC - Cast `price_per_product` from string to double for numeric operations

# COMMAND ----------

# DBTITLE 1,Enrich Products
# Cast price from string to double using shared utility
enriched_products = cast_price_column(raw_products)

print(f"Enriched products: {enriched_products.count()} rows")
null_prices = enriched_products.filter(F.col("price_per_product").isNull()).count()
print(f"Rows with unparseable price (set to NULL): {null_prices}")
display(enriched_products.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Enriched Orders
# MAGIC Denormalised table joining orders with customer and product details.
# MAGIC
# MAGIC Columns included:
# MAGIC - All order fields (with dates parsed to DateType)
# MAGIC - `profit` rounded to 2 decimal places
# MAGIC - `customer_name`, `country` from enriched customers
# MAGIC - `category`, `sub_category` from enriched products

# COMMAND ----------

# DBTITLE 1,Enrich Orders
# Parse date strings and round profit using shared utilities
orders_with_dates = parse_date_column(raw_orders, "order_date")
orders_with_dates = parse_date_column(orders_with_dates, "ship_date")
orders_with_dates = round_profit(orders_with_dates)

# Join with enriched customers to get name and country
# Join with enriched products to get category and sub-category
# LEFT JOIN preserves all orders; 44 product_ids in orders don't exist in Products.csv
enriched_orders = (
    orders_with_dates
    .join(
        enriched_customers.select("customer_id", "customer_name", "country"),
        on="customer_id",
        how="left"
    )
    .join(
        enriched_products.select("product_id", "category", "sub_category"),
        on="product_id",
        how="left"
    )
)

# Derive category/sub_category from product_id prefix for unmatched rows
# 44 product_ids in orders are not in Products.csv but their category is
# encoded in the product_id format: CAT-SUB-NUMBER (verified 1:1 mapping)
enriched_orders = derive_category_from_product_id(enriched_orders)

# Verify: zero NULLs remaining
null_cats = enriched_orders.filter(F.col("category").isNull()).count()
assert null_cats == 0, f"Still have {null_cats} NULL categories!"

print(f"Enriched orders: {enriched_orders.count()} rows, {len(enriched_orders.columns)} columns")
print(f"NULL categories: {null_cats} (derived from product_id prefix for unmatched products)")
print(f"Distinct categories: {sorted([r[0] for r in enriched_orders.select('category').distinct().collect()])}")
enriched_orders.printSchema()

# COMMAND ----------

# DBTITLE 1,Preview Enriched Orders
display(enriched_orders.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Validation

# COMMAND ----------

# DBTITLE 1,Validate Enriched Tables
# Ensure no data loss during enrichment (using shared validation util)
assert_row_count_match(enriched_customers, raw_customers, "customers")
assert_row_count_match(enriched_products, raw_products, "products")
assert_row_count_match(enriched_orders, raw_orders, "orders after joins")

# Verify enriched_orders has the required columns (using shared validation util)
assert_required_columns(enriched_orders, [
    "order_id", "customer_id", "product_id", "profit",
    "customer_name", "country", "category", "sub_category"
])

# Check that date parsing succeeded (no nulls introduced)
null_dates = enriched_orders.filter(
    F.col("order_date").isNull() | F.col("ship_date").isNull()
).count()
print(f"Null dates after parsing: {null_dates}")

print("All enrichment validations passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Write Enriched Tables

# COMMAND ----------

# DBTITLE 1,Write Enriched Tables
tables_to_write = {
    "enriched_customers": enriched_customers,
    "enriched_products": enriched_products,
    "enriched_orders": enriched_orders,
}

for table_name, df in tables_to_write.items():
    try:
        df.write.mode("overwrite").saveAsTable(f"{SCHEMA}.{table_name}")
        print(f"  \u2713 {SCHEMA}.{table_name} written successfully")
    except Exception as e:
        print(f"  \u2717 Failed to write {SCHEMA}.{table_name}: {e}")
        raise

# COMMAND ----------

# DBTITLE 1,Verify Enriched Tables
# Final verification - read back and confirm
for table in ["enriched_customers", "enriched_products", "enriched_orders"]:
    count = spark.table(f"{SCHEMA}.{table}").count()
    print(f"{table}: {count} rows")

# Show sample enriched order with all joined columns
display(
    spark.table(f"{SCHEMA}.enriched_orders")
    .select("order_id", "order_date", "customer_name", "country", 
            "category", "sub_category", "profit")
    .limit(5)
)