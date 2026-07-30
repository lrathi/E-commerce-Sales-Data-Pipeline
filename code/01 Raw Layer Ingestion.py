# Databricks notebook source
# DBTITLE 1,Notebook Configuration
# =============================================================================
# Notebook: 01 - Raw Layer Ingestion
# Purpose:  Read source datasets (Customer, Products, Orders), apply basic
#           standardisation (column naming, trimming, deduplication), and
#           persist as Delta tables in the raw layer.
# Inputs:   CSV and JSON files from /PEI/datasets/
# Outputs:  hive_metastore.pei.raw_customers
#           hive_metastore.pei.raw_products
#           hive_metastore.pei.raw_orders
# =============================================================================

# --- Configuration ---
DATABASE = "hive_metastore"
SCHEMA = "pei"

# Source file paths (workspace files require the file: prefix on serverless)
BASE_PATH = "file:/Workspace/Users/lakrathe@publicisgroupe.net/PEI/datasets"
CUSTOMER_FILE = f"{BASE_PATH}/Customer.csv"
PRODUCTS_FILE = f"{BASE_PATH}/Products.csv"
ORDERS_FILE = f"{BASE_PATH}/Orders.json"

# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ## 01 — Raw Layer Ingestion
# MAGIC
# MAGIC **Objective:** Ingest source datasets into the raw layer as Delta tables with minimal transformation.
# MAGIC
# MAGIC **Transformations applied:**
# MAGIC - Column name standardisation (lowercase, underscores)
# MAGIC - Leading/trailing whitespace trimming on string fields
# MAGIC - Deduplication on business keys
# MAGIC
# MAGIC **Data Quality Notes (from profiling):**
# MAGIC - `Customer.csv` is Latin-1 encoded with multi-line address fields
# MAGIC - `Orders.json` is a pretty-printed JSON array (requires `multiLine=True`)
# MAGIC - Customer names contain noise characters (special chars, digits) — handled in the enrichment layer

# COMMAND ----------

# DBTITLE 1,Import Utilities
# MAGIC %run /Users/lakrathe@publicisgroupe.net/PEI/utils/transformations.py

# COMMAND ----------

# DBTITLE 1,Import Utilities
# MAGIC %run /Users/lakrathe@publicisgroupe.net/PEI/utils/validations.py

# COMMAND ----------

# DBTITLE 1,Set Active Schema
spark.sql(f"USE {DATABASE}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Ingest Customers

# COMMAND ----------

# DBTITLE 1,Read and Standardise Customers
# Customer.csv uses Latin-1 encoding and has multi-line address values (quoted fields with newlines)
raw_customers = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("encoding", "latin1")
    .option("multiLine", True)
    .csv(CUSTOMER_FILE)
)

# Apply standard cleaning
raw_customers = standardize_column_names(raw_customers)
raw_customers = trim_string_columns(raw_customers)
raw_customers = remove_duplicate_keys(raw_customers, "customer_id")

print(f"Customers: {raw_customers.count()} rows, {len(raw_customers.columns)} columns")
raw_customers.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Ingest Products

# COMMAND ----------

# DBTITLE 1,Read and Standardise Products
raw_products = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(PRODUCTS_FILE)
)

# Apply standard cleaning
raw_products = standardize_column_names(raw_products)
raw_products = trim_string_columns(raw_products)
raw_products = remove_duplicate_keys(raw_products, "product_id")

print(f"Products: {raw_products.count()} rows, {len(raw_products.columns)} columns")
raw_products.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Ingest Orders

# COMMAND ----------

# DBTITLE 1,Read and Standardise Orders
# Orders.json is a pretty-printed JSON array, so multiLine is required
raw_orders = (
    spark.read
    .option("inferSchema", True)
    .option("multiLine", True)
    .json(ORDERS_FILE)
)

# Apply standard cleaning
raw_orders = standardize_column_names(raw_orders)
raw_orders = trim_string_columns(raw_orders)
raw_orders = remove_duplicate_keys(raw_orders, "row_id")

print(f"Orders: {raw_orders.count()} rows, {len(raw_orders.columns)} columns")
raw_orders.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Data Quality Checks
# MAGIC Validate each DataFrame before persisting to Delta.

# COMMAND ----------

# DBTITLE 1,Validate Raw DataFrames
# Validate all three DataFrames meet minimum quality criteria
assert_not_empty(raw_customers)
assert_not_empty(raw_products)
assert_not_empty(raw_orders)

assert_unique_key(raw_customers, "customer_id")
assert_unique_key(raw_products, "product_id")
assert_unique_key(raw_orders, "row_id")

# Confirm expected columns exist
assert_required_columns(raw_customers, ["customer_id", "customer_name", "segment", "country", "region"])
assert_required_columns(raw_products, ["product_id", "category", "sub_category", "product_name"])
assert_required_columns(raw_orders, ["order_id", "customer_id", "product_id", "quantity", "price", "profit"])

print("All validations passed.")

# COMMAND ----------

# DBTITLE 1,Null Summary
# Quick null profile for each dataset
print("--- Customers Null Counts ---")
display(null_summary(raw_customers))

print("--- Products Null Counts ---")
display(null_summary(raw_products))

print("--- Orders Null Counts ---")
display(null_summary(raw_orders))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Write Delta Tables
# MAGIC Persist cleaned DataFrames to the raw layer using `overwrite` mode for idempotency.

# COMMAND ----------

# DBTITLE 1,Write Raw Tables
# Write each DataFrame as a managed Delta table
tables_to_write = {
    "raw_customers": raw_customers,
    "raw_products": raw_products,
    "raw_orders": raw_orders,
}

for table_name, df in tables_to_write.items():
    try:
        df.write.mode("overwrite").saveAsTable(f"{SCHEMA}.{table_name}")
        print(f"  ✓ {SCHEMA}.{table_name} written successfully")
    except Exception as e:
        print(f"  ✗ Failed to write {SCHEMA}.{table_name}: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Verification
# MAGIC Confirm tables are readable and row counts match expectations.

# COMMAND ----------

# DBTITLE 1,Verify Written Tables
# Read back from Delta and verify counts match
customers_count = spark.table(f"{SCHEMA}.raw_customers").count()
products_count = spark.table(f"{SCHEMA}.raw_products").count()
orders_count = spark.table(f"{SCHEMA}.raw_orders").count()

print(f"raw_customers: {customers_count} rows")
print(f"raw_products:  {products_count} rows")
print(f"raw_orders:    {orders_count} rows")

# Quick preview
display(spark.table(f"{SCHEMA}.raw_orders").limit(5))