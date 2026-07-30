# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup
# MAGIC %sql
# MAGIC -- =============================================================================
# MAGIC -- Notebook: 04 - SQL Aggregates
# MAGIC -- Purpose:  Task 5 — SQL queries for profit analysis from enriched_orders
# MAGIC -- Source:   hive_metastore.pei.enriched_orders
# MAGIC -- =============================================================================
# MAGIC
# MAGIC USE hive_metastore.pei

# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ## 04 — SQL Aggregates
# MAGIC
# MAGIC **Task 5:** SQL-based profit analysis queries:
# MAGIC 1. Total profit by Year
# MAGIC 2. Total profit by Year + Category
# MAGIC 3. Total profit by Customer
# MAGIC 4. Total profit by Customer + Year
# MAGIC
# MAGIC All queries read from `hive_metastore.pei.enriched_orders`.

# COMMAND ----------

# DBTITLE 1,Query 1: Profit by Year
# MAGIC %sql
# MAGIC -- Total profit aggregated by year
# MAGIC SELECT
# MAGIC     YEAR(order_date) AS year,
# MAGIC     ROUND(SUM(profit), 2) AS total_profit,
# MAGIC     COUNT(*) AS order_count
# MAGIC FROM pei.enriched_orders
# MAGIC GROUP BY YEAR(order_date)
# MAGIC ORDER BY year

# COMMAND ----------

# DBTITLE 1,Query 2: Profit by Year and Category
# MAGIC %sql
# MAGIC -- Total profit by year and product category
# MAGIC SELECT
# MAGIC     YEAR(order_date) AS year,
# MAGIC     category,
# MAGIC     ROUND(SUM(profit), 2) AS total_profit,
# MAGIC     COUNT(*) AS order_count
# MAGIC FROM pei.enriched_orders
# MAGIC GROUP BY YEAR(order_date), category
# MAGIC ORDER BY year, category

# COMMAND ----------

# DBTITLE 1,Query 3: Profit by Customer
# MAGIC %sql
# MAGIC -- Total profit by customer (all-time)
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     ROUND(SUM(profit), 2) AS total_profit,
# MAGIC     COUNT(*) AS order_count
# MAGIC FROM pei.enriched_orders
# MAGIC GROUP BY customer_id, customer_name
# MAGIC ORDER BY total_profit DESC

# COMMAND ----------

# DBTITLE 1,Query 4: Profit by Customer and Year
# MAGIC %sql
# MAGIC -- Total profit by customer and year
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     YEAR(order_date) AS year,
# MAGIC     ROUND(SUM(profit), 2) AS total_profit,
# MAGIC     COUNT(*) AS order_count
# MAGIC FROM pei.enriched_orders
# MAGIC GROUP BY customer_id, customer_name, YEAR(order_date)
# MAGIC ORDER BY customer_name, year