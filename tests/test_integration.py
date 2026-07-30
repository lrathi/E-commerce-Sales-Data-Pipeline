"""
Integration tests — verify the end-to-end pipeline output.

These tests read from the actual Delta tables written by the pipeline
notebooks and validate correctness across layers.

Prerequisite: Notebooks 01, 02, 03 must have been run successfully.
"""

import pytest
from pyspark.sql import SparkSession, functions as F


SCHEMA = "pei"


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.getOrCreate()


# =============================================================================
# Raw Layer Integration
# =============================================================================

class TestRawLayerIntegration:
    """Verify raw tables exist and have expected characteristics."""

    def test_raw_customers_exists_and_not_empty(self, spark):
        df = spark.table(f"{SCHEMA}.raw_customers")
        assert df.count() > 0

    def test_raw_products_exists_and_not_empty(self, spark):
        df = spark.table(f"{SCHEMA}.raw_products")
        assert df.count() > 0

    def test_raw_orders_exists_and_not_empty(self, spark):
        df = spark.table(f"{SCHEMA}.raw_orders")
        assert df.count() > 0

    def test_raw_customers_unique_keys(self, spark):
        df = spark.table(f"{SCHEMA}.raw_customers")
        total = df.count()
        distinct = df.select("customer_id").distinct().count()
        assert total == distinct

    def test_raw_orders_unique_keys(self, spark):
        df = spark.table(f"{SCHEMA}.raw_orders")
        total = df.count()
        distinct = df.select("row_id").distinct().count()
        assert total == distinct

    def test_raw_column_names_standardised(self, spark):
        """All columns should be lowercase with underscores."""
        for table in ["raw_customers", "raw_products", "raw_orders"]:
            df = spark.table(f"{SCHEMA}.{table}")
            for col in df.columns:
                assert col == col.lower(), f"{table}.{col} not lowercase"
                assert " " not in col, f"{table}.{col} has spaces"


# =============================================================================
# Enrichment Layer Integration
# =============================================================================

class TestEnrichmentLayerIntegration:
    """Verify enriched tables have correct joins and transformations."""

    def test_enriched_orders_row_count_matches_raw(self, spark):
        """No rows lost during enrichment joins."""
        raw_count = spark.table(f"{SCHEMA}.raw_orders").count()
        enriched_count = spark.table(f"{SCHEMA}.enriched_orders").count()
        assert enriched_count == raw_count

    def test_enriched_orders_has_customer_columns(self, spark):
        df = spark.table(f"{SCHEMA}.enriched_orders")
        assert "customer_name" in df.columns
        assert "country" in df.columns

    def test_enriched_orders_has_product_columns(self, spark):
        df = spark.table(f"{SCHEMA}.enriched_orders")
        assert "category" in df.columns
        assert "sub_category" in df.columns

    def test_enriched_orders_profit_rounded(self, spark):
        """Profit should have at most 2 decimal places."""
        df = spark.table(f"{SCHEMA}.enriched_orders")
        # Check that rounding doesn't change values (already rounded)
        mismatch = df.filter(
            F.col("profit") != F.round(F.col("profit"), 2)
        ).count()
        assert mismatch == 0

    def test_enriched_orders_dates_are_date_type(self, spark):
        """order_date and ship_date should be DateType."""
        df = spark.table(f"{SCHEMA}.enriched_orders")
        schema_dict = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        assert schema_dict["order_date"] == "date"
        assert schema_dict["ship_date"] == "date"

    def test_enriched_customers_names_cleaned(self, spark):
        """No special chars or digits should remain in customer names."""
        import re
        df = spark.table(f"{SCHEMA}.enriched_customers")
        # Sample check: collect a subset and verify
        names = [row["customer_name"] for row in df.limit(100).collect() if row["customer_name"]]
        for name in names:
            assert not re.search(r"[0-9@#$%^&*]", name), f"Dirty name found: {name}"


# =============================================================================
# Aggregation Layer Integration
# =============================================================================

class TestAggregationLayerIntegration:
    """Verify aggregate table is correct."""

    def test_agg_table_exists_and_not_empty(self, spark):
        df = spark.table(f"{SCHEMA}.agg_profit_summary")
        assert df.count() > 0

    def test_agg_profit_total_matches_enriched(self, spark):
        """Sum of aggregated profit must equal sum of ALL enriched orders profit."""
        enriched_total = (
            spark.table(f"{SCHEMA}.enriched_orders")
            .agg(F.round(F.sum("profit"), 2))
            .collect()[0][0]
        )
        agg_total = (
            spark.table(f"{SCHEMA}.agg_profit_summary")
            .agg(F.round(F.sum("total_profit"), 2))
            .collect()[0][0]
        )
        assert enriched_total == agg_total

    def test_agg_categories_are_valid(self, spark):
        """Only real product categories (no NULLs, no invented values like 'Unknown')."""
        df = spark.table(f"{SCHEMA}.agg_profit_summary")
        categories = sorted([r[0] for r in df.select("category").distinct().collect()])
        assert categories == ["Furniture", "Office Supplies", "Technology"]

    def test_enriched_orders_no_null_categories(self, spark):
        """Every order must have a category (derived from product_id if not in products table)."""
        df = spark.table(f"{SCHEMA}.enriched_orders")
        null_cats = df.filter(F.col("category").isNull()).count()
        assert null_cats == 0, f"Found {null_cats} orders with NULL category"

    def test_agg_has_required_dimensions(self, spark):
        df = spark.table(f"{SCHEMA}.agg_profit_summary")
        required = ["year", "category", "sub_category", "customer_name", "total_profit"]
        for col in required:
            assert col in df.columns

    def test_agg_years_are_valid(self, spark):
        """Years should be between 2014 and 2017 based on source data."""
        df = spark.table(f"{SCHEMA}.agg_profit_summary")
        years = [row["year"] for row in df.select("year").distinct().collect()]
        for y in years:
            assert 2014 <= y <= 2017, f"Unexpected year: {y}"
