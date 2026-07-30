"""
Tests for aggregation layer logic.

Covers: year extraction, groupBy aggregation, profit summation,
        column presence, and edge cases.
"""

import pytest
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, LongType
)


@pytest.fixture
def enriched_orders_df(spark):
    """Simulated enriched_orders table for aggregation tests."""
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("order_date", DateType()),
        StructField("customer_id", StringType()),
        StructField("customer_name", StringType()),
        StructField("product_id", StringType()),
        StructField("category", StringType()),
        StructField("sub_category", StringType()),
        StructField("country", StringType()),
        StructField("profit", DoubleType()),
        StructField("price", DoubleType()),
        StructField("quantity", LongType()),
    ])
    data = [
        ("O-001", date(2016, 8, 21), "C-001", "Alice", "P-001",
         "Furniture", "Chairs", "US", 100.50, 500.0, 5),
        ("O-002", date(2016, 9, 15), "C-001", "Alice", "P-002",
         "Technology", "Phones", "US", 50.25, 300.0, 2),
        ("O-003", date(2017, 3, 10), "C-002", "Bob", "P-001",
         "Furniture", "Chairs", "US", -20.00, 150.0, 1),
        ("O-004", date(2017, 6, 5), "C-001", "Alice", "P-003",
         "Office Supplies", "Paper", "US", 10.75, 50.0, 10),
        ("O-005", date(2017, 12, 1), "C-002", "Bob", "P-002",
         "Technology", "Phones", "US", 200.00, 800.0, 3),
    ]
    return spark.createDataFrame(data, schema)


def build_aggregation(df):
    """Replicate the aggregation logic from notebook 03."""
    return (
        df
        .withColumn("year", F.year(F.col("order_date")))
        .groupBy("year", "category", "sub_category", "customer_name")
        .agg(
            F.round(F.sum("profit"), 2).alias("total_profit"),
            F.count("order_id").alias("order_count"),
            F.round(F.sum("price"), 2).alias("total_revenue")
        )
    )


# =============================================================================
# Aggregation Tests
# =============================================================================

class TestAggregation:
    """Tests for the profit aggregation logic."""

    def test_year_extraction(self, enriched_orders_df):
        """Year should be correctly extracted from order_date."""
        result = build_aggregation(enriched_orders_df)
        years = sorted(set(row["year"] for row in result.collect()))
        assert years == [2016, 2017]

    def test_groupby_produces_correct_row_count(self, enriched_orders_df):
        """Each unique (year, category, sub_category, customer_name) should have one row."""
        result = build_aggregation(enriched_orders_df)
        # 2016: Alice/Furniture/Chairs, Alice/Technology/Phones = 2
        # 2017: Bob/Furniture/Chairs, Alice/Office Supplies/Paper, Bob/Technology/Phones = 3
        assert result.count() == 5

    def test_profit_sum_matches_source(self, enriched_orders_df):
        """Sum of total_profit should equal sum of source profit."""
        source_total = enriched_orders_df.agg(
            F.round(F.sum("profit"), 2)
        ).collect()[0][0]
        agg = build_aggregation(enriched_orders_df)
        agg_total = agg.agg(
            F.round(F.sum("total_profit"), 2)
        ).collect()[0][0]
        assert source_total == agg_total

    def test_order_count_correct(self, enriched_orders_df):
        """Order count for Alice in 2016 should be 2."""
        result = build_aggregation(enriched_orders_df)
        alice_2016 = result.filter(
            (F.col("year") == 2016) & (F.col("customer_name") == "Alice")
        )
        total_orders = alice_2016.agg(F.sum("order_count")).collect()[0][0]
        assert total_orders == 2

    def test_negative_profit_preserved(self, enriched_orders_df):
        """Negative profits should not be lost in aggregation."""
        result = build_aggregation(enriched_orders_df)
        bob_chairs = result.filter(
            (F.col("customer_name") == "Bob") &
            (F.col("category") == "Furniture")
        ).collect()
        assert bob_chairs[0]["total_profit"] == -20.00

    def test_required_columns_present(self, enriched_orders_df):
        """Aggregated DataFrame should have all required columns."""
        result = build_aggregation(enriched_orders_df)
        required = ["year", "category", "sub_category", "customer_name",
                    "total_profit", "order_count", "total_revenue"]
        for col in required:
            assert col in result.columns

    def test_empty_dataframe(self, spark):
        """Aggregation of empty DataFrame should produce zero rows."""
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("order_date", DateType()),
            StructField("customer_name", StringType()),
            StructField("category", StringType()),
            StructField("sub_category", StringType()),
            StructField("profit", DoubleType()),
            StructField("price", DoubleType()),
        ])
        empty = spark.createDataFrame([], schema)
        result = build_aggregation(empty)
        assert result.count() == 0

    def test_single_row(self, spark):
        """Single-row DataFrame should produce one aggregation row."""
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("order_date", DateType()),
            StructField("customer_name", StringType()),
            StructField("category", StringType()),
            StructField("sub_category", StringType()),
            StructField("profit", DoubleType()),
            StructField("price", DoubleType()),
        ])
        df = spark.createDataFrame(
            [("O-001", date(2020, 1, 1), "Test", "Cat", "Sub", 99.99, 200.0)],
            schema
        )
        result = build_aggregation(df)
        assert result.count() == 1
        row = result.collect()[0]
        assert row["total_profit"] == 99.99
        assert row["year"] == 2020
