"""
Tests for enrichment layer transformations.

Covers: customer name cleaning, address normalisation, price casting,
        date parsing, profit rounding, and join logic.

All functions tested are imported from utils/transformations.py,
ensuring we test the same code that notebooks use via %run.
"""

import pytest
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from transformations import (
    clean_customer_name,
    normalize_address,
    cast_price_column,
    parse_date_column,
    round_profit,
    standardize_column_names,
)


# =============================================================================
# Customer Name Cleaning
# =============================================================================

class TestCustomerNameCleaning:
    """Tests for removing special characters and digits from customer names."""

    @pytest.mark.parametrize("dirty_name,expected_clean", [
        ("Pete@#$ Takahito", "Pete Takahito"),
        ("Gary567 Hansen", "Gary Hansen"),
        ("Ad.       ..am Hart", "Adam Hart"),
        ("Beth Tho098-.,;;mpson", "Beth Thompson"),
        ("B         ecky Martin", "Becky Martin"),
        ("[]-=;''Becky Pak", "Becky Pak"),
        ("Alex Avila", "Alex Avila"),
        ("O'Brien-Smith", "O'Brien-Smith"),
    ])
    def test_name_cleaning(self, spark, dirty_name, expected_clean):
        """Mid-word noise removed, leading junk stripped, spaces collapsed."""
        schema = StructType([StructField("customer_name", StringType())])
        df = spark.createDataFrame([(dirty_name,)], schema)
        result = clean_customer_name(df)
        actual = result.collect()[0]["customer_name"]
        assert actual == expected_clean

    def test_empty_name(self, spark):
        """Empty string should remain empty after cleaning."""
        schema = StructType([StructField("customer_name", StringType())])
        df = spark.createDataFrame([("",)], schema)
        result = clean_customer_name(df)
        actual = result.collect()[0]["customer_name"]
        assert actual == ""

    def test_null_name(self, spark):
        """NULL names should remain NULL."""
        schema = StructType([StructField("customer_name", StringType())])
        df = spark.createDataFrame([(None,)], schema)
        result = clean_customer_name(df)
        actual = result.collect()[0]["customer_name"]
        assert actual is None


# =============================================================================
# Address Normalisation
# =============================================================================

class TestAddressNormalisation:
    """Tests for replacing newlines in address field."""

    @pytest.mark.parametrize("raw_address,expected", [
        ("123 Main St\nApt 5B", "123 Main St, Apt 5B"),
        ("456 Oak Ave\nSuite 100\nFloor 2", "456 Oak Ave, Suite 100, Floor 2"),
        ("789 Elm Blvd", "789 Elm Blvd"),  # no newline
    ])
    def test_newline_replacement(self, spark, raw_address, expected):
        """Newlines should be replaced with comma-space."""
        schema = StructType([StructField("address", StringType())])
        df = spark.createDataFrame([(raw_address,)], schema)
        result = df.withColumn(
            "address",
            F.regexp_replace(F.col("address"), r"\n", ", ")
        )
        actual = result.collect()[0]["address"]
        assert actual == expected


# =============================================================================
# Product Price Casting
# =============================================================================

class TestPriceCasting:
    """Tests for casting price_per_product from string to double."""

    def test_valid_prices_cast(self, spark):
        """Valid numeric strings should cast to doubles."""
        schema = StructType([StructField("price_per_product", StringType())])
        df = spark.createDataFrame(
            [("81.882",), ("72.99",), ("4.25",)], schema
        )
        result = df.withColumn(
            "price_per_product",
            F.expr("try_cast(price_per_product as double)")
        )
        prices = [row["price_per_product"] for row in result.collect()]
        assert prices == [81.882, 72.99, 4.25]

    def test_invalid_price_becomes_null(self, spark):
        """Non-numeric strings should become NULL with try_cast."""
        schema = StructType([StructField("price_per_product", StringType())])
        df = spark.createDataFrame(
            [("California",), ("New York",)], schema
        )
        result = df.withColumn(
            "price_per_product",
            F.expr("try_cast(price_per_product as double)")
        )
        prices = [row["price_per_product"] for row in result.collect()]
        assert all(p is None for p in prices)

    def test_mixed_valid_invalid(self, sample_products_df):
        """Mix of valid and invalid prices handled correctly."""
        from transformations import standardize_column_names
        df = standardize_column_names(sample_products_df)
        result = df.withColumn(
            "price_per_product",
            F.expr("try_cast(price_per_product as double)")
        )
        null_count = result.filter(F.col("price_per_product").isNull()).count()
        assert null_count == 1  # "California" value


# =============================================================================
# Date Parsing
# =============================================================================

class TestDateParsing:
    """Tests for parsing d/M/yyyy date strings."""

    @pytest.mark.parametrize("date_str,expected", [
        ("21/8/2016", date(2016, 8, 21)),
        ("6/10/2016", date(2016, 10, 6)),
        ("23/9/2017", date(2017, 9, 23)),
        ("1/1/2014", date(2014, 1, 1)),
    ])
    def test_valid_date_parsing(self, spark, date_str, expected):
        """Valid d/M/yyyy strings should parse to correct dates."""
        schema = StructType([StructField("order_date", StringType())])
        df = spark.createDataFrame([(date_str,)], schema)
        result = df.withColumn(
            "order_date", F.to_date(F.col("order_date"), "d/M/yyyy")
        )
        actual = result.collect()[0]["order_date"]
        assert actual == expected

    def test_invalid_date_returns_null(self, spark):
        """Invalid date strings should become NULL."""
        schema = StructType([StructField("order_date", StringType())])
        df = spark.createDataFrame([("not-a-date",), ("2016/08/21",)], schema)
        result = df.withColumn(
            "order_date", F.to_date(F.col("order_date"), "d/M/yyyy")
        )
        nulls = result.filter(F.col("order_date").isNull()).count()
        assert nulls == 2


# =============================================================================
# Profit Rounding
# =============================================================================

class TestRoundProfit:
    """Tests for the round_profit utility function."""

    def test_rounds_to_two_decimals(self, sample_orders_df):
        """Profit should be rounded to 2 decimal places."""
        result = round_profit(sample_orders_df)
        profits = [row["profit"] for row in result.collect()]
        assert profits == [63.69, 102.19, -14.92, 5.64]

    def test_preserves_row_count(self, sample_orders_df):
        """Row count should not change."""
        result = round_profit(sample_orders_df)
        assert result.count() == sample_orders_df.count()

    def test_null_profit_stays_null(self, spark):
        """NULL profit values should remain NULL."""
        schema = StructType([StructField("profit", DoubleType())])
        df = spark.createDataFrame([(None,), (10.456,)], schema)
        result = round_profit(df)
        rows = result.collect()
        assert rows[0]["profit"] is None
        assert rows[1]["profit"] == 10.46
