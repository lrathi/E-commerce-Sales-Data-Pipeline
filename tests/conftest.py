"""
Shared pytest fixtures for PEI assessment test suite.

Provides a SparkSession and small test DataFrames for
positive, negative, and edge-case testing.
"""

import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, LongType, DateType
)

# Make utils importable
sys.path.insert(0, "/Workspace/Users/lakrathe@publicisgroupe.net/PEI/utils")


@pytest.fixture(scope="session")
def spark():
    """Provide a SparkSession for tests."""
    return SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# Raw Layer Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_customers_df(spark):
    """Small customers DataFrame with dirty names and multiline addresses."""
    schema = StructType([
        StructField("Customer ID", StringType()),
        StructField("Customer Name", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("address", StringType()),
        StructField("Segment", StringType()),
        StructField("Country", StringType()),
        StructField("City", StringType()),
        StructField("State", StringType()),
        StructField("Postal Code", StringType()),
        StructField("Region", StringType()),
    ])
    data = [
        ("AA-10315", "Alex Avila", "alex@test.com", "555-0001",
         "123 Main St", "Consumer", "United States", "Austin", "Texas", "78701", "Central"),
        ("PT-19090", "Pete@#$ Takahito", "pete@test.com", "555-0002",
         "456 Oak Ave\nSuite 100", "Consumer", "United States", "Dallas", "Texas", "75001", "Central"),
        ("AH-10075", "Ad.       ..am Hart", "adam@test.com", "555-0003",
         "789 Elm Blvd\nApt 5B", "Corporate", "United States", "Columbus", "Ohio", "43229", "East"),
        ("GN-14567", "Gary567 Nelson", "gary@test.com", "555-0004",
         "321 Pine Rd", "Home Office", "United States", "Chicago", "Illinois", "60601", "Central"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_products_df(spark):
    """Small products DataFrame with string prices."""
    schema = StructType([
        StructField("Product ID", StringType()),
        StructField("Category", StringType()),
        StructField("Sub-Category", StringType()),
        StructField("Product Name", StringType()),
        StructField("State", StringType()),
        StructField("Price per product", StringType()),
    ])
    data = [
        ("FUR-CH-001", "Furniture", "Chairs", "Leather Chair", "New York", "81.882"),
        ("TEC-AC-002", "Technology", "Accessories", "USB Drive", "Oklahoma", "72.99"),
        ("OFF-BI-003", "Office Supplies", "Binders", "Ring Binders", "Colorado", "4.25"),
        ("OFF-PA-004", "Office Supplies", "Paper", "Xerox Paper", "California", "California"),  # malformed
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_orders_df(spark):
    """Small orders DataFrame with string dates."""
    schema = StructType([
        StructField("row_id", LongType()),
        StructField("order_id", StringType()),
        StructField("order_date", StringType()),
        StructField("ship_date", StringType()),
        StructField("ship_mode", StringType()),
        StructField("customer_id", StringType()),
        StructField("product_id", StringType()),
        StructField("quantity", LongType()),
        StructField("price", DoubleType()),
        StructField("discount", DoubleType()),
        StructField("profit", DoubleType()),
    ])
    data = [
        (1, "CA-2016-001", "21/8/2016", "25/8/2016", "Standard Class",
         "AA-10315", "FUR-CH-001", 7, 573.17, 0.3, 63.6876),
        (2, "CA-2017-002", "23/9/2017", "29/9/2017", "Standard Class",
         "PT-19090", "TEC-AC-002", 4, 291.96, 0.0, 102.19),
        (3, "US-2016-003", "6/10/2016", "7/10/2016", "First Class",
         "AH-10075", "OFF-BI-003", 4, 17.0, 0.7, -14.923),
        (4, "CA-2016-004", "15/3/2016", "20/3/2016", "Second Class",
         "AA-10315", "OFF-PA-004", 2, 15.55, 0.2, 5.6412),
    ]
    return spark.createDataFrame(data, schema)


# ---------------------------------------------------------------------------
# Edge-Case Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def empty_df(spark):
    """Empty DataFrame with basic schema."""
    schema = StructType([
        StructField("id", StringType()),
        StructField("value", StringType()),
    ])
    return spark.createDataFrame([], schema)


@pytest.fixture
def df_with_duplicates(spark):
    """DataFrame with duplicate keys for dedup testing."""
    data = [
        ("K1", "first"),
        ("K1", "duplicate"),
        ("K2", "unique"),
        ("K3", "first"),
        ("K3", "duplicate"),
        ("K3", "triplicate"),
    ]
    schema = StructType([
        StructField("key", StringType()),
        StructField("value", StringType()),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def df_with_spaces(spark):
    """DataFrame with leading/trailing spaces in string columns."""
    data = [
        ("  AA-001  ", "  Alex  ", 100),
        ("BB-002", "  Bob", 200),
        ("CC-003  ", "Charlie  ", 300),
    ]
    schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("amount", IntegerType()),
    ])
    return spark.createDataFrame(data, schema)
