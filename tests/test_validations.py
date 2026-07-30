"""
Tests for validation utility functions (utils/validations.py).

Ensures the pipeline quality-gate functions work correctly:
- assert_not_empty, assert_unique_key, assert_required_columns
- assert_row_count_match, assert_profit_reconciliation
- null_summary, duplicate_summary
"""

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from validations import (
    null_summary,
    duplicate_summary,
    assert_not_empty,
    assert_unique_key,
    assert_required_columns,
    assert_row_count_match,
    assert_profit_reconciliation,
)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.getOrCreate()


# =============================================================================
# Tests for assert_not_empty
# =============================================================================

class TestAssertNotEmpty:

    def test_non_empty_passes(self, spark):
        df = spark.createDataFrame([(1,), (2,)], ["id"])
        assert_not_empty(df)  # Should not raise

    def test_empty_raises(self, spark):
        df = spark.createDataFrame([], StructType([StructField("id", IntegerType())]))
        with pytest.raises(AssertionError, match="empty"):
            assert_not_empty(df)


# =============================================================================
# Tests for assert_unique_key
# =============================================================================

class TestAssertUniqueKey:

    def test_unique_keys_pass(self, spark):
        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        assert_unique_key(df, "id")  # Should not raise

    def test_duplicate_keys_raise(self, spark):
        df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
        with pytest.raises(AssertionError, match="Duplicate"):
            assert_unique_key(df, "id")

    def test_single_row_passes(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        assert_unique_key(df, "id")


# =============================================================================
# Tests for assert_required_columns
# =============================================================================

class TestAssertRequiredColumns:

    def test_all_present_passes(self, spark):
        df = spark.createDataFrame([(1, "a", 3.0)], ["id", "name", "value"])
        assert_required_columns(df, ["id", "name", "value"])

    def test_subset_present_passes(self, spark):
        df = spark.createDataFrame([(1, "a", 3.0)], ["id", "name", "value"])
        assert_required_columns(df, ["id", "name"])

    def test_missing_column_raises(self, spark):
        df = spark.createDataFrame([(1, "a")], ["id", "name"])
        with pytest.raises(AssertionError, match="Missing columns"):
            assert_required_columns(df, ["id", "name", "country"])

    def test_empty_required_list_passes(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        assert_required_columns(df, [])


# =============================================================================
# Tests for assert_row_count_match
# =============================================================================

class TestAssertRowCountMatch:

    def test_same_count_passes(self, spark):
        df1 = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        df2 = spark.createDataFrame([("a",), ("b",), ("c",)], ["name"])
        assert_row_count_match(df1, df2, "test")

    def test_different_count_raises(self, spark):
        df1 = spark.createDataFrame([(1,), (2,)], ["id"])
        df2 = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        with pytest.raises(AssertionError, match="Row count mismatch"):
            assert_row_count_match(df1, df2, "test")

    def test_both_empty_passes(self, spark):
        schema = StructType([StructField("id", IntegerType())])
        df1 = spark.createDataFrame([], schema)
        df2 = spark.createDataFrame([], schema)
        assert_row_count_match(df1, df2)


# =============================================================================
# Tests for assert_profit_reconciliation
# =============================================================================

class TestAssertProfitReconciliation:

    def test_matching_totals_pass(self, spark):
        source = spark.createDataFrame([(10.5,), (20.3,), (-5.8,)], ["profit"])
        agg = spark.createDataFrame([(25.0,)], ["total_profit"])
        assert_profit_reconciliation(source, agg)

    def test_mismatched_totals_raise(self, spark):
        source = spark.createDataFrame([(10.0,), (20.0,)], ["profit"])
        agg = spark.createDataFrame([(100.0,)], ["total_profit"])
        with pytest.raises(AssertionError, match="Profit mismatch"):
            assert_profit_reconciliation(source, agg)


# =============================================================================
# Tests for null_summary
# =============================================================================

class TestNullSummary:

    def test_counts_nulls_correctly(self, spark):
        df = spark.createDataFrame(
            [(1, "a"), (2, None), (None, None)],
            StructType([
                StructField("id", IntegerType()),
                StructField("name", StringType()),
            ])
        )
        result = null_summary(df).collect()[0]
        assert result["id"] == 1
        assert result["name"] == 2

    def test_no_nulls_returns_zeros(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        result = null_summary(df).collect()[0]
        assert result["id"] == 0
        assert result["name"] == 0


# =============================================================================
# Tests for duplicate_summary
# =============================================================================

class TestDuplicateSummary:

    def test_finds_duplicates(self, spark):
        df = spark.createDataFrame([(1,), (1,), (2,), (3,), (3,), (3,)], ["id"])
        result = duplicate_summary(df, "id")
        assert result.count() == 2  # id=1 (count 2), id=3 (count 3)

    def test_no_duplicates_returns_empty(self, spark):
        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        result = duplicate_summary(df, "id")
        assert result.count() == 0
