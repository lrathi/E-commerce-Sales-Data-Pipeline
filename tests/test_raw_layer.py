"""
Tests for raw layer transformation functions.

Covers: standardize_column_names, trim_string_columns, remove_duplicate_keys
"""

import pytest
from pyspark.sql import functions as F
from transformations import (
    standardize_column_names,
    trim_string_columns,
    remove_duplicate_keys,
)


# =============================================================================
# standardize_column_names
# =============================================================================

class TestStandardizeColumnNames:
    """Tests for column name standardisation."""

    def test_converts_to_lowercase(self, sample_customers_df):
        """Column names should be lowercased."""
        result = standardize_column_names(sample_customers_df)
        for col in result.columns:
            assert col == col.lower()

    def test_replaces_spaces_with_underscores(self, sample_customers_df):
        """Spaces in column names become underscores."""
        result = standardize_column_names(sample_customers_df)
        for col in result.columns:
            assert " " not in col

    def test_replaces_hyphens_with_underscores(self, sample_products_df):
        """Hyphens in column names become underscores."""
        result = standardize_column_names(sample_products_df)
        for col in result.columns:
            assert "-" not in col

    def test_preserves_row_count(self, sample_customers_df):
        """Row count should not change after column rename."""
        original_count = sample_customers_df.count()
        result = standardize_column_names(sample_customers_df)
        assert result.count() == original_count

    @pytest.mark.parametrize("input_col,expected_col", [
        ("Customer ID", "customer_id"),
        ("Product Name", "product_name"),
        ("Sub-Category", "sub_category"),
        ("Price per product", "price_per_product"),
        ("Postal Code", "postal_code"),
    ])
    def test_specific_column_mappings(self, spark, input_col, expected_col):
        """Verify specific column name transformations."""
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([StructField(input_col, StringType())])
        df = spark.createDataFrame([("value",)], schema)
        result = standardize_column_names(df)
        assert expected_col in result.columns

    def test_empty_dataframe(self, empty_df):
        """Should handle empty DataFrames without error."""
        result = standardize_column_names(empty_df)
        assert result.count() == 0
        assert len(result.columns) == 2


# =============================================================================
# trim_string_columns
# =============================================================================

class TestTrimStringColumns:
    """Tests for trimming whitespace from string columns."""

    def test_removes_leading_trailing_spaces(self, df_with_spaces):
        """Leading and trailing spaces should be removed."""
        result = trim_string_columns(df_with_spaces)
        rows = result.collect()
        assert rows[0]["id"] == "AA-001"
        assert rows[0]["name"] == "Alex"
        assert rows[1]["name"] == "Bob"
        assert rows[2]["id"] == "CC-003"

    def test_does_not_modify_numeric_columns(self, df_with_spaces):
        """Numeric columns should remain unchanged."""
        result = trim_string_columns(df_with_spaces)
        amounts = [row["amount"] for row in result.collect()]
        assert amounts == [100, 200, 300]

    def test_preserves_row_count(self, df_with_spaces):
        """Row count should not change."""
        result = trim_string_columns(df_with_spaces)
        assert result.count() == df_with_spaces.count()

    def test_empty_dataframe(self, empty_df):
        """Should handle empty DataFrames."""
        result = trim_string_columns(empty_df)
        assert result.count() == 0

    def test_already_trimmed_data(self, spark):
        """Already-clean data should pass through unchanged."""
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([StructField("name", StringType())])
        df = spark.createDataFrame([("Alice",), ("Bob",)], schema)
        result = trim_string_columns(df)
        names = [row["name"] for row in result.collect()]
        assert names == ["Alice", "Bob"]


# =============================================================================
# remove_duplicate_keys
# =============================================================================

class TestRemoveDuplicateKeys:
    """Tests for deduplication by business key."""

    def test_removes_duplicates(self, df_with_duplicates):
        """Duplicate keys should be reduced to one row each."""
        result = remove_duplicate_keys(df_with_duplicates, "key")
        assert result.count() == 3  # K1, K2, K3

    def test_unique_keys_unchanged(self, spark):
        """DataFrame with no duplicates should pass through unchanged."""
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("key", StringType()),
            StructField("val", StringType()),
        ])
        df = spark.createDataFrame([("A", "1"), ("B", "2"), ("C", "3")], schema)
        result = remove_duplicate_keys(df, "key")
        assert result.count() == 3

    def test_all_duplicates(self, spark):
        """All rows sharing the same key should collapse to one."""
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("key", StringType()),
            StructField("val", StringType()),
        ])
        df = spark.createDataFrame(
            [("X", "a"), ("X", "b"), ("X", "c")], schema
        )
        result = remove_duplicate_keys(df, "key")
        assert result.count() == 1

    def test_row_number_column_removed(self, df_with_duplicates):
        """Internal row_number column should not appear in output."""
        result = remove_duplicate_keys(df_with_duplicates, "key")
        assert "row_number" not in result.columns

    def test_empty_dataframe(self, empty_df):
        """Should handle empty DataFrames."""
        result = remove_duplicate_keys(empty_df, "id")
        assert result.count() == 0
