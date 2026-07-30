# Databricks notebook source
"""
Reusable validation functions.

These checks help verify data quality before datasets are written
to Delta tables. Used across all code notebooks via %run.

All functions are Spark Connect compatible.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

def null_summary(df: DataFrame) -> DataFrame:
    """
    Return the number of NULL values for every column.
    """
    cols = df.columns  # single Analyze RPC
    return df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in cols
    ])


def duplicate_summary(df: DataFrame, key_column: str) -> DataFrame:
    """
    Return duplicate business keys (count > 1).
    """
    return (
        df
        .groupBy(key_column)
        .count()
        .filter(F.col("count") > 1)
    )


def assert_not_empty(df: DataFrame):
    """
    Ensure the DataFrame contains data.
    """
    assert df.count() > 0, "DataFrame is empty."


def assert_unique_key(df: DataFrame, key_column: str):
    """
    Ensure duplicate business keys do not exist.
    """
    duplicates = duplicate_summary(df, key_column)
    assert duplicates.count() == 0, (
        f"Duplicate values found in {key_column}"
    )


def assert_required_columns(df: DataFrame, required_columns: list):
    """
    Verify all expected columns exist in the DataFrame.
    """
    df_cols = set(df.columns)  # single Analyze RPC, cached as set
    missing = [c for c in required_columns if c not in df_cols]
    assert len(missing) == 0, f"Missing columns: {missing}"


def assert_row_count_match(df1: DataFrame, df2: DataFrame, label: str = ""):
    """
    Assert two DataFrames have the same row count.

    Used to verify no rows lost/gained during enrichment joins.
    """
    count1 = df1.count()
    count2 = df2.count()
    assert count1 == count2, (
        f"Row count mismatch{' (' + label + ')' if label else ''}: "
        f"{count1} vs {count2}"
    )


def assert_profit_reconciliation(source_df: DataFrame, agg_df: DataFrame,
                                  source_col: str = "profit",
                                  agg_col: str = "total_profit"):
    """
    Assert that the sum of profit in aggregate matches source.

    Used as a cross-layer data integrity check.
    """
    source_total = source_df.agg(F.round(F.sum(source_col), 2)).collect()[0][0]
    agg_total = agg_df.agg(F.round(F.sum(agg_col), 2)).collect()[0][0]
    assert source_total == agg_total, (
        f"Profit mismatch: source={source_total}, aggregate={agg_total}"
    )