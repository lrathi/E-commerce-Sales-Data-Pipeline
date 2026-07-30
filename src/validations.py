"""
Reusable validation functions.

These checks help verify data quality before datasets are written
to Delta tables. Used across all code notebooks via %run.

All functions are Spark Connect compatible.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def null_summary(df: DataFrame) -> DataFrame:
    cols = df.columns
    return df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in cols
    ])


def duplicate_summary(df: DataFrame, key_column: str) -> DataFrame:
    return (
        df
        .groupBy(key_column)
        .count()
        .filter(F.col("count") > 1)
    )


def assert_not_empty(df: DataFrame):
    assert df.count() > 0, "DataFrame is empty."


def assert_unique_key(df: DataFrame, key_column: str):
    duplicates = duplicate_summary(df, key_column)
    assert duplicates.count() == 0, (
        f"Duplicate values found in {key_column}"
    )


def assert_required_columns(df: DataFrame, required_columns: list):
    df_cols = set(df.columns)
    missing = [c for c in required_columns if c not in df_cols]
    assert len(missing) == 0, f"Missing columns: {missing}"


def assert_row_count_match(df1: DataFrame, df2: DataFrame, label: str = ""):
    count1 = df1.count()
    count2 = df2.count()
    assert count1 == count2, (
        f"Row count mismatch{' (' + label + ')' if label else ''}: "
        f"{count1} vs {count2}"
    )


def assert_profit_reconciliation(source_df: DataFrame, agg_df: DataFrame,
                                  source_col: str = "profit",
                                  agg_col: str = "total_profit"):
    source_total = source_df.agg(F.round(F.sum(source_col), 2)).collect()[0][0]
    agg_total = agg_df.agg(F.round(F.sum(agg_col), 2)).collect()[0][0]
    assert source_total == agg_total, (
        f"Profit mismatch: source={source_total}, aggregate={agg_total}"
    )
