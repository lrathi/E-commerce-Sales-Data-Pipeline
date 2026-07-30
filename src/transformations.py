"""
Reusable transformation functions for PEI pipeline.
Importable version for pytest (mirrors utils/transformations.py notebook).
"""

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F


def standardize_column_names(df: DataFrame) -> DataFrame:
    new_names = [
        c.strip().lower().replace(" ", "_").replace("-", "_")
        for c in df.columns
    ]
    return df.toDF(*new_names)


def trim_string_columns(df: DataFrame) -> DataFrame:
    string_cols = [
        field.name for field in df.schema.fields
        if field.dataType.simpleString() == "string"
    ]
    if not string_cols:
        return df
    return df.withColumns({c: F.trim(F.col(c)) for c in string_cols})


def remove_duplicate_keys(df: DataFrame, key_column: str) -> DataFrame:
    window = Window.partitionBy(key_column).orderBy(F.lit(1))
    return (
        df
        .withColumns({"_row_num": F.row_number().over(window)})
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def clean_customer_name(df: DataFrame, column: str = "customer_name") -> DataFrame:
    step1 = F.regexp_replace(F.col(column), r"(?<=[a-zA-Z])[^a-zA-Z]+(?=[a-z])", "")
    step2 = F.regexp_replace(step1, r"[^a-zA-Z\s'\-]", "")
    step3 = F.regexp_replace(step2, r"^[^a-zA-Z]+|[^a-zA-Z]+$", "")
    cleaned = F.trim(F.regexp_replace(step3, r"\s+", " "))
    return df.withColumns({column: cleaned})


def normalize_address(df: DataFrame, column: str = "address") -> DataFrame:
    return df.withColumns({column: F.regexp_replace(F.col(column), r"\n", ", ")})


def cast_price_column(df: DataFrame, column: str = "price_per_product") -> DataFrame:
    return df.withColumns({column: F.expr(f"try_cast({column} as double)")})


def parse_date_column(df: DataFrame, column: str, fmt: str = "d/M/yyyy") -> DataFrame:
    return df.withColumns({column: F.try_to_date(F.col(column), fmt)})


def round_profit(df: DataFrame, column_name: str = "profit") -> DataFrame:
    return df.withColumns({column_name: F.round(F.col(column_name), 2)})


def derive_category_from_product_id(df: DataFrame) -> DataFrame:
    _cat_prefix = F.substring(F.col("product_id"), 1, 3)
    _sub_prefix = F.substring(F.col("product_id"), 5, 2)
    _derived_category = (
        F.when(_cat_prefix == "FUR", "Furniture")
         .when(_cat_prefix == "OFF", "Office Supplies")
         .when(_cat_prefix == "TEC", "Technology")
    )
    _derived_sub_category = (
        F.when(_sub_prefix == "AC", "Accessories")
         .when(_sub_prefix == "AP", "Appliances")
         .when(_sub_prefix == "AR", "Art")
         .when(_sub_prefix == "BI", "Binders")
         .when(_sub_prefix == "BO", "Bookcases")
         .when(_sub_prefix == "CH", "Chairs")
         .when(_sub_prefix == "CO", "Copiers")
         .when(_sub_prefix == "EN", "Envelopes")
         .when(_sub_prefix == "FA", "Fasteners")
         .when(_sub_prefix == "FU", "Furnishings")
         .when(_sub_prefix == "LA", "Labels")
         .when(_sub_prefix == "MA", "Machines")
         .when(_sub_prefix == "PA", "Paper")
         .when(_sub_prefix == "PH", "Phones")
         .when(_sub_prefix == "ST", "Storage")
         .when(_sub_prefix == "SU", "Supplies")
         .when(_sub_prefix == "TA", "Tables")
    )
    return df.withColumns({
        "category": F.coalesce(F.col("category"), _derived_category),
        "sub_category": F.coalesce(F.col("sub_category"), _derived_sub_category),
    })
