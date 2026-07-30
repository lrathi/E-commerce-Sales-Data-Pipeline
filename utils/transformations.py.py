# Databricks notebook source
"""
Reusable transformation functions used across Databricks notebooks.

These functions cover:
- Raw layer: column standardisation, trimming, deduplication
- Enrichment layer: name cleaning, address normalisation, date parsing, price casting

All functions are Spark Connect compatible (no .withColumn in loops,
no repeated .columns/.schema access).
"""

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# =============================================================================
# Raw Layer Functions
# =============================================================================

def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Standardize column names: lowercase, underscores, no leading/trailing spaces.

    Uses .toDF() (single-pass rename) to avoid deeply nested plans.
    """
    new_names = [
        c.strip().lower().replace(" ", "_").replace("-", "_")
        for c in df.columns
    ]
    return df.toDF(*new_names)


def trim_string_columns(df: DataFrame) -> DataFrame:
    """
    Remove leading and trailing whitespace from all string columns.

    Uses .withColumns() (batch API) to avoid nested execution plans.
    """
    string_cols = [
        field.name for field in df.schema.fields
        if field.dataType.simpleString() == "string"
    ]
    if not string_cols:
        return df
    return df.withColumns({
        c: F.trim(F.col(c)) for c in string_cols
    })


def remove_duplicate_keys(df: DataFrame, key_column: str) -> DataFrame:
    """
    Keep one record per business key using row_number window.

    Retains the first record encountered per key.
    """
    window = Window.partitionBy(key_column).orderBy(F.lit(1))
    return (
        df
        .withColumns({"_row_num": F.row_number().over(window)})
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# =============================================================================
# Enrichment Layer Functions
# =============================================================================

def clean_customer_name(df: DataFrame, column: str = "customer_name") -> DataFrame:
    """
    Clean customer names by removing noise characters and rejoining split words.

    Handles real-world issues found in source data:
    - 'Ad.       ..am Hart'     -> 'Adam Hart'      (mid-word noise removed)
    - 'Beth Tho098-.,;;mpson'   -> 'Beth Thompson'  (mid-word junk rejoined)
    - 'B         ecky Martin'   -> 'Becky Martin'   (spaces within word removed)
    - '[]-=;''Becky Pak'        -> 'Becky Pak'      (leading junk stripped)
    - 'Pete@#$ Takahito'        -> 'Pete Takahito'  (inter-word junk removed)
    - 'Gary567 Hansen'          -> 'Gary Hansen'    (digits removed)

    Preserves apostrophes and hyphens at word boundaries (O'Brien, Anne-Marie)
    because lookahead requires lowercase (uppercase = new word boundary = keep).

    Algorithm:
    1. Remove mid-word noise: non-alpha between letters where right is lowercase
    2. Remove remaining non-alpha (except spaces, apostrophes, hyphens)
    3. Strip leading/trailing non-alpha characters
    4. Collapse multiple spaces to single space
    """
    # Step 1: Join split words — remove noise between alpha chars where next is lowercase
    step1 = F.regexp_replace(F.col(column), r"(?<=[a-zA-Z])[^a-zA-Z]+(?=[a-z])", "")
    # Step 2: Remove remaining junk (digits, symbols) keeping spaces/apostrophes/hyphens
    step2 = F.regexp_replace(step1, r"[^a-zA-Z\s'\-]", "")
    # Step 3: Strip leading/trailing non-alpha (e.g., leading -'' from '[]-=;''Becky')
    step3 = F.regexp_replace(step2, r"^[^a-zA-Z]+|[^a-zA-Z]+$", "")
    # Step 4: Collapse whitespace and trim
    cleaned = F.trim(F.regexp_replace(step3, r"\s+", " "))
    return df.withColumns({column: cleaned})


def normalize_address(df: DataFrame, column: str = "address") -> DataFrame:
    """
    Replace embedded newlines in addresses with comma-space.

    Source CSV has multiline quoted fields; this normalises them
    to single-line format for cleaner display.
    """
    return df.withColumns({
        column: F.regexp_replace(F.col(column), r"\n", ", ")
    })


def cast_price_column(df: DataFrame, column: str = "price_per_product") -> DataFrame:
    """
    Safely cast a string price column to double.

    Uses try_cast to handle malformed values (e.g., CSV misalignment
    where state names appear in the price column). Invalid values
    become NULL instead of raising an error.
    """
    return df.withColumns({
        column: F.expr(f"try_cast({column} as double)")
    })


def parse_date_column(df: DataFrame, column: str, fmt: str = "d/M/yyyy") -> DataFrame:
    """
    Parse a string date column to DateType using the given format.

    Uses try_to_date (not to_date) because Databricks ANSI mode throws
    DateTimeException on invalid input. try_to_date returns NULL gracefully.
    The source data uses d/M/yyyy (e.g., '6/10/2016', '21/8/2016').
    """
    return df.withColumns({
        column: F.try_to_date(F.col(column), fmt)
    })


def round_profit(df: DataFrame, column_name: str = "profit") -> DataFrame:
    """
    Round profit to 2 decimal places (assessment requirement).
    """
    return df.withColumns({
        column_name: F.round(F.col(column_name), 2)
    })


def derive_category_from_product_id(df: DataFrame) -> DataFrame:
    """
    Fill NULL category/sub_category by decoding the product_id prefix.

    Product ID format: CAT-SUB-NUMBER (e.g., FUR-BO-10000112)
    - Position 1-3: Category prefix (FUR/OFF/TEC)
    - Position 5-6: Sub-category prefix (BO/CH/BI/...)

    This handles 44 product_ids in orders that are not in Products.csv
    but whose category can be reliably derived from the ID encoding.
    Only fills NULLs — already-populated values from the JOIN are untouched.
    """
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