# PEI — E-commerce Sales Data Pipeline

## Overview

A medallion-architecture data pipeline built on Databricks that processes e-commerce sales data (customers, products, orders) through Raw → Enriched → Aggregated layers using PySpark, with SQL-based reporting on top.

## Architecture

```
┌──────────────┐     ┌──────────────────┐     ┌───────────────────┐     ┌────────────────┐
│  Source Files│ ──► │  01 Raw Layer    │ ──► │  02 Enrichment    │ ──► │ 03 Aggregation │
│  (CSV/JSON)  │     │  (Delta tables)  │     │  (Cleaned+Joined) │     │ (Profit Summary│
└──────────────┘     └──────────────────┘     └───────────────────┘     └────────────────┘
                                                                               │
                                                                               ▼
                                                                        ┌────────────────┐
                                                                        │ 04 SQL Queries │
                                                                        └────────────────┘
```

## Project Structure

```
/PEI/
├── code/
│   ├── 01 Raw Layer Ingestion      # Task 1: Ingest CSV/JSON → Delta
│   ├── 02 Enrichment Layer          # Tasks 2-3: Clean + denormalise
│   ├── 03 Aggregation Layer         # Task 4: Profit aggregations
│   └── 04 SQL Aggregates            # Task 5: SQL reporting queries
├── tests/
│   ├── __init__.py
│   ├── conftest.py                  # Shared fixtures (SparkSession, sample DFs)
│   ├── test_raw_layer.py            # 15 unit tests (standardize, trim, dedup)
│   ├── test_enrichment_layer.py     # 14 unit tests (name clean, address, price, date, profit)
│   ├── test_aggregation_layer.py    # 8 unit tests (groupBy, profit sum, dimensions)
│   ├── test_validations.py          # 14 unit tests (all 7 validation functions)
│   └── test_integration.py          # 16 integration tests (actual Delta tables)
├── utils/
│   ├── transformations.py           # 10 reusable PySpark transform functions
│   └── validations.py               # 7 data quality assertion functions
├── datasets/
│   ├── Customer.csv                 # 793 rows, Latin-1, multiline addresses
│   ├── Products.csv                 # 1851 rows (1818 unique products)
│   └── Orders.json                  # 9994 rows, multiLine JSON array
├── pytest.ini                       # pytest configuration
├── requirements.txt                 # pyspark, delta-spark, pytest
├── README.md                        # This file
└── ARCHITECTURE.md                  # Detailed design decisions, Q&A for reviewer
```

## Tables Created

| Layer | Table | Rows | Description |
|-------|-------|------|-------------|
| Raw | `pei.raw_customers` | 793 | Standardised column names, trimmed, deduplicated |
| Raw | `pei.raw_products` | 1818 | Standardised, deduplicated on product_id |
| Raw | `pei.raw_orders` | 9994 | Standardised, deduplicated on row_id |
| Enriched | `pei.enriched_customers` | 793 | Names cleaned (regex), addresses normalised |
| Enriched | `pei.enriched_products` | 1818 | price_per_product cast to double |
| Enriched | `pei.enriched_orders` | 9994 | Joined with customers+products, dates parsed, profit rounded, category derived from product_id |
| Aggregated | `pei.agg_profit_summary` | 8037 | Profit by year/category/sub_category/customer |

## How to Run

**Execute notebooks in order:**

1. `01 Raw Layer Ingestion` — creates raw tables
2. `02 Enrichment Layer` — creates enriched tables
3. `03 Aggregation Layer` — creates aggregate table
4. `04 SQL Aggregates` — outputs SQL results (read-only)

**Run tests:**
```
cd /Workspace/Users/lakrathe@publicisgroupe.net/PEI
python -m pytest tests/ -v
```

## Verified Execution Results (30 July 2026)

All notebooks executed end-to-end with zero failures:
- Task 1: 793 + 1818 + 9994 raw rows written, all uniqueness assertions passed
- Tasks 2-3: Enrichment preserved all 9994 orders, 0 null dates, 0 unrounded profits, 0 null categories, 0 dirty names remaining
- Task 4: 8037 aggregate rows, profit reconciliation matched ($278,417.03), categories = [Furniture, Office Supplies, Technology]
- Task 5: All 4 SQL queries return correct results (4 years, 12 year-category combos, 793 customers, 2490 customer-year combos)

## Design Decisions

- **Overwrite mode**: All writes use `mode("overwrite")` for idempotency — re-running any notebook produces the same result
- **Left joins**: Orders join to customers/products with LEFT JOIN to preserve all 9994 orders even if a product_id is missing from Products.csv
- **Category derivation from product_id**: 44 product_ids in orders are not in Products.csv. Rather than filtering (loses profit) or inventing "Unknown" (wrong category count), category is derived from the product_id prefix encoding (`FUR`→Furniture, `OFF`→Office Supplies, `TEC`→Technology) — verified 1:1 mapping across all 1818 known products
- **4-step regex name cleaning**: Uses `(?<=[a-zA-Z])[^a-zA-Z]+(?=[a-z])` lookahead to rejoin split words (`Ad...am`→`Adam`, `B   ecky`→`Becky`) while preserving word boundaries (uppercase = new word = kept). Handles `O'Brien`, `Anne-Marie` correctly
- **Phone `#ERROR!` left as-is**: 103 rows have `#ERROR!` in phone — confirmed deliberate source data, not a parsing error
- **try_cast for products**: 51 rows have CSV-misaligned data (state names in price column) — `try_cast` returns NULL instead of failing
- **Shared utils**: 10 transform functions + 7 validation functions in `/utils/`, reused via `%run`
- **Profit reconciliation**: `SUM(enriched_orders.profit) == SUM(agg.total_profit)` = $278,417.03

## Key Data Facts

- **Categories**: Exactly 3 (Furniture, Office Supplies, Technology) — no NULLs, no invented values
- **Profit**: $278,417.03 total, reconciled across all layers
- **Orders preserved**: All 9994 raw orders present in enriched_orders
- **Products deduped**: 1851 → 1818 (33 duplicates removed)
- **Test coverage**: 67 tests total (15 + 14 + 8 + 14 + 16) across 5 test files

## Utils Functions

### transformations.py (10 functions)
| Function | Purpose |
|----------|---------|
| `standardize_column_names(df)` | Lowercase, underscores, strip spaces |
| `trim_string_columns(df)` | Remove whitespace from all string columns |
| `remove_duplicate_keys(df, key)` | Keep one row per key via row_number() |
| `clean_customer_name(df, col)` | 4-step regex: rejoin split words, strip junk, collapse spaces |
| `normalize_address(df, col)` | Replace `\n` with `, ` |
| `cast_price_column(df, col)` | try_cast string to double |
| `parse_date_column(df, col, fmt)` | String to DateType |
| `round_profit(df, col)` | Round to 2 decimal places |
| `derive_category_from_product_id(df)` | Fill NULL category from product_id prefix encoding |

### validations.py (7 functions)
| Function | Purpose |
|----------|---------|
| `null_summary(df)` | Count NULLs per column |
| `duplicate_summary(df, key)` | Find duplicate business keys |
| `assert_not_empty(df)` | Fail if 0 rows |
| `assert_unique_key(df, key)` | Fail if duplicate keys exist |
| `assert_required_columns(df, cols)` | Fail if columns missing |
| `assert_row_count_match(df1, df2)` | Fail if row counts differ |
| `assert_profit_reconciliation(src, agg)` | Fail if profit sums don't match |

## Test Files

| File | Tests | What It Validates |
|------|-------|------------------|
| `test_raw_layer.py` | 15 | standardize_column_names, trim_string_columns, remove_duplicate_keys |
| `test_enrichment_layer.py` | 14 | clean_customer_name (8 parametrized cases), normalize_address, cast_price_column, parse_date_column, round_profit |
| `test_aggregation_layer.py` | 8 | GroupBy logic, profit sum matches source, dimensions correct |
| `test_validations.py` | 14 | All 7 validation functions (pass + fail cases using pytest.raises) |
| `test_integration.py` | 16 | Actual Delta tables: row counts, unique keys, no NULL categories, profit reconciliation, categories == [Furniture, Office Supplies, Technology] |

## For Reviewer

See `ARCHITECTURE.md` for:
- Detailed explanation of every function (what/how/why)
- Alternatives considered and rejected with reasoning
- Potential reviewer questions with prepared answers
- Complete verified output metrics
