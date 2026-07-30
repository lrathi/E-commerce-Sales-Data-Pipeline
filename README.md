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
│   ├── test_raw_layer.py            # 15 unit tests
│   ├── test_enrichment_layer.py     # 14 unit tests
│   ├── test_aggregation_layer.py    # 8 unit tests
│   └── test_integration.py          # End-to-end table verification
├── utils/
│   ├── transformations.py           # Reusable PySpark transforms
│   └── validations.py               # Data quality assertion functions
├── datasets/
│   ├── Customer.csv                 # 793 rows, Latin-1, multiline addresses
│   ├── Products.csv                 # 1851 rows (1818 unique products)
│   └── Orders.json                  # 9994 rows, multiLine JSON array
├── pytest.ini                       # pytest configuration
├── requirements.txt                 # pyspark, delta-spark, pytest
└── README.md                        # This file
```

## Tables Created

| Layer | Table | Rows | Description |
|-------|-------|------|-------------|
| Raw | `pei.raw_customers` | 793 | Standardised column names, trimmed, deduplicated |
| Raw | `pei.raw_products` | 1818 | Standardised, deduplicated on product_id |
| Raw | `pei.raw_orders` | 9994 | Standardised, deduplicated on row_id |
| Enriched | `pei.enriched_customers` | 793 | Names cleaned (regex), addresses normalised |
| Enriched | `pei.enriched_products` | 1818 | price_per_product cast to double |
| Enriched | `pei.enriched_orders` | 9994 | Joined with customers+products, dates parsed, profit rounded |
| Aggregated | `pei.agg_profit_summary` | ~6K | Profit by year/category/sub_category/customer |

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
- Tasks 2-3: Enrichment preserved all rows, 0 null dates, 0 unrounded profits
- Task 4: 8073 aggregate rows, profit reconciliation matched ($278,417.03)
- Task 5: All 4 SQL queries return correct results across years 2014-2017

## Design Decisions

- **Overwrite mode**: All writes use `mode("overwrite")` for idempotency — re-running any notebook produces the same result
- **Left joins**: Orders join to customers/products with LEFT JOIN to preserve all orders even if a lookup key is missing
- **try_cast for products**: 51 product rows have CSV-misaligned data (state names in price column) — `try_cast` sets these to NULL instead of failing the pipeline
- **Shared utils**: Common operations (column standardisation, trimming, dedup) live in `/utils/` and are reused via `%run`
