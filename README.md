# Retail Sales ETL Pipeline

A compact **data-engineering** project that turns a messy raw sales export into a
clean, typed, analysis-ready dataset — Extract → Transform → Load.

The pipeline is written as small, reusable, testable functions in
[`src/etl.py`](src/etl.py); the notebook documents what it fixes and why.

## The problem (raw data)

The raw export (`data/raw/sales_raw.csv`, ~5,080 rows) is deliberately messy, like
real ingestion sources:

- mixed date formats (`2024-03-01`, `01/04/2024`, `20 Jun 2024`, …)
- currency symbols & thousands separators in `revenue` (`$1,234.50`)
- inconsistent category / region capitalisation and stray whitespace
- missing values, negative quantities and duplicate rows

## The pipeline

| stage | what it does |
|-------|--------------|
| **Extract** | read the raw sales export + a product reference table |
| **Transform** | parse dates & money, normalise text, drop duplicates/invalid rows, impute regions, join product data, derive `expected_revenue` & `order_month` |
| **Load** | write a strongly-typed **Parquet** file (+ CSV preview) |

Run audit (reproducible, seed-based):

```
duplicates_removed   : 80
invalid_rows_removed : 100
region_imputed       : 118
rows_out             : 4,900
```

## What's inside

```
retail-sales-etl/
├── data/
│   ├── generate_raw.py        # builds the messy raw data
│   ├── raw/                   # sales_raw.csv, products.csv
│   └── processed/             # sales_clean.parquet (output)
├── src/
│   └── etl.py                 # extract / transform / load + run_pipeline()
├── notebooks/
│   └── etl_pipeline.ipynb     # annotated walkthrough with before/after views
└── requirements.txt
```

## Run it

```bash
pip install -r requirements.txt
python data/generate_raw.py     # (re)create the messy raw data
python src/etl.py               # run the full pipeline -> Parquet
jupyter notebook notebooks/etl_pipeline.ipynb
```

## Why these choices

- **Function-per-concern** (`parse_money`, `parse_dates`, `normalise_text`, …) makes
  the logic unit-testable and easy to schedule (cron / Airflow).
- **Parquet** output preserves dtypes and is columnar/compressed for downstream analytics.
- An **audit dict** records exactly what was changed — essential for data-quality tracking.

---
Part of my [data & ML portfolio](https://github.com/ABouns).
