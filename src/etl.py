"""Retail sales ETL pipeline: raw messy CSV -> validated, tidy Parquet.

Stages
------
1. **Extract**  – read the raw sales export and product reference table.
2. **Transform** – parse mixed date formats, strip currency symbols, normalise
   categories/regions, drop duplicates and invalid rows, then enrich by joining
   the product table and deriving fields.
3. **Load**     – write a partition-ready, typed Parquet file plus a CSV preview.

Run as a script to execute the whole pipeline:

    python src/etl.py

or import `run_pipeline()` / individual `clean_*` helpers from a notebook.
"""

from __future__ import annotations

import os
import re
import pandas as pd

RAW_SALES = "data/raw/sales_raw.csv"
RAW_PRODUCTS = "data/raw/products.csv"
OUT_PARQUET = "data/processed/sales_clean.parquet"
OUT_CSV = "data/processed/sales_clean_preview.csv"


# ---------------------------------------------------------------- extract ----
def extract(sales_path: str = RAW_SALES,
            products_path: str = RAW_PRODUCTS) -> tuple[pd.DataFrame, pd.DataFrame]:
    sales = pd.read_csv(sales_path, dtype={"order_id": "Int64"})
    products = pd.read_csv(products_path)
    return sales, products


# -------------------------------------------------------------- transform ----
def parse_money(series: pd.Series) -> pd.Series:
    """'$1,234.50' / '12.00' / '' -> float (NaN for blanks)."""
    cleaned = (series.astype(str)
               .str.replace(r"[^0-9.\-]", "", regex=True)
               .replace("", pd.NA))
    return pd.to_numeric(cleaned, errors="coerce")


def parse_dates(series: pd.Series) -> pd.Series:
    """Handle multiple date formats in one column."""
    s = series.astype(str).str.strip()
    out = pd.to_datetime(s, format="mixed", dayfirst=True, errors="coerce")
    return out


def normalise_text(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.title().replace("Nan", pd.NA)


def transform(sales: pd.DataFrame, products: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    audit = {"rows_in": len(sales)}

    df = sales.copy()
    df["order_date"] = parse_dates(df["order_date"])
    df["revenue"] = parse_money(df["revenue"])
    df["category"] = normalise_text(df["category"])
    df["region"] = normalise_text(df["region"])

    # drop exact duplicates
    before = len(df)
    df = df.drop_duplicates()
    audit["duplicates_removed"] = before - len(df)

    # invalid rows: non-positive quantity, unparseable date or revenue
    invalid = (df["quantity"] <= 0) | df["order_date"].isna() | df["revenue"].isna()
    audit["invalid_rows_removed"] = int(invalid.sum())
    df = df[~invalid].copy()

    # impute missing region as 'Unknown'
    audit["region_imputed"] = int(df["region"].isna().sum())
    df["region"] = df["region"].fillna("Unknown")

    # enrich: authoritative product name/price from the reference table
    df = df.merge(products[["product_id", "product_name", "unit_price"]],
                  on="product_id", how="left")

    # derived fields
    df["expected_revenue"] = (df["quantity"] * df["unit_price"]).round(2)
    df["order_month"] = df["order_date"].dt.to_period("M").astype(str)

    df = df[[
        "order_id", "order_date", "order_month", "product_id", "product_name",
        "category", "region", "quantity", "unit_price", "revenue",
        "expected_revenue",
    ]].sort_values("order_date").reset_index(drop=True)

    audit["rows_out"] = len(df)
    return df, audit


# ------------------------------------------------------------------- load ----
def load(df: pd.DataFrame, parquet_path: str = OUT_PARQUET,
         csv_path: str = OUT_CSV) -> None:
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    df.to_parquet(parquet_path, index=False)
    df.head(50).to_csv(csv_path, index=False)


# --------------------------------------------------------------- pipeline ----
def run_pipeline() -> tuple[pd.DataFrame, dict]:
    sales, products = extract()
    clean, audit = transform(sales, products)
    load(clean)
    return clean, audit


if __name__ == "__main__":
    clean, audit = run_pipeline()
    print("ETL complete — audit:")
    for k, v in audit.items():
        print(f"  {k:>22}: {v}")
    print(f"\nWrote {OUT_PARQUET} ({len(clean)} rows)")
