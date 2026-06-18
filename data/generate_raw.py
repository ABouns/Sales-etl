"""Generate intentionally *messy* raw retail data to be cleaned by the ETL pipeline.

The mess mirrors real-world ingestion problems:
  * mixed date formats and stray whitespace
  * inconsistent category capitalisation / typos
  * currency symbols and thousands separators in numeric fields
  * missing values, duplicate rows, negative quantities
  * a separate (clean-ish) product reference table to join against

Run:  python data/generate_raw.py
Outputs: data/raw/sales_raw.csv, data/raw/products.csv
"""

import numpy as np
import pandas as pd

RNG = np.random.default_rng(7)
N = 5000

PRODUCTS = {
    "P01": ("Wireless Mouse", "Electronics", 24.99),
    "P02": ("USB-C Cable", "Electronics", 9.50),
    "P03": ("Notebook A5", "Stationery", 4.20),
    "P04": ("Desk Lamp", "Home", 32.00),
    "P05": ("Water Bottle", "Home", 14.75),
    "P06": ("Coffee Mug", "Home", 8.30),
    "P07": ("Mechanical Keyboard", "Electronics", 79.00),
    "P08": ("Gel Pen 5-pack", "Stationery", 6.40),
}
REGIONS = ["North", "South", "East", "West"]


def messy_date(ts: pd.Timestamp) -> str:
    fmts = ["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%d %b %Y"]
    return ts.strftime(RNG.choice(fmts))


def main() -> None:
    pids = list(PRODUCTS)
    dates = pd.to_datetime("2024-01-01") + pd.to_timedelta(
        RNG.integers(0, 365, N), unit="D")

    rows = []
    for i in range(N):
        pid = RNG.choice(pids)
        name, cat, price = PRODUCTS[pid]
        qty = int(RNG.integers(1, 8))
        # messy category: random caps / occasional typo
        c = cat.upper() if RNG.random() < 0.3 else cat.lower()
        if RNG.random() < 0.05:
            c = c + " "  # trailing space
        region = RNG.choice(REGIONS)
        if RNG.random() < 0.1:
            region = f" {region} "  # padding
        # messy revenue string with currency + thousands sep
        revenue = qty * price
        rev_str = f"${revenue:,.2f}" if RNG.random() < 0.5 else f"{revenue:.2f}"
        rows.append({
            "order_id": 100000 + i,
            "order_date": messy_date(dates[i]),
            "product_id": pid,
            "category": c,
            "region": region,
            "quantity": qty,
            "revenue": rev_str,
        })

    df = pd.DataFrame(rows)

    # inject problems
    miss = RNG.choice(df.index, size=120, replace=False)
    df.loc[miss, "region"] = np.nan
    miss2 = RNG.choice(df.index, size=60, replace=False)
    df.loc[miss2, "revenue"] = ""
    neg = RNG.choice(df.index, size=40, replace=False)
    df.loc[neg, "quantity"] = -df.loc[neg, "quantity"].abs()
    # exact duplicate rows
    dup = df.sample(80, random_state=1)
    df = pd.concat([df, dup], ignore_index=True)
    df = df.sample(frac=1, random_state=2).reset_index(drop=True)

    df.to_csv("data/raw/sales_raw.csv", index=False)

    prod = pd.DataFrame(
        [(k, v[0], v[1], v[2]) for k, v in PRODUCTS.items()],
        columns=["product_id", "product_name", "category", "unit_price"],
    )
    prod.to_csv("data/raw/products.csv", index=False)

    print(f"Wrote data/raw/sales_raw.csv: {len(df)} rows (messy)")
    print(f"Wrote data/raw/products.csv: {len(prod)} products")


if __name__ == "__main__":
    main()
