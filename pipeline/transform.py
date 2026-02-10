import pandas as pd


REQUIRED_COLS = ["order_id", "customer_id", "amount", "currency", "date"]


def transform_orders(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df["order_id"] = df["order_id"].astype(int)
    df["customer_id"] = df["customer_id"].astype(int)
    df["amount"] = df["amount"].astype(float)
    df["currency"] = df["currency"].astype(str)
    df["date"] = pd.to_datetime(df["date"], errors="raise").dt.date

    if (df["amount"] < 0).any():
        raise ValueError("amount must be >= 0")

    if df["order_id"].duplicated().any():
        raise ValueError("order_id must be unique")

    return df
