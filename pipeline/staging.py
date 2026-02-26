import os
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert

from db.models import Order, OrderStaging


def load_orders_staging(df, batch_id: str) -> int:
    engine = create_engine(os.environ["DATABASE_URL"])

    rows = df.to_dict(orient="records")
    for r in rows:
        r["batch_id"] = batch_id

    with engine.begin() as conn:
        conn.execute(insert(OrderStaging), rows)

    return len(rows)


def merge_orders(batch_id: str) -> int:
    engine = create_engine(os.environ["DATABASE_URL"])

    merge_sql = """
    INSERT INTO orders (order_id, customer_id, amount, currency, date, source)
    SELECT
        order_id, customer_id, amount, currency, date, source
    FROM orders_staging
    WHERE batch_id = :batch_id
    ON CONFLICT (order_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        amount      = EXCLUDED.amount,
        currency    = EXCLUDED.currency,
        date        = EXCLUDED.date,
        source      = EXCLUDED.source;
    """

    cleanup_sql = """
    DELETE FROM orders_staging WHERE batch_id = :batch_id;
    """

    with engine.begin() as conn:
        conn.execute(text(merge_sql), {"batch_id": batch_id})
        res = conn.execute(text(cleanup_sql), {"batch_id": batch_id})

    # res.rowcount = hur många staging-rader som togs bort (bra proxy)
    return int(res.rowcount or 0)