import pandas as pd
from sqlalchemy import create_engine, select

from pipeline.load import load_orders
from db.models import Order


def test_upsert_updates_existing_row():
    df = pd.DataFrame(
        [
            {
                "order_id": 1,
                "customer_id": 101,
                "amount": 250.0,
                "currency": "SEK",
                "date": "2024-01-01",
            }
        ]
    )

    # första load
    load_orders(df)

    # ändra amount och kör igen
    df2 = df.copy()
    df2["amount"] = 999.0
    load_orders(df2)

    engine = create_engine(
        "postgresql+psycopg2://pipeline_user:pipeline_pass@db:5432/pipeline"
    )
    with engine.begin() as conn:
        amount = conn.execute(
            select(Order.amount).where(Order.order_id == 1)
        ).scalar_one()

    assert float(amount) == 999.0
