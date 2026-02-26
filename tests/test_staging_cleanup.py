import pandas as pd
from sqlalchemy import create_engine, select
from db.models import OrderStaging
from pipeline.load import load_orders


def test_staging_is_cleaned_after_merge():
    df = pd.DataFrame(
        [
            {
                "order_id": 1,
                "customer_id": 101,
                "amount": 250.0,
                "currency": "SEK",
                "date": "2024-01-01",
                "source": "csv",
            }
        ]
    )

    load_orders(df)

    engine = create_engine(
        "postgresql+psycopg2://pipeline_user:pipeline_pass@db:5432/pipeline"
    )
    with engine.begin() as conn:
        count = conn.execute(select(OrderStaging.id)).all()

    assert len(count) == 0