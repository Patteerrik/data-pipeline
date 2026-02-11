import os
from sqlalchemy import select, func
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert

from db.models import Base, Order

load_dotenv()


def get_engine():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise ValueError("DATABASE_URL missing")
    return create_engine(url)


def init_db(engine) -> None:
    Base.metadata.create_all(engine)


def load_orders(df: pd.DataFrame) -> int:
    engine = get_engine()
    init_db(engine)

    records = df.to_dict(orient="records")

    with engine.begin() as conn:
        stmt = insert(Order).values(records)

        # om order_id finns: uppdatera övriga fält
        update_cols = {
            "customer_id": stmt.excluded.customer_id,
            "amount": stmt.excluded.amount,
            "currency": stmt.excluded.currency,
            "date": stmt.excluded.date,
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=[Order.order_id],
            set_=update_cols,
        )

        conn.execute(stmt)

        # returnera total rows i tabellen
        count = conn.execute(
            select(func.count()).select_from(Order)
        ).scalar_one()


    return int(count)
