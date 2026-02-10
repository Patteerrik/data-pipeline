import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

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

    with Session(engine) as session:
        for r in records:
            exists = session.get(Order, r["order_id"])
            if exists:
                continue

            session.add(
                Order(
                    order_id=r["order_id"],
                    customer_id=r["customer_id"],
                    amount=r["amount"],
                    currency=r["currency"],
                    date=r["date"],
                )
            )
        session.commit()

    with Session(engine) as session:
        return session.query(Order).count()
