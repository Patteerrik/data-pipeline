import os
import pytest
from sqlalchemy import create_engine, text


@pytest.fixture(autouse=True)
def clean_db():
    url = os.getenv("DATABASE_URL")
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE orders RESTART IDENTITY CASCADE;"))
