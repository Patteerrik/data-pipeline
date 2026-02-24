import os
import pytest
from sqlalchemy import create_engine
from db.models import Base


@pytest.fixture(autouse=True)
def clean_db():
    engine = create_engine(os.environ["DATABASE_URL"])

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    assert "orders" in Base.metadata.tables, (
        f"orders not registered. tables={list(Base.metadata.tables.keys())}"
    )

    yield