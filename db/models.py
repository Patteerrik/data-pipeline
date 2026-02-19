from sqlalchemy import Date, Float, Integer, String, UniqueConstraint, Column
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Message(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True)
    source = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint("source", name="uq_messages_source"),
    )


class Order(Base):
    __tablename__ = "orders"

    order_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True
    )
    customer_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False
    )
    amount: Mapped[float] = mapped_column(
        Float,
        nullable=False
    )
    currency: Mapped[str] = mapped_column(
        String(3),
        nullable=False
    )
    date: Mapped[object] = mapped_column(
        Date,
        nullable=False
    )
    source: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="csv"
    )
