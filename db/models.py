from sqlalchemy import Date, Float, Integer, String, UniqueConstraint, Column, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


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

    order_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    customer_id: Mapped[int] = mapped_column(Integer, nullable=False)
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    date: Mapped[object] = mapped_column(Date, nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False, default="csv")

    created_at = Column(DateTime(timezone=True),
                        server_default=func.now())
    updated_at = Column(DateTime(timezone=True),
                        server_default=func.now(),
                        onupdate=func.now())


class MessageLog(Base):
    __tablename__ = "messages_logs"

    id = Column(Integer, primary_key=True)
    source = Column(String, nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("source", name="uq_messages_logs_source"),
    )


class OrderStaging(Base):
    __tablename__ = "orders_staging"

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    batch_id: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        index=True,
    )

    order_id: Mapped[int] = mapped_column(Integer, nullable=False)
    customer_id: Mapped[int] = mapped_column(Integer, nullable=False)
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    date: Mapped[object] = mapped_column(Date, nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)

    loaded_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
