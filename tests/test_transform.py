import pytest

from pipeline.transform import transform_orders


def test_transform_orders_ok():
    df = transform_orders("data/raw/orders.csv")

    assert not df.empty
    assert "amount" in df.columns
    assert df["amount"].dtype.kind == "f"
    assert df["order_id"].is_unique


def test_transform_orders_rejects_negative_amount(tmp_path):
    p = tmp_path / "bad.csv"
    p.write_text(
        "order_id,customer_id,amount,currency,date\n"
        "1,101,-1,SEK,2024-01-01\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        transform_orders(str(p))
