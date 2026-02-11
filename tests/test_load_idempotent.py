from pipeline.transform import transform_orders
from pipeline.load import load_orders


def test_load_is_idempotent():
    df = transform_orders("data/raw/orders.csv")

    count1 = load_orders(df)
    count2 = load_orders(df)

    assert count2 == count1
