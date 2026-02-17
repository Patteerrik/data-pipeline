from pipeline.transform import transform_orders
from pipeline.load import load_orders

def test_load_sets_source_and_is_idempotent():
    df = transform_orders("data/raw/orders.csv")

    count1 = load_orders(df)
    count2 = load_orders(df)

    assert count1 == 3
    assert count2 == 3
