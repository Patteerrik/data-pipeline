from pipeline.transform import transform_orders
from pipeline.load import load_orders


def main() -> None:
    df = transform_orders("data/raw/orders.csv")
    count = load_orders(df)
    print(f"Loaded rows (total in DB): {count}")


if __name__ == "__main__":
    main()

