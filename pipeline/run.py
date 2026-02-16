import logging
import sys

from pipeline.transform import transform_orders
from pipeline.load import load_orders


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def main():
    try:
        logger.info("Starting pipeline")

        df = transform_orders("data/raw/orders.csv")
        logger.info("Transformation complete")

        count = load_orders(df)
        logger.info(f"Load complete. Total rows in DB: {count}")

        logger.info("Pipeline finished successfully")
        sys.exit(0)

    except Exception as e:
        logger.exception("Pipeline failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
