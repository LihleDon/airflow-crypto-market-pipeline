import logging
from extract import fetch_with_metadata
from transform import transform_prices
from load import load_to_duckdb, query_latest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    """
    Execute the full extract → transform → load sequence.
    Each step is logged. Failures raise exceptions and halt the pipeline.
    """
    logger.info("Pipeline started")

    logger.info("Step 1/3 — Fetching prices from CoinGecko")
    raw = fetch_with_metadata()
    logger.info(f"Fetched data for {len(raw['data'])} coins at {raw['fetched_at']}")

    logger.info("Step 2/3 — Transforming raw payload")
    df = transform_prices(raw)
    logger.info(f"Transformed {len(df)} rows")

    logger.info("Step 3/3 — Loading to DuckDB")
    rows_inserted = load_to_duckdb(df)
    logger.info(f"Inserted {rows_inserted} rows into crypto_prices")

    logger.info("Verifying load — latest 5 rows:")
    latest = query_latest(5)
    logger.info(f"\n{latest.to_string()}")

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    run_pipeline()