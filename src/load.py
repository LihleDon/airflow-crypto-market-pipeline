import duckdb
import os
from datetime import datetime, timezone


def load_to_duckdb(records: list[dict]):
    """
    Takes the list of records from extract.py and loads them
    into a DuckDB database file. Creates the table if it does
    not exist yet. Appends new records on every run so we build
    up a price history over time.
    """

    # Create the processed folder if it does not exist.
    os.makedirs("data/processed", exist_ok=True)

    # duckdb.connect() opens a connection to a DuckDB database file.
    # If the file does not exist yet, DuckDB creates it automatically.
    # This file lives on disk and persists between runs — it is your
    # local warehouse for this project.
    con = duckdb.connect("data/processed/crypto_prices.duckdb")

    # CREATE TABLE IF NOT EXISTS means this only runs once.
    # On the second run and every run after, DuckDB sees the table
    # already exists and skips this statement completely.
    # Each column type is chosen deliberately:
    # VARCHAR for text, DOUBLE for decimals, TIMESTAMP WITH TIME ZONE
    # for our timezone-aware pulled_at string.
    con.execute("""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            coin VARCHAR,
            price_usd DOUBLE,
            market_cap_usd DOUBLE,
            change_24h_pct DOUBLE,
            pulled_at TIMESTAMPTZ
        )
    """)

    # We loop through each record and insert it individually.
    # The ? placeholders are parameterised — DuckDB substitutes the
    # actual values safely. This prevents SQL injection and also
    # handles special characters in strings without breaking the query.
    inserted = 0
    for record in records:
        con.execute("""
            INSERT INTO crypto_prices
                (coin, price_usd, market_cap_usd, change_24h_pct, pulled_at)
            VALUES (?, ?, ?, ?, ?)
        """, [
            record["coin"],
            record["price_usd"],
            record["market_cap_usd"],
            record["change_24h_pct"],
            record["pulled_at"]
        ])
        inserted += 1

    # Verify the load worked by querying the total row count.
    # This is a lightweight sanity check — if the number grows
    # each time the pipeline runs, we know data is accumulating.
    total = con.execute("SELECT COUNT(*) FROM crypto_prices").fetchone()[0]
    print(f"Inserted {inserted} records. Total rows in table: {total}")

    # Always close the connection when you are done.
    # Leaving connections open can cause file lock issues,
    # especially on Windows where DuckDB files get locked by the OS.
    con.close()


if __name__ == "__main__":
    # We import extract here so we can test the full flow
    # extract -> load in one command.
    from extract import extract_crypto_prices
    records = extract_crypto_prices()
    load_to_duckdb(records)