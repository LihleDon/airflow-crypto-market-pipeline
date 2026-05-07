import duckdb
import os
from datetime import datetime, timezone


def load_to_duckdb(records: list[dict]):
    """
    Appends a list of price records to the crypto_prices table in DuckDB.
    Creates the database and table on first run. Subsequent runs append.
    """

    os.makedirs("data/processed", exist_ok=True)

    con = duckdb.connect("data/processed/crypto_prices.duckdb")

    con.execute("""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            coin VARCHAR,
            price_usd DOUBLE,
            market_cap_usd DOUBLE,
            change_24h_pct DOUBLE,
            pulled_at TIMESTAMPTZ
        )
    """)

    # Parameterised inserts prevent SQL injection and handle special characters safely
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

    total = con.execute("SELECT COUNT(*) FROM crypto_prices").fetchone()[0]
    print(f"Inserted {inserted} records. Total rows in table: {total}")

    con.close()


if __name__ == "__main__":
    from extract import extract_crypto_prices
    records = extract_crypto_prices()
    load_to_duckdb(records) 
