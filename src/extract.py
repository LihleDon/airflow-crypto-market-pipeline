import requests
import json
import os
from datetime import datetime, timezone


def extract_crypto_prices():
    """
    Calls the CoinGecko free API and pulls current prices for
    Bitcoin, Ethereum, and Solana in USD. No API key required.
    Returns a list of records ready for loading.
    """

    # The CoinGecko simple price endpoint is completely free.
    # ids= specifies which coins we want.
    # vs_currencies=usd means we want prices in US dollars.
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,solana",
        "vs_currencies": "usd",
        "include_24hr_change": "true",
        "include_market_cap": "true"
    }

    # requests.get() sends an HTTP GET request to the URL.
    # params= appends the query string automatically.
    # So the actual URL called looks like:
    # https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,...
    response = requests.get(url, params=params, timeout=10)

    # raise_for_status() checks if the API returned an error code
    # like 429 (rate limited) or 500 (server error).
    # If it did, this line raises an exception immediately
    # instead of silently passing bad data downstream.
    response.raise_for_status()

    # .json() parses the raw response text into a Python dictionary.
    raw = response.json()

    # We capture the exact moment we pulled this data.
    # This timestamp becomes a column in the database so we can
    # track price history over time.
    pulled_at = datetime.now(timezone.utc).isoformat()

    # The API returns a nested dict like:
    # {"bitcoin": {"usd": 60000, "usd_24h_change": 1.5, ...}}
    # We flatten it into a list of clean records.
    records = []
    for coin, metrics in raw.items():
        records.append({
            "coin": coin,
            "price_usd": metrics.get("usd"),
            "market_cap_usd": metrics.get("usd_market_cap"),
            "change_24h_pct": metrics.get("usd_24h_change"),
            "pulled_at": pulled_at
        })

    # Save raw response to data/raw/ for auditing.
    # In production pipelines you always keep the raw source data
    # so you can replay or debug if something goes wrong downstream.
    os.makedirs("data/raw", exist_ok=True)
    raw_path = f"data/raw/prices_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    with open(raw_path, "w") as f:
        json.dump(raw, f, indent=2)

    print(f"Extracted {len(records)} coins. Raw saved to {raw_path}")
    return records


if __name__ == "__main__":
    records = extract_crypto_prices()
    for r in records:
        print(r)