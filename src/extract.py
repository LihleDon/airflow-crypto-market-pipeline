import requests
import json
import os
from datetime import datetime, timezone


def extract_crypto_prices():
    """
    Pulls current price, market cap, and 24h change for BTC, ETH, and SOL
    from the CoinGecko free API. No authentication required.
    Returns a list of flat records ready for loading.
    """

    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,solana",
        "vs_currencies": "usd",
        "include_24hr_change": "true",
        "include_market_cap": "true"
    }

    response = requests.get(url, params=params, timeout=10)

    # Raises immediately on 4xx/5xx so bad responses never reach the load step
    response.raise_for_status()

    raw = response.json()
    pulled_at = datetime.now(timezone.utc).isoformat()

    records = []
    for coin, metrics in raw.items():
        records.append({
            "coin": coin,
            "price_usd": metrics.get("usd"),
            "market_cap_usd": metrics.get("usd_market_cap"),
            "change_24h_pct": metrics.get("usd_24h_change"),
            "pulled_at": pulled_at
        })

    # Keep the raw API response for auditability and pipeline replay
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
