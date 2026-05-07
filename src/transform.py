import pandas as pd
from datetime import datetime


def transform_prices(raw_payload: dict) -> pd.DataFrame:
    """
    Convert the raw CoinGecko API payload into a clean, flat DataFrame
    with one row per coin per fetch.

    Input shape:
    {
        "fetched_at": "2026-05-03T10:30:00",
        "source": "coingecko",
        "data": {
            "bitcoin": {"usd": 67200, "usd_24h_change": 1.23, ...},
            ...
        }
    }

    Output: DataFrame with columns:
    coin_id, price_usd, change_24h_pct, market_cap_usd,
    volume_24h_usd, fetched_at, source
    """
    fetched_at = raw_payload["fetched_at"]
    source = raw_payload["source"]
    coin_data = raw_payload["data"]

    rows = []

    for coin_id, metrics in coin_data.items():
        row = {
            "coin_id": coin_id,
            "price_usd": metrics.get("usd"),
            "change_24h_pct": metrics.get("usd_24h_change"),
            "market_cap_usd": metrics.get("usd_market_cap"),
            "volume_24h_usd": metrics.get("usd_24h_vol"),
            "fetched_at": fetched_at,
            "source": source,
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    df["fetched_at"] = pd.to_datetime(df["fetched_at"])
    df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce")
    df["change_24h_pct"] = pd.to_numeric(df["change_24h_pct"], errors="coerce")
    df["market_cap_usd"] = pd.to_numeric(df["market_cap_usd"], errors="coerce")
    df["volume_24h_usd"] = pd.to_numeric(df["volume_24h_usd"], errors="coerce")

    df = df.dropna(subset=["price_usd"])

    df = df.sort_values("coin_id").reset_index(drop=True)

    return df


if __name__ == "__main__":
    sample = {
        "fetched_at": datetime.utcnow().isoformat(),
        "source": "coingecko",
        "data": {
            "bitcoin": {
                "usd": 67200,
                "usd_24h_change": 1.23,
                "usd_market_cap": 1320000000000,
                "usd_24h_vol": 28000000000,
            },
            "ethereum": {
                "usd": 3500,
                "usd_24h_change": -0.45,
                "usd_market_cap": 420000000000,
                "usd_24h_vol": 14000000000,
            },
        },
    }

    result = transform_prices(sample)
    print(result)
    print(result.dtypes)