# Airflow Crypto Market Pipeline

A scheduled data pipeline that pulls live cryptocurrency prices daily
and builds a queryable price history — no paid APIs, no cloud required.

Bitcoin, Ethereum, and Solana prices land in a local DuckDB warehouse
every 24 hours, orchestrated entirely by Apache Airflow.

## What it does

Airflow triggers the pipeline on a daily schedule. The extract task
calls the CoinGecko free API and captures current price, market cap,
and 24-hour change for three coins. The load task writes those records
into DuckDB, appending to the history table on every run.

After 30 days of daily runs, you have 90 rows of real price data —
ready to query, analyse, or feed into a downstream model.

## Tech stack

- **Apache Airflow 3** — DAG scheduling and task orchestration
- **CoinGecko API** — free, no API key required
- **DuckDB** — local analytical warehouse
- **Python** — extract and load scripts

## Architecture
CoinGecko API
│
▼
extract.py  ──►  data/raw/prices_YYYYMMDD.json
│
▼  (XCom)
load.py
│
▼
DuckDB (crypto_prices table)

## Project structure
airflow-crypto-market-pipeline/
├── dags/
│   └── crypto_pipeline_dag.py   # Airflow DAG definition
├── src/
│   ├── extract.py               # CoinGecko API call
│   └── load.py                  # DuckDB write
├── data/
│   ├── raw/                     # Raw API responses (gitignored)
│   └── processed/               # DuckDB file (gitignored)
├── airflow_home/                # Airflow config (gitignored)
├── requirements.txt
└── README.md

## Setup

**Clone and create virtual environment:**
```bash
git clone https://github.com/LihleDon/airflow-crypto-market-pipeline.git
cd airflow-crypto-market-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**Initialise Airflow:**
```bash
export AIRFLOW_HOME="$(pwd)/airflow_home"
airflow db migrate
airflow standalone
```

**Point Airflow at the DAGs folder:**

Open `airflow_home/airflow.cfg` and set:
dags_folder = /full/path/to/airflow-crypto-market-pipeline/dags

**Open the UI and trigger the DAG:**

Go to `http://localhost:8080`, log in, find `crypto_market_pipeline`,
and click the trigger button. Both tasks should go green within 30 seconds.

## Query the data

```python
import duckdb
con = duckdb.connect("data/processed/crypto_prices.duckdb")
df = con.execute("SELECT * FROM crypto_prices ORDER BY pulled_at DESC").fetchdf()
print(df)
con.close()
```

## Why this project

Airflow is the most requested orchestration tool in data engineering
job postings. Most junior candidates have never run a real DAG.
This project demonstrates a working scheduled pipeline with proper
task dependencies, XCom data passing, and a persistent local warehouse.