# Airflow Crypto Market Pipeline

A scheduled pipeline that pulls live crypto prices daily and builds
a queryable price history. No paid APIs, no cloud, no moving parts
beyond a local Airflow instance and a DuckDB file.

Bitcoin, Ethereum, and Solana prices are captured every 24 hours
and appended to a local warehouse. After 30 days you have 90 rows
of real market data ready to query or feed downstream.

## How it works

Airflow triggers the pipeline on a daily schedule. The extract task
hits the CoinGecko free API and grabs current price, market cap, and
24-hour change for three coins. The load task writes those records
into DuckDB, appending to the history table on every run.

Data passes between tasks using Airflow XCom. If extract fails,
load never starts.

## Architecture
CoinGecko API
|
v
extract.py
|
+-- data/raw/prices_YYYYMMDD.json  (audit copy)
|
v  XCom
load.py
|
v
DuckDB (crypto_prices table)

## Project structure
airflow-crypto-market-pipeline/
├── dags/
│   └── crypto_pipeline_dag.py
├── src/
│   ├── extract.py
│   └── load.py
├── data/
│   ├── raw/                     (gitignored)
│   └── processed/               (gitignored)
├── airflow_home/                (gitignored)
├── requirements.txt
└── README.md

## Tech stack

- Apache Airflow 3 for scheduling and orchestration
- CoinGecko API, free tier, no key required
- DuckDB as the local analytical warehouse
- Python for extract and load logic

## Setup

Clone the repo and create a virtual environment:

```bash
git clone https://github.com/LihleDon/airflow-crypto-market-pipeline.git
cd airflow-crypto-market-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Initialise Airflow:

```bash
export AIRFLOW_HOME="$(pwd)/airflow_home"
airflow db migrate
airflow standalone
```

Point Airflow at the DAGs folder by opening `airflow_home/airflow.cfg`
and setting:
dags_folder = /full/path/to/airflow-crypto-market-pipeline/dags

Open `http://localhost:8080`, log in, find `crypto_market_pipeline`,
and trigger it. Both tasks should go green within 30 seconds.

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
job postings. Most junior candidates have never run a real DAG. This
project shows a working scheduled pipeline with proper task dependencies,
XCom data passing, and a persistent local warehouse built at zero cost.
