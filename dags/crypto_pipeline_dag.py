from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import sys
import os

# Adds project root to path so src/ modules resolve correctly when Airflow runs tasks
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extract import extract_crypto_prices
from src.load import load_to_duckdb


def run_extract(**context):
    records = extract_crypto_prices()
    context["ti"].xcom_push(key="records", value=records)
    print(f"Extract complete. {len(records)} records pushed to XCom.")


def run_load(**context):
    records = context["ti"].xcom_pull(task_ids="extract", key="records")

    if not records:
        raise ValueError("No records received from extract task. Aborting load.")

    load_to_duckdb(records)
    print("Load complete.")


with DAG(
    dag_id="crypto_market_pipeline",
    start_date=datetime(2026, 5, 7, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    description="Pulls live crypto prices from CoinGecko and loads into DuckDB daily",
    tags=["crypto", "coingecko", "duckdb"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=run_load,
    )

    extract_task >> load_task
