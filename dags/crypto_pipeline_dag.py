from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import sys
import os

# We need to tell Python where to find our src/ scripts.
# By default Airflow only knows about the dags/ folder.
# This line adds the project root to the Python path so
# that "from src.extract import ..." works correctly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extract import extract_crypto_prices
from src.load import load_to_duckdb


def run_extract(**context):
    """
    Airflow calls this function when the extract task runs.
    We store the records in XCom — Airflow's built-in mechanism
    for passing data between tasks in the same DAG run.
    context is automatically injected by Airflow and gives us
    access to the task instance (ti) which handles XCom pushing.
    """
    records = extract_crypto_prices()

    # ti.xcom_push stores the records so the next task can retrieve them.
    # XCom stands for cross-communication. Think of it as a small
    # shared notepad between tasks in the same pipeline run.
    context["ti"].xcom_push(key="records", value=records)
    print(f"Extract complete. {len(records)} records pushed to XCom.")


def run_load(**context):
    """
    Airflow calls this function when the load task runs.
    We pull the records that the extract task stored in XCom
    and pass them into the load function.
    """

    # ti.xcom_pull retrieves what the extract task stored.
    # task_ids= tells it which task produced the data we want.
    # key= must match exactly what was used in xcom_push above.
    records = context["ti"].xcom_pull(task_ids="extract", key="records")

    if not records:
        raise ValueError("No records received from extract task. Aborting load.")

    load_to_duckdb(records)
    print("Load complete.")


# This is the DAG definition — the blueprint Airflow reads
# to understand the pipeline structure and schedule.
with DAG(
    # dag_id is the unique name shown in the Airflow UI.
    dag_id="crypto_market_pipeline",

    # start_date tells Airflow when this DAG became active.
    # We use today so Airflow does not try to backfill
    # missed runs from the past.
    start_date=datetime(2026, 5, 7, tzinfo=timezone.utc),

    # schedule means run this DAG once every 24 hours.
    # @daily is a shorthand for "0 0 * * *" in cron syntax —
    # midnight UTC every day.
    schedule="@daily",

    # catchup=False tells Airflow not to run all the missed
    # daily runs between start_date and today.
    # Without this, Airflow would try to backfill every day
    # since start_date which is not what we want.
    catchup=False,

    # A human-readable description shown in the Airflow UI.
    description="Pulls live crypto prices from CoinGecko and loads into DuckDB daily",

    # tags help organise DAGs in the UI when you have many of them.
    tags=["crypto", "coingecko", "duckdb"],
) as dag:

    # PythonOperator runs a Python function as a task.
    # task_id is the name shown in the Airflow UI graph view.
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=run_load,
    )

    # The >> operator sets the dependency.
    # This means: run extract first, then run load.
    # If extract fails, load never starts.
    extract_task >> load_task