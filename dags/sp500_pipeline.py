# ============================================================
# sp500_pipeline.py
# Purpose: Airflow DAG that orchestrates the entire SP500
#          ETL/ELT pipeline from extraction to loading
#
# What is a DAG?
#   DAG = Directed Acyclic Graph. It's a definition of tasks
#   and the order they must run in. "Directed" means tasks
#   flow in one direction. "Acyclic" means no circular loops.
#   Airflow reads this file and schedules it automatically.
#
# Pipeline flow:
#   Extract (3 sources in parallel)
#     -> Transform (PySpark)
#     -> Load PostgreSQL
#     -> Load S3
#
# Schedule: runs daily at midnight UTC
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Add our project folder to Python path so Airflow can
# import our extraction and loading scripts
sys.path.insert(0, '/opt/airflow/project')

# ============================================================
# Default arguments applied to every task in the DAG
# retries=2 means if a task fails it will retry twice
# retry_delay=5min means wait 5 minutes between retries
# ============================================================
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ============================================================
# Task functions
# Each function wraps one of our pipeline scripts.
# Airflow calls these functions when it runs each task.
# ============================================================

def run_extract_csv():
    """Extract S&P 500 data from Kaggle CSV file"""
    from extract.extract_csv import extract_kaggle_csv, save_raw_copy
    df = extract_kaggle_csv("/opt/airflow/project/data/raw/kaggle/financials.csv")
    save_raw_copy(df, "/opt/airflow/project/data/processed/kaggle_raw.csv")
    print(f"CSV extraction complete: {len(df)} rows")


def run_extract_api():
    """Extract financial statements from Alpha Vantage API"""
    from extract.extract_api import extract_tickers
    # Using a small set of tickers to respect free tier limits
    tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "JPM"]
    os.chdir("/opt/airflow/project")
    extract_tickers(tickers)
    print(f"API extraction complete for {tickers}")


def run_extract_sec():
    """Extract financial facts from SEC EDGAR"""
    from extract.extract_sec import extract_all_sec_data, TICKER_TO_CIK
    os.chdir("/opt/airflow/project")
    extract_all_sec_data(TICKER_TO_CIK)
    print("SEC EDGAR extraction complete")


def run_load_postgres():
    """Load all raw data into PostgreSQL data warehouse"""
    from load.load_to_postgres import get_engine, load_kaggle_financials
    from load.load_to_postgres import load_api_income_statements
    from load.load_to_postgres import load_sec_facts, verify_tables
    os.chdir("/opt/airflow/project")
    engine = get_engine()
    load_kaggle_financials(engine)
    load_api_income_statements(engine)
    load_sec_facts(engine)
    verify_tables(engine)
    engine.dispose()
    print("PostgreSQL load complete")


def run_load_s3():
    """Upload all data to S3 data lake"""
    from load.load_to_s3 import (
        upload_raw_kaggle, upload_raw_api,
        upload_raw_sec, upload_curated_parquet,
        upload_mart_as_csv, verify_s3_upload
    )
    os.chdir("/opt/airflow/project")
    upload_raw_kaggle()
    upload_raw_api()
    upload_raw_sec()
    upload_curated_parquet()
    upload_mart_as_csv()
    verify_s3_upload()
    print("S3 upload complete")


# ============================================================
# DAG Definition
# This is where we wire everything together.
# with DAG(...) creates the DAG object.
# Each PythonOperator is one task in the pipeline.
# The >> operator sets the execution order.
# ============================================================
with DAG(
    # Unique name for this DAG — shows in the Airflow UI
    dag_id="sp500_financial_pipeline",

    # Default arguments defined above
    default_args=default_args,

    # Human-readable description shown in the UI
    description="Extract, transform, and load S&P 500 financial data",

    # Run once per day at midnight UTC
    schedule_interval="@daily",

    # Start date in the past — Airflow won't backfill old runs
    start_date=datetime(2024, 1, 1),

    # Don't run for all past dates since start_date
    catchup=False,

    # Tags help organise DAGs in the UI
    tags=["sp500", "finance", "etl"],

) as dag:

    # --------------------------------------------------------
    # Task 1a: Extract from Kaggle CSV
    # --------------------------------------------------------
    extract_csv_task = PythonOperator(
        task_id="extract_kaggle_csv",
        python_callable=run_extract_csv,
    )

    # --------------------------------------------------------
    # Task 1b: Extract from Alpha Vantage API
    # Runs in PARALLEL with extract_csv_task and extract_sec_task
    # because they don't depend on each other
    # --------------------------------------------------------
    extract_api_task = PythonOperator(
        task_id="extract_alpha_vantage_api",
        python_callable=run_extract_api,
    )

    # --------------------------------------------------------
    # Task 1c: Extract from SEC EDGAR
    # Also runs in parallel with the other extractions
    # --------------------------------------------------------
    extract_sec_task = PythonOperator(
        task_id="extract_sec_edgar",
        python_callable=run_extract_sec,
    )

    # --------------------------------------------------------
    # Task 2: Transform with PySpark
    # Runs AFTER all three extractions complete
    # BashOperator runs a shell command inside the container
    # --------------------------------------------------------
    transform_task = BashOperator(
        task_id="transform_with_spark",
        bash_command="cd /opt/airflow/project && python transform/spark_transform.py",
    )

    # --------------------------------------------------------
    # Task 3: Run dbt ELT models
    # Runs AFTER Spark transform completes
    # --------------------------------------------------------
    dbt_task = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/airflow/project/sp500_dbt && dbt run --profiles-dir /opt/airflow/project/sp500_dbt",
    )

    # --------------------------------------------------------
    # Task 4: Load to PostgreSQL
    # Runs AFTER dbt models complete
    # --------------------------------------------------------
    load_postgres_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=run_load_postgres,
    )

    # --------------------------------------------------------
    # Task 5: Load to S3
    # Runs AFTER PostgreSQL load completes
    # --------------------------------------------------------
    load_s3_task = PythonOperator(
        task_id="load_to_s3",
        python_callable=run_load_s3,
    )

    # --------------------------------------------------------
    # Define task dependencies using the >> operator
    # This tells Airflow the order tasks must run in
    #
    # Flow:
    # [extract_csv, extract_api, extract_sec]  <- run in parallel
    #              |
    #         transform_task
    #              |
    #           dbt_task
    #              |
    #       load_postgres_task
    #              |
    #          load_s3_task
    # --------------------------------------------------------
    [extract_csv_task, extract_api_task, extract_sec_task] >> transform_task
    transform_task >> dbt_task
    dbt_task >> load_postgres_task
    load_postgres_task >> load_s3_task
