from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys, os

sys.path.insert(0, '/opt/airflow/project')

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

# Read DB connection from environment variables injected by Docker Compose
# Inside Docker, we use the service name "sp500-postgres" not "localhost"
DB_HOST = os.getenv("SP500_DB_HOST", "localhost")
DB_PORT = os.getenv("SP500_DB_PORT", "5432")
DB_USER = os.getenv("SP500_DB_USER", "sp500user")
DB_PASS = os.getenv("SP500_DB_PASS", "sp500pass")
DB_NAME = os.getenv("SP500_DB_NAME", "sp500_dw")
DB_CONN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def run_extract_csv():
    from extract.extract_csv import extract_kaggle_csv, save_raw_copy
    df = extract_kaggle_csv("/opt/airflow/project/data/raw/kaggle/financials.csv")
    save_raw_copy(df, "/opt/airflow/project/data/processed/kaggle_raw.csv")
    print(f"CSV extraction complete: {len(df)} rows")

def run_extract_api():
    sys.path.insert(0, '/opt/airflow/project')
    from extract.extract_api import extract_tickers
    os.chdir("/opt/airflow/project")
    extract_tickers(["AAPL", "MSFT", "GOOGL", "AMZN", "JPM"])
    print("API extraction complete")

def run_extract_sec():
    from extract.extract_sec import extract_all_sec_data, TICKER_TO_CIK
    os.chdir("/opt/airflow/project")
    extract_all_sec_data(TICKER_TO_CIK)
    print("SEC extraction complete")

def run_pandas_transform():
    import pandas as pd
    os.chdir("/opt/airflow/project")
    df = pd.read_csv("data/raw/kaggle/financials.csv")
    df.columns = (df.columns.str.lower()
                  .str.replace("/", "_per_", regex=False)
                  .str.replace(" ", "_", regex=False))
    df = df.dropna(subset=["symbol", "market_cap", "ebitda"])
    df = df[df["market_cap"] > 0]
    df["ebitda_to_market_cap"] = round(df["ebitda"] / df["market_cap"] * 100, 2)
    df["price_range_pct"] = round(
        (df["52_week_high"] - df["52_week_low"]) / df["52_week_low"] * 100, 2)
    os.makedirs("data/processed/financials_transformed_pandas", exist_ok=True)
    df.to_csv("data/processed/financials_transformed_pandas/financials.csv", index=False)
    print(f"Pandas transform complete: {len(df)} rows")

def run_load_postgres():
    # Import and override connection string to use Docker network hostname
    import load.load_to_postgres as loader
    from sqlalchemy import create_engine
    os.chdir("/opt/airflow/project")
    engine = create_engine(DB_CONN)
    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}")
    loader.load_kaggle_financials(engine)
    loader.load_api_income_statements(engine)
    loader.load_sec_facts(engine)
    loader.verify_tables(engine)
    engine.dispose()
    print("PostgreSQL load complete")

def run_load_s3():
    from load.load_to_s3 import (upload_raw_kaggle, upload_raw_api,
                                   upload_raw_sec, upload_mart_as_csv,
                                   verify_s3_upload)
    os.chdir("/opt/airflow/project")
    upload_raw_kaggle()
    upload_raw_api()
    upload_raw_sec()
    # Pass the correct DB connection for mart export
    import pandas as pd
    from sqlalchemy import create_engine
    import boto3
    engine = create_engine(DB_CONN)
    df = pd.read_sql("SELECT * FROM mart_financials", engine)
    engine.dispose()
    df.to_csv("/tmp/mart_financials.csv", index=False)
    s3 = boto3.client("s3", region_name="eu-west-1")
    s3.upload_file("/tmp/mart_financials.csv",
                   "sp500-etl-pipeline-lake",
                   "curated/mart_financials/mart_financials.csv")
    print(f"S3 upload complete: {len(df)} rows")

with DAG(
    dag_id="sp500_financial_pipeline",
    default_args=default_args,
    description="Extract, transform, and load S&P 500 financial data",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sp500", "finance", "etl"],
) as dag:

    extract_csv_task = PythonOperator(
        task_id="extract_kaggle_csv",
        python_callable=run_extract_csv,
    )
    extract_api_task = PythonOperator(
        task_id="extract_alpha_vantage_api",
        python_callable=run_extract_api,
    )
    extract_sec_task = PythonOperator(
        task_id="extract_sec_edgar",
        python_callable=run_extract_sec,
    )
    transform_task = PythonOperator(
        task_id="transform_with_pandas",
        python_callable=run_pandas_transform,
    )
    dbt_task = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/airflow/project/sp500_dbt && dbt run --profiles-dir /root/.dbt 2>&1 || echo 'dbt skipped'",
    )
    load_postgres_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=run_load_postgres,
    )
    load_s3_task = PythonOperator(
        task_id="load_to_s3",
        python_callable=run_load_s3,
    )

    [extract_csv_task, extract_api_task, extract_sec_task] >> transform_task
    transform_task >> dbt_task
    dbt_task >> load_postgres_task
    load_postgres_task >> load_s3_task
