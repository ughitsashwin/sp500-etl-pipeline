import pandas as pd
import json
import os
from sqlalchemy import create_engine, text

CONNECTION_STRING = "postgresql://sp500user:sp500pass@localhost:5432/sp500_dw"

def get_engine(connection_string=None):
    conn = connection_string or CONNECTION_STRING
    engine = create_engine(conn)
    print("Database engine created successfully")
    return engine

def drop_table_cascade(engine, table_name):
    """Drop a table with CASCADE to remove dependent views first"""
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
        conn.commit()

def load_kaggle_financials(engine):
    print("\nLoading Kaggle financials into PostgreSQL...")
    for path in ["data/raw/kaggle/financials.csv",
                 "/opt/airflow/project/data/raw/kaggle/financials.csv"]:
        if os.path.exists(path):
            df = pd.read_csv(path)
            break
    else:
        print("  financials.csv not found — skipping")
        return
    df.columns = (df.columns.str.lower()
                  .str.replace("/", "_per_", regex=False)
                  .str.replace(" ", "_", regex=False))
    # Drop with CASCADE first to remove dependent dbt views
    drop_table_cascade(engine, "raw_financials")
    df.to_sql("raw_financials", con=engine, schema="public",
              if_exists="replace", index=False, method="multi", chunksize=500)
    print(f"Loaded {len(df)} rows into raw_financials")

def load_api_income_statements(engine):
    print("\nLoading API income statements into PostgreSQL...")
    all_records = []
    for folder in ["data/raw/api", "/opt/airflow/project/data/raw/api"]:
        if os.path.exists(folder):
            api_folder = folder
            break
    else:
        print("  API folder not found — skipping")
        return
    for filename in os.listdir(api_folder):
        if not filename.endswith("_income_statement.json"):
            continue
        with open(os.path.join(api_folder, filename), "r") as f:
            data = json.load(f)
        ticker = data.get("symbol", "")
        for report in data.get("annualReports", []):
            report["ticker"] = ticker
            all_records.append(report)
    if not all_records:
        print("  No income statement files found — skipping")
        return
    df = pd.DataFrame(all_records)
    drop_table_cascade(engine, "raw_income_statements")
    df.to_sql("raw_income_statements", con=engine, schema="public",
              if_exists="replace", index=False, method="multi")
    print(f"Loaded {len(df)} rows into raw_income_statements")

def load_sec_facts(engine):
    print("\nLoading SEC facts into PostgreSQL...")
    all_records = []
    for folder in ["data/raw/sec", "/opt/airflow/project/data/raw/sec"]:
        if os.path.exists(folder):
            sec_folder = folder
            break
    else:
        print("  SEC folder not found — skipping")
        return
    for filename in os.listdir(sec_folder):
        if not filename.endswith("_sec_facts.json"):
            continue
        with open(os.path.join(sec_folder, filename), "r") as f:
            data = json.load(f)
        ticker = data.get("ticker", "")
        company_name = data.get("company_name", "")
        for metric_name, values in data.get("metrics", {}).items():
            for value in values:
                all_records.append({
                    "ticker": ticker,
                    "company_name": company_name,
                    "metric_name": metric_name,
                    "fiscal_year": value.get("end", ""),
                    "value": value.get("val", None),
                    "form": value.get("form", ""),
                    "currency": "USD"
                })
    if not all_records:
        print("  No SEC files found — skipping")
        return
    df = pd.DataFrame(all_records)
    drop_table_cascade(engine, "raw_sec_facts")
    df.to_sql("raw_sec_facts", con=engine, schema="public",
              if_exists="replace", index=False, method="multi")
    print(f"Loaded {len(df)} rows into raw_sec_facts")

def verify_tables(engine):
    print("\nVerifying tables in PostgreSQL...")
    tables = ["raw_financials", "raw_income_statements", "raw_sec_facts"]
    for table in tables:
        try:
            result = pd.read_sql(
                f"SELECT COUNT(*) as row_count FROM {table}", engine)
            print(f"  {table}: {result['row_count'][0]} rows")
        except Exception:
            print(f"  {table}: not found (source was skipped)")

if __name__ == "__main__":
    engine = get_engine()
    load_kaggle_financials(engine)
    load_api_income_statements(engine)
    load_sec_facts(engine)
    verify_tables(engine)
    print("\nAll raw data loaded into PostgreSQL successfully!")
    engine.dispose()
