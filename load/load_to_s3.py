# ============================================================
# load_to_s3.py
# Purpose: Upload raw and transformed data to AWS S3 data lake
# 
# Data lake zone structure:
#   s3://sp500-etl-pipeline-lake/
#     raw/          <- Original files exactly as extracted
#       kaggle/     <- CSV files from Kaggle
#       api/        <- JSON files from Alpha Vantage API
#       sec/        <- JSON files from SEC EDGAR
#     curated/      <- Transformed Parquet files ready for analysis
#
# Why two zones:
#   raw    = immutable source of truth, never modified
#   curated = clean, optimised for querying with Athena
# ============================================================

import boto3          # AWS SDK for Python
import os             # File and path operations
import pandas as pd   # Read transformed data
import json           # Verify JSON files before uploading

# ============================================================
# S3 Configuration
# ============================================================
BUCKET_NAME = "sp500-etl-pipeline-lake"
AWS_REGION  = "eu-west-1"

# Initialise the S3 client
# boto3 automatically uses the credentials from `aws configure`
# so we don't need to pass keys explicitly here
s3_client = boto3.client("s3", region_name=AWS_REGION)


def upload_file(local_path: str, s3_key: str):
    """
    Upload a single file to S3.
    local_path: path to the file on your Mac
    s3_key:     the full path inside the S3 bucket
                e.g. "raw/kaggle/financials.csv"
    """
    try:
        s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
        file_size = os.path.getsize(local_path)
        print(f"  Uploaded: {s3_key} ({file_size:,} bytes)")
    except Exception as e:
        print(f"  ERROR uploading {local_path}: {e}")


def upload_raw_kaggle():
    """
    Upload raw Kaggle CSV files to the raw/kaggle/ zone.
    These are the original files — never modified.
    """
    print("\nUploading raw Kaggle files to S3...")

    kaggle_folder = "data/raw/kaggle"

    for filename in os.listdir(kaggle_folder):
        # Only upload CSV files, skip .gitkeep
        if not filename.endswith(".csv"):
            continue

        local_path = os.path.join(kaggle_folder, filename)
        s3_key = f"raw/kaggle/{filename}"
        upload_file(local_path, s3_key)


def upload_raw_api():
    """
    Upload raw Alpha Vantage JSON files to the raw/api/ zone.
    These are the original API responses — never modified.
    """
    print("\nUploading raw API files to S3...")

    api_folder = "data/raw/api"

    for filename in os.listdir(api_folder):
        if not filename.endswith(".json"):
            continue

        local_path = os.path.join(api_folder, filename)
        s3_key = f"raw/api/{filename}"
        upload_file(local_path, s3_key)


def upload_raw_sec():
    """
    Upload raw SEC EDGAR JSON files to the raw/sec/ zone.
    """
    print("\nUploading raw SEC files to S3...")

    sec_folder = "data/raw/sec"

    for filename in os.listdir(sec_folder):
        if not filename.endswith(".json"):
            continue

        local_path = os.path.join(sec_folder, filename)
        s3_key = f"raw/sec/{filename}"
        upload_file(local_path, s3_key)


def upload_curated_parquet():
    """
    Upload the transformed Parquet files to the curated/ zone.

    Why Parquet for the curated zone:
    - Columnar format — Athena only reads columns it needs
      which makes queries faster and cheaper
    - Compressed automatically — smaller files = less S3 storage
    - Preserves data types — no re-casting needed when querying
    - Industry standard for data lakes
    """
    print("\nUploading curated Parquet files to S3...")

    parquet_folder = "data/processed/financials_transformed"

    if not os.path.exists(parquet_folder):
        print("  No Parquet files found — run spark_transform.py first")
        return

    for filename in os.listdir(parquet_folder):
        # Spark writes several files — upload the actual .parquet files
        # and the _SUCCESS marker file
        if not (filename.endswith(".parquet") or filename == "_SUCCESS"):
            continue

        local_path = os.path.join(parquet_folder, filename)
        s3_key = f"curated/financials/{filename}"
        upload_file(local_path, s3_key)


def upload_mart_as_csv():
    """
    Export the mart_financials table from PostgreSQL and upload
    to the curated zone as CSV — this makes it queryable by
    Athena without needing a Glue crawler to infer the schema.

    Why also upload as CSV:
    - Athena can query both Parquet and CSV
    - CSV is human-readable — easier to verify the data looks right
    - Demonstrates loading from a database to a data lake
    """
    print("\nExporting mart_financials from PostgreSQL and uploading to S3...")

    try:
        from sqlalchemy import create_engine

        # Connect to our local PostgreSQL container
        engine = create_engine(
            "postgresql://sp500user:sp500pass@localhost:5432/sp500_dw"
        )

        # Read the final mart table
        df = pd.read_sql("SELECT * FROM mart_financials", engine)
        engine.dispose()

        print(f"  Exported {len(df)} rows from mart_financials")

        # Save locally first, then upload
        local_path = "/tmp/mart_financials.csv"
        df.to_csv(local_path, index=False)

        # Upload to curated zone
        upload_file(local_path, "curated/mart_financials/mart_financials.csv")

    except Exception as e:
        print(f"  ERROR exporting mart: {e}")


def verify_s3_upload():
    """
    List everything in the bucket to confirm all files
    were uploaded correctly with their sizes.
    """
    print("\nVerifying S3 upload — listing all files in bucket:")
    print("-" * 60)

    # List all objects in the bucket
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)

    if "Contents" not in response:
        print("  Bucket is empty!")
        return

    total_size = 0
    for obj in response["Contents"]:
        size_kb = obj["Size"] / 1024
        print(f"  {obj['Key']:<55} {size_kb:>8.1f} KB")
        total_size += obj["Size"]

    print("-" * 60)
    print(f"  Total: {len(response['Contents'])} files, "
          f"{total_size/1024/1024:.2f} MB")


# ============================================================
# Entry point
# ============================================================
if __name__ == "__main__":

    print("Starting S3 data lake upload...")
    print(f"Target bucket: s3://{BUCKET_NAME}/\n")

    # Upload raw zone — original files untouched
    upload_raw_kaggle()
    upload_raw_api()
    upload_raw_sec()

    # Upload curated zone — transformed, optimised files
    upload_curated_parquet()
    upload_mart_as_csv()

    # Verify everything landed correctly
    verify_s3_upload()

    print("\nS3 upload complete!")
    print(f"View your data lake at: "
          f"https://s3.console.aws.amazon.com/s3/buckets/{BUCKET_NAME}")
