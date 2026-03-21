# ============================================================
# spark_transform.py
# Purpose: Clean raw financial data and calculate key metrics
#          using Apache Spark (ETL path — transform before load)
# Why Spark: Spark is the industry standard for large-scale
#            batch transformations. Even running locally it
#            demonstrates the same skills used on real clusters
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import os

# ============================================================
# Initialise Spark Session
# This is always the first step in any PySpark script.
# SparkSession is the entry point to all Spark functionality.
# "local[*]" means run locally using all available CPU cores
# ============================================================
spark = SparkSession.builder \
    .appName("SP500FinancialTransform") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Suppress verbose Spark logs so our output is readable
spark.sparkContext.setLogLevel("ERROR")

print("Spark session started successfully")
print(f"Spark version: {spark.version}\n")


def load_kaggle_data(filepath: str):
    """
    Load the raw Kaggle CSV into a Spark DataFrame.
    A DataFrame is Spark's equivalent of a pandas DataFrame —
    a table of rows and columns that can be processed in parallel.
    """

    print(f"Loading Kaggle data from: {filepath}")

    df = spark.read.csv(
        filepath,
        header=True,       # First row contains column names
        inferSchema=True   # Automatically detect data types
    )

    print(f"Loaded {df.count()} rows and {len(df.columns)} columns")
    return df


def clean_data(df):
    """
    Clean the raw DataFrame:
    - Rename columns to snake_case (standard naming convention)
    - Cast columns to correct data types
    - Remove rows where critical financial values are missing
    - Filter out rows with zero or negative revenue (bad data)
    """

    print("\nCleaning data...")

    # Rename columns to snake_case — removes spaces and special chars
    # This makes them easier to reference in SQL and Python code
    df_clean = df \
        .withColumnRenamed("Symbol", "ticker") \
        .withColumnRenamed("Name", "company_name") \
        .withColumnRenamed("Sector", "sector") \
        .withColumnRenamed("Price", "stock_price") \
        .withColumnRenamed("Price/Earnings", "price_earnings_ratio") \
        .withColumnRenamed("Dividend Yield", "dividend_yield") \
        .withColumnRenamed("Earnings/Share", "earnings_per_share") \
        .withColumnRenamed("52 Week Low", "week_52_low") \
        .withColumnRenamed("52 Week High", "week_52_high") \
        .withColumnRenamed("Market Cap", "market_cap") \
        .withColumnRenamed("EBITDA", "ebitda") \
        .withColumnRenamed("Price/Sales", "price_sales_ratio") \
        .withColumnRenamed("Price/Book", "price_book_ratio") \
        .withColumnRenamed("SEC Filings", "sec_filings_url")

    # Cast numeric columns to DoubleType to ensure correct math later
    # inferSchema sometimes reads numbers as strings — this fixes that
    numeric_cols = [
        "stock_price", "price_earnings_ratio", "dividend_yield",
        "earnings_per_share", "week_52_low", "week_52_high",
        "market_cap", "ebitda", "price_sales_ratio", "price_book_ratio"
    ]

    for col in numeric_cols:
        df_clean = df_clean.withColumn(col, F.col(col).cast(DoubleType()))

    # Drop rows where critical columns are null
    # We can't calculate ratios without these values
    df_clean = df_clean.dropna(
        subset=["ticker", "market_cap", "ebitda", "stock_price"]
    )

    # Filter out rows with zero or negative market cap (corrupt data)
    df_clean = df_clean.filter(F.col("market_cap") > 0)

    print(f"Rows after cleaning: {df_clean.count()}")
    return df_clean


def calculate_financial_metrics(df):
    """
    Calculate key financial ratios and metrics.
    These are the derived columns that analysts actually use
    to compare companies and make investment decisions.
    """

    print("\nCalculating financial metrics...")

    df_metrics = df \
        .withColumn(
            # EBITDA Margin: how much of revenue becomes EBITDA
            # Higher = more operationally efficient
            # Formula: EBITDA / Market Cap * 100 (proxy since we don't have revenue here)
            "ebitda_to_market_cap",
            F.round(F.col("ebitda") / F.col("market_cap") * 100, 2)
        ) \
        .withColumn(
            # 52-week price range: how wide is the price swing?
            # Large range = more volatile stock
            "price_range_pct",
            F.round(
                (F.col("week_52_high") - F.col("week_52_low"))
                / F.col("week_52_low") * 100, 2
            )
        ) \
        .withColumn(
            # Price position: where is the current price within the 52-week range?
            # 0% = at the low, 100% = at the high
            "price_position_in_range",
            F.round(
                (F.col("stock_price") - F.col("week_52_low"))
                / (F.col("week_52_high") - F.col("week_52_low")) * 100, 2
            )
        ) \
        .withColumn(
            # Market cap category — classify companies by size
            # These are standard Wall Street classifications
            "company_size",
            F.when(F.col("market_cap") >= 200_000_000_000, "Mega Cap")
             .when(F.col("market_cap") >= 10_000_000_000, "Large Cap")
             .when(F.col("market_cap") >= 2_000_000_000, "Mid Cap")
             .otherwise("Small Cap")
        ) \
        .withColumn(
            # Value score: lower P/E + lower P/B = potentially undervalued
            # This is a simplified value investing metric
            "value_score",
            F.round(
                F.col("price_earnings_ratio") + F.col("price_book_ratio"), 2
            )
        )

    print("Financial metrics calculated successfully")
    return df_metrics


def show_sector_summary(df):
    """
    Print a summary of average metrics by sector.
    This is a quick sanity check to make sure our
    calculations look reasonable before saving.
    """

    print("\nSector summary (average metrics):")
    print("-" * 60)

    df.groupBy("sector") \
      .agg(
          F.count("ticker").alias("company_count"),
          F.round(F.avg("stock_price"), 2).alias("avg_price"),
          F.round(F.avg("price_earnings_ratio"), 2).alias("avg_pe_ratio"),
          F.round(F.avg("ebitda_to_market_cap"), 2).alias("avg_ebitda_pct")
      ) \
      .orderBy("company_count", ascending=False) \
      .show(truncate=False)


def save_as_parquet(df, output_path: str):
    """
    Save the transformed DataFrame as Parquet format.

    Why Parquet instead of CSV?
    - Parquet is a columnar format — it stores data column by column
      instead of row by row. This makes analytical queries much faster
      because you only read the columns you need.
    - It's compressed automatically — typically 5-10x smaller than CSV
    - It preserves data types — no need to re-cast when you read it back
    - It's the industry standard for data lakes (S3, HDFS, etc.)
    """

    print(f"\nSaving transformed data as Parquet to: {output_path}")

    # coalesce(1) combines all partitions into a single file
    # By default Spark splits output into many small files
    # For our small dataset, one file is cleaner
    df.coalesce(1).write.parquet(output_path, mode="overwrite")

    print("Parquet file saved successfully")


# ============================================================
# Entry point
# ============================================================
if __name__ == "__main__":

    # --- Step 1: Load raw data ---
    raw_df = load_kaggle_data("data/raw/kaggle/financials.csv")

    # --- Step 2: Clean the data ---
    clean_df = clean_data(raw_df)

    # --- Step 3: Calculate financial metrics ---
    metrics_df = calculate_financial_metrics(clean_df)

    # --- Step 4: Show sector summary for validation ---
    show_sector_summary(metrics_df)

    # --- Step 5: Save as Parquet for loading to S3 ---
    save_as_parquet(metrics_df, "data/processed/financials_transformed")

    print("\nTransformation complete!")
    print("Next step: load Parquet files to S3 and PostgreSQL")

    # Stop the Spark session cleanly
    spark.stop()