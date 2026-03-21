import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def extract_kaggle_csv(filepath: str) -> pd.DataFrame:
    """
    Extract S&P 500 financial data from a CSV file.
    Returns a cleaned pandas DataFrame.
    """
    print(f"Reading CSV from: {filepath}")

    df = pd.read_csv(filepath)

    print(f"Extracted {len(df)} rows and {len(df.columns)} columns")
    print(f"Columns: {list(df.columns)}")
    print(f"Sample data:\n{df.head(2)}")

    return df

def save_raw_copy(df: pd.DataFrame, output_path: str):
    """
    Save a raw copy to data/processed for the next stage.
    We never modify what's in data/raw — that stays as the original.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Raw copy saved to: {output_path}")

if __name__ == "__main__":
    input_path = "data/raw/kaggle/financials.csv"
    output_path = "data/processed/kaggle_raw.csv"

    df = extract_kaggle_csv(input_path)
    save_raw_copy(df, output_path)
    print("\nExtraction complete.")