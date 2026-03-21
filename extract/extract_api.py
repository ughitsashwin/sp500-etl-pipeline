# ============================================================
# extract_api.py
# Purpose: Extract financial statements from Alpha Vantage API
# Source:  https://www.alphavantage.co
# Data:    Income Statement, Balance Sheet, Cash Flow
#          for a list of S&P 500 tickers
# ============================================================

import requests   # Makes HTTP calls to the API
import json       # Saves API responses as JSON files
import os         # Creates folders and handles file paths
import time       # Adds delays to respect API rate limits
from dotenv import load_dotenv  # Reads secrets from .env file
import pandas as pd             # Used later for data manipulation

# Load environment variables from .env file
# This is how we access ALPHA_VANTAGE_KEY without hardcoding it
load_dotenv()

# Read the API key from the .env file
# os.getenv() returns None if the key isn't found — we'll handle that below
API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

# Base URL for all Alpha Vantage API calls
# We append different parameters to this URL for each type of request
BASE_URL = "https://www.alphavantage.co/query"


def extract_income_statement(ticker: str) -> dict:
    """
    Fetch the annual income statement for a given stock ticker.
    The income statement shows revenue, expenses, and profit over time.
    Returns the raw JSON response as a Python dictionary.
    """

    # Build the query parameters for this specific API endpoint
    params = {
        "function": "INCOME_STATEMENT",  # Which financial statement to fetch
        "symbol": ticker,                # The stock ticker e.g. AAPL, MSFT
        "apikey": API_KEY                # Our authentication key
    }

    # Make the GET request to the API
    response = requests.get(BASE_URL, params=params)

    # Parse the JSON response into a Python dictionary
    data = response.json()

    print(f"  Fetched income statement for {ticker}")
    return data


def extract_balance_sheet(ticker: str) -> dict:
    """
    Fetch the annual balance sheet for a given stock ticker.
    The balance sheet shows assets, liabilities, and shareholder equity.
    Returns the raw JSON response as a Python dictionary.
    """

    params = {
        "function": "BALANCE_SHEET",
        "symbol": ticker,
        "apikey": API_KEY
    }

    response = requests.get(BASE_URL, params=params)

    print(f"  Fetched balance sheet for {ticker}")
    return response.json()


def extract_cash_flow(ticker: str) -> dict:
    """
    Fetch the annual cash flow statement for a given stock ticker.
    Cash flow shows how money moves in and out of the business —
    operating, investing, and financing activities.
    Returns the raw JSON response as a Python dictionary.
    """

    params = {
        "function": "CASH_FLOW",
        "symbol": ticker,
        "apikey": API_KEY
    }

    response = requests.get(BASE_URL, params=params)

    print(f"  Fetched cash flow for {ticker}")
    return response.json()


def save_json(data: dict, filepath: str):
    """
    Save a raw API response (dictionary) to a JSON file on disk.
    We always save the raw response before doing anything else —
    this means if something breaks later, we don't need to call
    the API again. We can just re-read from the saved file.
    """

    # Create the folder if it doesn't exist yet
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # Write the dictionary to a JSON file
    # indent=2 makes it human-readable (pretty printed)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)


def extract_tickers(tickers: list):
    """
    Loop through a list of tickers and extract all three
    financial statements for each one.
    Saves each response as a separate JSON file in data/raw/api/
    """

    for ticker in tickers:
        print(f"\nExtracting data for {ticker}...")

        # --- Income Statement ---
        income = extract_income_statement(ticker)
        save_json(income, f"data/raw/api/{ticker}_income_statement.json")

        # Wait 12 seconds before the next call
        # Alpha Vantage free tier allows max 5 calls per minute
        # 12 seconds between calls = safe to never exceed the limit
        time.sleep(12)

        # --- Balance Sheet ---
        balance = extract_balance_sheet(ticker)
        save_json(balance, f"data/raw/api/{ticker}_balance_sheet.json")

        time.sleep(12)

        # --- Cash Flow ---
        cashflow = extract_cash_flow(ticker)
        save_json(cashflow, f"data/raw/api/{ticker}_cash_flow.json")

        # Wait before moving to the next ticker
        time.sleep(12)

        print(f"  All 3 statements saved for {ticker}")


# ============================================================
# Entry point — only runs when this file is executed directly
# When Airflow imports this file later, this block is skipped
# ============================================================
if __name__ == "__main__":

    # Sanity check — make sure the API key was loaded correctly
    if not API_KEY:
        print("ERROR: ALPHA_VANTAGE_KEY not found in .env file")
        exit(1)

    # Start with 5 well-known tickers to stay within the free tier
    # 5 tickers x 3 statements = 15 API calls (limit is 25/day)
    tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "JPM"]

    print(f"Starting extraction for: {tickers}")
    print("Pausing 12 seconds between each API call (free tier rate limit)\n")

    extract_tickers(tickers)

    print("\nAPI extraction complete.")
    print(f"Raw JSON files saved to: data/raw/api/")