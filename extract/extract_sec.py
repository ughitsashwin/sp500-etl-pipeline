# ============================================================
# extract_sec.py
# Purpose: Extract company financial facts from SEC EDGAR
# Source:  https://data.sec.gov (free, no API key required)
# Data:    Official XBRL financial data filed with the SEC
#          by publicly traded US companies
# ============================================================

import requests   # Makes HTTP calls to the SEC API
import json       # Saves responses as JSON files
import os         # Creates folders and handles file paths
import time       # Adds delays to be respectful to the SEC servers
import pandas as pd  # Used to read our existing company list

# SEC EDGAR requires a User-Agent header identifying who is making
# the request — this is in their terms of service and prevents blocks
# Format: "Your Name your@email.com"
HEADERS = {
    "User-Agent": "Ashwin Sureshbabu ashwin@example.com"
}

# Base URL for the SEC EDGAR company facts API
# Each company has a unique CIK (Central Index Key) number
SEC_BASE_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# Known CIK numbers for our 5 target companies
# CIK is the SEC's unique identifier for each public company
# You can look these up at: https://www.sec.gov/cgi-bin/browse-edgar
TICKER_TO_CIK = {
    "AAPL":  "0000320193",   # Apple Inc
    "MSFT":  "0000789019",   # Microsoft Corporation
    "GOOGL": "0001652044",   # Alphabet Inc (Google)
    "AMZN":  "0001018724",   # Amazon.com Inc
    "JPM":   "0000019617",   # JPMorgan Chase & Co
}


def extract_company_facts(ticker: str, cik: str) -> dict:
    """
    Fetch all XBRL financial facts for a company from SEC EDGAR.
    XBRL (eXtensible Business Reporting Language) is the standard
    format that all US public companies use to file financial data
    with the SEC. This gives us highly structured, reliable data
    straight from the official source.
    """

    # Build the URL by inserting the CIK number
    url = SEC_BASE_URL.format(cik=cik)

    print(f"  Fetching SEC data for {ticker} (CIK: {cik})")
    print(f"  URL: {url}")

    # Make the request with our User-Agent header
    response = requests.get(url, headers=HEADERS)

    # Check if the request was successful
    # Status 200 = OK, anything else = problem
    if response.status_code != 200:
        print(f"  ERROR: Got status {response.status_code} for {ticker}")
        return {}

    # Parse the JSON response
    data = response.json()

    print(f"  Successfully fetched SEC data for {ticker}")
    return data


def extract_key_metrics(data: dict, ticker: str) -> dict:
    """
    The full SEC EDGAR response is very large (can be 10MB+).
    This function pulls out only the key financial metrics we need
    so we don't store unnecessarily large files.

    We extract:
    - Net Income (us-gaap/NetIncomeLoss)
    - Revenue (us-gaap/Revenues or us-gaap/RevenueFromContractWithCustomer)
    - Total Assets (us-gaap/Assets)
    - Total Liabilities (us-gaap/Liabilities)
    """

    # Navigate into the nested JSON structure
    # SEC data is structured as: facts -> us-gaap -> MetricName -> units -> USD -> [values]
    facts = data.get("facts", {}).get("us-gaap", {})

    # Dictionary to store the metrics we care about
    extracted = {
        "ticker": ticker,
        "company_name": data.get("entityName", ""),
        "metrics": {}
    }

    # List of metrics we want to extract
    # These are standard US GAAP accounting terms
    metrics_to_extract = [
        "NetIncomeLoss",          # Net income / profit
        "Revenues",               # Total revenue
        "Assets",                 # Total assets
        "Liabilities",            # Total liabilities
        "StockholdersEquity",     # Shareholders equity
        "OperatingIncomeLoss",    # Operating income
    ]

    for metric in metrics_to_extract:
        if metric in facts:
            # Get the USD values for this metric
            usd_values = facts[metric].get("units", {}).get("USD", [])

            # Filter to annual filings only (form 10-K)
            # 10-K is the annual report, 10-Q is quarterly
            annual_values = [
                v for v in usd_values
                if v.get("form") == "10-K"
            ]

            # Keep only the most recent 5 years of annual data
            annual_values = sorted(
                annual_values,
                key=lambda x: x.get("end", ""),
                reverse=True
            )[:5]

            extracted["metrics"][metric] = annual_values
            print(f"    Found {len(annual_values)} annual records for {metric}")
        else:
            print(f"    Warning: {metric} not found for {ticker}")

    return extracted


def save_json(data: dict, filepath: str):
    """
    Save extracted data to a JSON file.
    Creates the folder if it doesn't exist.
    """

    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    print(f"  Saved to: {filepath}")


def extract_all_sec_data(ticker_cik_map: dict):
    """
    Loop through all tickers, fetch SEC data for each,
    extract the key metrics, and save to disk.
    """

    for ticker, cik in ticker_cik_map.items():
        print(f"\nProcessing {ticker}...")

        # Fetch the full company facts from SEC EDGAR
        raw_data = extract_company_facts(ticker, cik)

        if not raw_data:
            print(f"  Skipping {ticker} due to fetch error")
            continue

        # Extract only the metrics we need
        # (avoids storing 10MB+ files for each company)
        extracted = extract_key_metrics(raw_data, ticker)

        # Save the extracted metrics to disk
        output_path = f"data/raw/sec/{ticker}_sec_facts.json"
        save_json(extracted, output_path)

        # Wait 1 second between requests to be respectful to SEC servers
        # SEC asks that automated tools make no more than 10 requests/second
        time.sleep(1)

    print(f"\nAll SEC data saved to: data/raw/sec/")


# ============================================================
# Entry point — only runs when this file is executed directly
# ============================================================
if __name__ == "__main__":

    print("Starting SEC EDGAR extraction...")
    print(f"Extracting data for: {list(TICKER_TO_CIK.keys())}\n")

    extract_all_sec_data(TICKER_TO_CIK)

    print("\nSEC extraction complete.")